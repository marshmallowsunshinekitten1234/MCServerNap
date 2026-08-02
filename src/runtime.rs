use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use tokio::io::AsyncWriteExt as _;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, TryAcquireError, mpsc, oneshot, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{Instant, sleep, sleep_until, timeout};

use crate::backend_use::{
    BackendCycle, BackendUseCoordinator, BackendUseLease, LoginTransferProxySession,
};
use crate::context::ServerContext;
use crate::endpoint::Endpoint;
use crate::minecraft::{ClientRequest, ConnectionEnvelope, HandshakeIntent, MinecraftResponder};
use crate::supervisor::{
    self, BackendEndpoint, LifecycleState, ServerPhase, SupervisorConfig, WakeRequest,
};

const PENDING_WAKE_CAPACITY: usize = 1;
const SUPERVISOR_SHUTDOWN_MARGIN: Duration = Duration::from_secs(10);

fn supervisor_shutdown_wait_limit(
    command_timeout: Duration,
    shutdown_timeout: Duration,
) -> Result<Duration> {
    command_timeout
        .checked_add(shutdown_timeout)
        .and_then(|combined| combined.checked_add(SUPERVISOR_SHUTDOWN_MARGIN))
        .context(
            "command_timeout_seconds plus shutdown_timeout_seconds and the supervisor shutdown margin exceed the supported duration; reduce one of the timeout settings",
        )
}

pub struct ServerRuntimeConfig {
    pub bind_endpoint: Endpoint,
    pub backend_endpoint: Endpoint,
    pub handshake_timeout: Duration,
    pub proxy_connect_timeout: Duration,
    pub supervisor: SupervisorConfig,
}

struct PreparedRuntimeInputs {
    context: ServerContext,
    backend_endpoint: Endpoint,
    handshake_timeout: Duration,
    proxy_connect_timeout: Duration,
    supervisor: SupervisorConfig,
    supervisor_wait_limit: Duration,
    responder: MinecraftResponder,
    connection_limit: Arc<Semaphore>,
}

pub struct PreparedServerRuntime {
    bind_endpoint: Endpoint,
    inputs: PreparedRuntimeInputs,
}

pub struct BoundServerRuntime {
    bind_endpoint: Endpoint,
    listener: TcpListener,
    inputs: PreparedRuntimeInputs,
}

pub struct ServerRuntime {
    context: ServerContext,
    listener: TcpListener,
    client_context: ClientContext,
    connection_limit: Arc<Semaphore>,
    connections: JoinSet<()>,
    supervisor: JoinHandle<Result<()>>,
    supervisor_shutdown: oneshot::Sender<()>,
    supervisor_wait_limit: Duration,
}

#[derive(Clone)]
struct ClientContext {
    context: ServerContext,
    lifecycle: watch::Receiver<LifecycleState>,
    wake_requests: mpsc::Sender<WakeRequest>,
    backend_use: Arc<BackendUseCoordinator>,
    responder: Arc<MinecraftResponder>,
    backend_endpoint: Endpoint,
    handshake_timeout: Duration,
    proxy_connect_timeout: Duration,
}

enum RunExit {
    Shutdown(Result<()>),
    Supervisor(std::result::Result<Result<()>, tokio::task::JoinError>),
    ClientTaskPanic(tokio::task::JoinError),
}

impl ServerRuntime {
    #[cfg(test)]
    pub(crate) fn replace_supervisor<F, Fut>(&mut self, run: F)
    where
        F: FnOnce(oneshot::Receiver<()>) -> Fut,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.supervisor.abort();
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        self.supervisor_shutdown = shutdown_sender;
        self.supervisor = tokio::spawn(run(shutdown_receiver));
    }

    pub fn prepare(
        config: ServerRuntimeConfig,
        responder: MinecraftResponder,
        connection_limit: Arc<Semaphore>,
    ) -> Result<PreparedServerRuntime> {
        let ServerRuntimeConfig {
            bind_endpoint,
            backend_endpoint,
            handshake_timeout,
            proxy_connect_timeout,
            supervisor: supervisor_config,
        } = config;
        let supervisor_wait_limit = supervisor_shutdown_wait_limit(
            supervisor_config.command_timeout,
            supervisor_config.shutdown_timeout,
        )?;

        Ok(PreparedServerRuntime {
            bind_endpoint,
            inputs: PreparedRuntimeInputs {
                context: supervisor_config.context.clone(),
                backend_endpoint,
                handshake_timeout,
                proxy_connect_timeout,
                supervisor: supervisor_config,
                supervisor_wait_limit,
                responder,
                connection_limit,
            },
        })
    }

    pub async fn run_until<F>(mut self, shutdown: F) -> Result<()>
    where
        F: Future<Output = Result<()>>,
    {
        tokio::pin!(shutdown);

        let exit = loop {
            tokio::select! {
                biased;

                result = &mut self.supervisor => break RunExit::Supervisor(result),
                result = &mut shutdown => break RunExit::Shutdown(result),
                completed = self.connections.join_next(), if !self.connections.is_empty() => {
                    if let Some(Err(error)) = completed {
                        break RunExit::ClientTaskPanic(error);
                    }
                }
                accepted = self.listener.accept() => {
                    let (socket, peer) = match accepted {
                        Ok(connection) => connection,
                        Err(error) => {
                            log::warn!("[server={}] Failed to accept a client connection: {error}", self.context);
                            sleep(Duration::from_millis(100)).await;
                            continue;
                        }
                    };

                    let permit = match Arc::clone(&self.connection_limit).try_acquire_owned() {
                        Ok(permit) => permit,
                        Err(TryAcquireError::NoPermits) => {
                            log::warn!("[server={}] Dropping connection from {peer}: connection limit reached", self.context);
                            continue;
                        }
                        Err(TryAcquireError::Closed) => {
                            break RunExit::Shutdown(Err(anyhow::anyhow!(
                                "connection limiter unexpectedly closed"
                            )));
                        }
                    };

                    let context = self.client_context.clone();
                    self.connections.spawn(async move {
                        let _permit = permit;
                        let server_context = context.context.clone();
                        if let Err(error) = handle_client(socket, peer, context).await {
                            log::debug!("[server={server_context}] Connection from {peer} ended: {error:#}");
                        }
                    });
                }
            }
        };

        self.connections.abort_all();
        let mut cleanup_client_panic = None;
        while let Some(completed) = self.connections.join_next().await {
            if let Err(error) = completed
                && error.is_panic()
                && cleanup_client_panic.is_none()
            {
                cleanup_client_panic = Some(anyhow::anyhow!("client task panicked: {error}"));
            }
        }

        match exit {
            RunExit::Supervisor(result) => combine_runtime_errors(
                match result {
                    Err(error) => Err(anyhow::anyhow!(
                        "supervisor panicked; cleanup of this runtime is uncertain: {error}"
                    )),
                    Ok(Err(error)) => Err(error).context("server supervisor failed"),
                    Ok(Ok(())) => Err(anyhow::anyhow!("server supervisor stopped unexpectedly")),
                },
                cleanup_client_panic,
            ),
            RunExit::Shutdown(shutdown_result) => {
                let shutdown_result = combine_runtime_errors(shutdown_result, cleanup_client_panic);
                self.finish_shutdown(shutdown_result).await
            }
            RunExit::ClientTaskPanic(error) => {
                let client_panic = Err(anyhow::anyhow!("client task panicked: {error}"));
                self.finish_shutdown(combine_runtime_errors(client_panic, cleanup_client_panic))
                    .await
            }
        }
    }

    async fn finish_shutdown(mut self, shutdown_result: Result<()>) -> Result<()> {
        let shutdown_requested = self.supervisor_shutdown.send(()).is_ok();
        let mut cleanup_result =
            await_supervisor_shutdown(&mut self.supervisor, self.supervisor_wait_limit).await;
        if !shutdown_requested && cleanup_result.is_ok() {
            cleanup_result = Err(anyhow::anyhow!(
                "server supervisor stopped before shutdown was requested"
            ));
        }

        match (shutdown_result, cleanup_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
            (Err(shutdown_error), Err(cleanup_error)) => {
                Err(shutdown_error
                    .context(format!("runtime cleanup also failed: {cleanup_error:#}")))
            }
        }
    }
}

fn combine_runtime_errors(primary: Result<()>, additional: Option<anyhow::Error>) -> Result<()> {
    match (primary, additional) {
        (result, None) => result,
        (Ok(()), Some(error)) => Err(error),
        (Err(primary), Some(additional)) => {
            Err(primary.context(format!("another runtime failure occurred: {additional:#}")))
        }
    }
}

impl PreparedServerRuntime {
    pub async fn bind(self) -> Result<BoundServerRuntime> {
        let listener = TcpListener::bind((self.bind_endpoint.host(), self.bind_endpoint.port()))
            .await
            .with_context(|| format!("failed to listen on {}", self.bind_endpoint))?;

        Ok(BoundServerRuntime {
            bind_endpoint: self.bind_endpoint,
            listener,
            inputs: self.inputs,
        })
    }
}

impl BoundServerRuntime {
    #[must_use]
    pub fn activate(self) -> ServerRuntime {
        let BoundServerRuntime {
            bind_endpoint,
            listener,
            inputs,
        } = self;
        let PreparedRuntimeInputs {
            context,
            backend_endpoint,
            handshake_timeout,
            proxy_connect_timeout,
            supervisor: supervisor_config,
            supervisor_wait_limit,
            responder,
            connection_limit,
        } = inputs;

        let (wake_requests, wake_receiver) = mpsc::channel(PENDING_WAKE_CAPACITY);
        let (supervisor_shutdown, shutdown_receiver) = oneshot::channel();
        let (lifecycle_sender, lifecycle) = watch::channel(LifecycleState::reconciling());
        let backend_use = Arc::new(BackendUseCoordinator::new());
        let supervisor = tokio::spawn(supervisor::run(
            wake_receiver,
            shutdown_receiver,
            lifecycle_sender,
            supervisor_config,
            BackendEndpoint::network(backend_endpoint.clone(), proxy_connect_timeout),
            Arc::clone(&backend_use),
        ));
        let client_context = ClientContext {
            context: context.clone(),
            lifecycle,
            wake_requests,
            backend_use,
            responder: Arc::new(responder),
            backend_endpoint,
            handshake_timeout,
            proxy_connect_timeout,
        };

        log::info!("[server={context}] Listening on {bind_endpoint}");

        ServerRuntime {
            context,
            listener,
            client_context,
            connection_limit,
            connections: JoinSet::new(),
            supervisor,
            supervisor_shutdown,
            supervisor_wait_limit,
        }
    }
}

async fn await_supervisor_shutdown(
    supervisor: &mut JoinHandle<Result<()>>,
    wait_limit: Duration,
) -> Result<()> {
    if let Ok(result) = timeout(wait_limit, &mut *supervisor).await {
        result.context("server supervisor task failed")??;
        return Ok(());
    }

    supervisor.abort();
    let _ = (&mut *supervisor).await;
    bail!("server supervisor did not stop within {wait_limit:?}")
}

async fn handle_client(
    socket: TcpStream,
    peer: std::net::SocketAddr,
    context: ClientContext,
) -> Result<()> {
    socket
        .set_nodelay(true)
        .context("failed to enable TCP_NODELAY for client")?;
    let initial_deadline = Instant::now() + context.handshake_timeout;
    let connection = ConnectionEnvelope::read(socket, initial_deadline).await?;

    // A future routing lookup belongs here, before this runtime's state is sampled.
    let observed_lifecycle = *context.lifecycle.borrow();
    handle_sampled_connection(
        connection,
        peer,
        context,
        observed_lifecycle,
        initial_deadline,
    )
    .await
}

async fn handle_sampled_connection(
    mut connection: ConnectionEnvelope,
    peer: std::net::SocketAddr,
    context: ClientContext,
    observed_lifecycle: LifecycleState,
    initial_deadline: Instant,
) -> Result<()> {
    let current_phase = observed_lifecycle.phase();

    if current_phase == ServerPhase::Running {
        let cycle = observed_lifecycle
            .owned_running_cycle()
            .expect("running lifecycle must have an owned backend cycle");
        return proxy_owned_connection(connection, peer, &context, cycle).await;
    }
    if current_phase == ServerPhase::External {
        return proxy_external_connection(connection, peer, &context).await;
    }

    let operation_timeout = context.handshake_timeout;
    let Some(request) = context
        .responder
        .read_sleeping_request(&mut connection, initial_deadline)
        .await?
    else {
        return Ok(());
    };

    match request {
        ClientRequest::Status => {
            context
                .responder
                .serve_status(connection.socket_mut(), operation_timeout)
                .await?;
        }
        ClientRequest::UnsupportedProtocol { protocol_version } => {
            log::info!(
                "[server={}] Rejected login from {peer}: protocol {protocol_version} is not supported",
                context.context
            );
            context
                .responder
                .send_incompatible_disconnect(connection.socket_mut(), operation_timeout)
                .await?;
        }
        ClientRequest::Login { intent } => {
            log::info!(
                "[server={}] Minecraft {intent:?} request from {peer}",
                context.context
            );
            if matches!(
                current_phase,
                ServerPhase::Reconciling
                    | ServerPhase::Stopped
                    | ServerPhase::Stopping
                    | ServerPhase::Cooldown
                    | ServerPhase::Conflict
            ) {
                match context
                    .wake_requests
                    .try_send(WakeRequest::observed(observed_lifecycle))
                {
                    Ok(()) => log::info!(
                        "[server={}] Queued server wake-up request from {peer}",
                        context.context
                    ),
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        log::debug!(
                            "[server={}] Wake-up request already pending",
                            context.context
                        );
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        bail!("server supervisor wake channel is closed");
                    }
                }
            }
            if current_phase == ServerPhase::Conflict {
                context
                    .responder
                    .send_conflict_disconnect(connection.socket_mut(), operation_timeout)
                    .await?;
            } else {
                context
                    .responder
                    .send_login_disconnect(connection.socket_mut(), operation_timeout)
                    .await?;
            }
        }
    }

    Ok(())
}

async fn proxy_owned_connection(
    connection: ConnectionEnvelope,
    peer: std::net::SocketAddr,
    context: &ClientContext,
    cycle: BackendCycle,
) -> Result<()> {
    let lease = context.backend_use.acquire_shared(cycle).await;
    ensure_current_owned_cycle(&context.lifecycle, cycle)?;
    let backend = connect_backend(&context.backend_endpoint, context.proxy_connect_timeout).await?;
    backend
        .set_nodelay(true)
        .context("failed to enable TCP_NODELAY for backend")?;
    proxy_connected_owned(connection, backend, peer, context, cycle, lease).await
}

async fn proxy_connected_owned(
    connection: ConnectionEnvelope,
    mut backend: TcpStream,
    peer: std::net::SocketAddr,
    context: &ClientContext,
    cycle: BackendCycle,
    lease: BackendUseLease,
) -> Result<()> {
    ensure_current_owned_cycle(&context.lifecycle, cycle)?;
    let intent = connection.intent();
    let (mut client, framed_handshake) = connection.into_proxy_parts();
    log::debug!(
        "[server={}] Proxying {peer} to {}",
        context.context,
        context.backend_endpoint
    );

    backend
        .write_all(&framed_handshake)
        .await
        .with_context(|| format!("failed to replay Minecraft handshake for {peer}"))?;
    let _session = establish_owned_proxy_activity(context, cycle, intent, lease)?;
    proxy_streams(
        &mut client,
        &mut backend,
        peer,
        &context.context,
        framed_handshake.len(),
    )
    .await
}

fn establish_owned_proxy_activity(
    context: &ClientContext,
    cycle: BackendCycle,
    intent: HandshakeIntent,
    lease: BackendUseLease,
) -> Result<Option<LoginTransferProxySession>> {
    ensure_current_owned_cycle(&context.lifecycle, cycle)?;
    match intent {
        HandshakeIntent::Login | HandshakeIntent::Transfer => {
            let session = context
                .backend_use
                .establish_login_transfer_session(lease)
                .map_err(|_stale_lease| {
                    anyhow::anyhow!("backend activity cycle changed before proxy establishment")
                })?;
            Ok(Some(session))
        }
        HandshakeIntent::Status | HandshakeIntent::Unknown(_) => {
            drop(lease);
            Ok(None)
        }
    }
}

async fn proxy_external_connection(
    connection: ConnectionEnvelope,
    peer: std::net::SocketAddr,
    context: &ClientContext,
) -> Result<()> {
    let (mut client, framed_handshake) = connection.into_proxy_parts();
    let mut backend =
        connect_backend(&context.backend_endpoint, context.proxy_connect_timeout).await?;
    backend
        .set_nodelay(true)
        .context("failed to enable TCP_NODELAY for backend")?;
    log::debug!(
        "[server={}] Proxying {peer} to {}",
        context.context,
        context.backend_endpoint
    );
    backend
        .write_all(&framed_handshake)
        .await
        .with_context(|| format!("failed to replay Minecraft handshake for {peer}"))?;
    proxy_streams(
        &mut client,
        &mut backend,
        peer,
        &context.context,
        framed_handshake.len(),
    )
    .await
}

fn ensure_current_owned_cycle(
    lifecycle: &watch::Receiver<LifecycleState>,
    cycle: BackendCycle,
) -> Result<()> {
    ensure!(
        lifecycle.has_changed().is_ok(),
        "server lifecycle authority stopped while opening a proxy connection"
    );
    ensure!(
        lifecycle.borrow().owned_running_cycle() == Some(cycle),
        "owned backend cycle changed while opening a proxy connection"
    );
    Ok(())
}

async fn proxy_streams(
    client: &mut TcpStream,
    backend: &mut TcpStream,
    peer: std::net::SocketAddr,
    context: &ServerContext,
    replayed_handshake_bytes: usize,
) -> Result<()> {
    let (client_to_server, server_to_client) = tokio::io::copy_bidirectional(client, backend)
        .await
        .with_context(|| format!("proxy I/O failed for {peer}"))?;
    let client_to_server = client_to_server
        + u64::try_from(replayed_handshake_bytes).expect("handshake length fits in u64");
    log::debug!(
        "[server={context}] Proxy for {peer} closed ({client_to_server} bytes upstream, {server_to_client} downstream)"
    );
    Ok(())
}

async fn connect_backend(endpoint: &Endpoint, connect_timeout: Duration) -> Result<TcpStream> {
    let deadline = Instant::now() + connect_timeout;
    let mut last_error = None;

    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        let attempt_timeout = remaining.min(Duration::from_secs(1));
        match timeout(
            attempt_timeout,
            TcpStream::connect((endpoint.host(), endpoint.port())),
        )
        .await
        {
            Ok(Ok(stream)) => return Ok(stream),
            Ok(Err(error)) => last_error = Some(error.to_string()),
            Err(_) => last_error = Some("connection attempt timed out".to_owned()),
        }
        let retry_at = (Instant::now() + Duration::from_millis(100)).min(deadline);
        sleep_until(retry_at).await;
    }

    let detail = last_error.unwrap_or_else(|| "timeout elapsed".to_owned());
    bail!("could not connect to Minecraft backend {endpoint}: {detail}")
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use anyhow::anyhow;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::sync::oneshot;

    use super::*;
    use crate::minecraft::{MinecraftResponderSettings, MinecraftVersion};
    use crate::process::LaunchCommand;
    use crate::rcon::RconSecret;
    use crate::supervisor::RconConfig;

    fn runtime_config(
        command: String,
        arguments: Vec<String>,
        rcon_endpoint: Endpoint,
    ) -> ServerRuntimeConfig {
        ServerRuntimeConfig {
            bind_endpoint: Endpoint::new("127.0.0.1", 0),
            backend_endpoint: Endpoint::new("127.0.0.1", 25_566),
            handshake_timeout: Duration::from_secs(30),
            proxy_connect_timeout: Duration::from_secs(1),
            supervisor: SupervisorConfig {
                context: ServerContext::new("test".to_owned()),
                launch: LaunchCommand::new(
                    OsString::from(command),
                    arguments.into_iter().map(OsString::from).collect(),
                    std::env::current_dir().expect("test working directory should be known"),
                ),
                rcon: Some(RconConfig {
                    endpoint: rcon_endpoint,
                    password: RconSecret::new("secret".to_owned()).unwrap(),
                }),
                poll_interval: Duration::from_secs(30),
                idle_timeout: Duration::from_secs(30),
                startup_timeout: Duration::from_secs(30),
                retry_interval: Duration::from_millis(10),
                command_timeout: Duration::from_secs(5),
                shutdown_timeout: Duration::from_millis(100),
            },
        }
    }

    fn test_responder() -> MinecraftResponder {
        MinecraftResponder::new(MinecraftResponderSettings {
            minecraft_version: MinecraftVersion::latest(),
            sleeping_motd_text: "Sleeping".to_owned(),
            sleeping_motd_color: "aqua".to_owned(),
            sleeping_motd_bold: true,
            startup_message_text: "Starting".to_owned(),
            startup_message_color: "light_purple".to_owned(),
            startup_message_bold: true,
            server_icon: None,
        })
        .expect("test responder should be constructed")
    }

    fn endpoint(address: std::net::SocketAddr) -> Endpoint {
        Endpoint::new(address.ip().to_string(), address.port())
    }

    fn construction_config(
        rcon_address: std::net::SocketAddr,
        backend_address: std::net::SocketAddr,
        launch_signal_address: std::net::SocketAddr,
    ) -> ServerRuntimeConfig {
        let executable = std::env::current_exe().expect("test executable path should be known");
        let mut config = runtime_config(
            executable.to_string_lossy().into_owned(),
            vec![
                "--ignored".to_owned(),
                "--exact".to_owned(),
                "runtime::tests::launch_signal_fixture".to_owned(),
                format!("launch-signal={launch_signal_address}"),
                "--quiet".to_owned(),
            ],
            endpoint(rcon_address),
        );
        config.backend_endpoint = endpoint(backend_address);
        config
    }

    #[test]
    #[ignore = "launched only to detect an unexpected runtime process start"]
    fn launch_signal_fixture() {
        let address = std::env::args()
            .find_map(|argument| argument.strip_prefix("launch-signal=").map(str::to_owned))
            .expect("fixture should receive a launch signal address");
        let mut signal = std::net::TcpStream::connect(address)
            .expect("fixture should connect to its launch observer");
        std::io::Write::write_all(&mut signal, &[1]).expect("fixture should report its launch");
    }

    async fn bind_test_runtime(config: ServerRuntimeConfig) -> ServerRuntime {
        ServerRuntime::prepare(config, test_responder(), Arc::new(Semaphore::new(1)))
            .expect("test runtime should be prepared")
            .bind()
            .await
            .expect("test runtime should bind")
            .activate()
    }

    fn write_varint(value: i32, output: &mut Vec<u8>) {
        let mut remaining = value.cast_unsigned();
        loop {
            let mut byte = u8::try_from(remaining & 0x7f).expect("masked to seven bits");
            remaining >>= 7;
            if remaining != 0 {
                byte |= 0x80;
            }
            output.push(byte);
            if remaining == 0 {
                return;
            }
        }
    }

    fn frame_packet(body: &[u8]) -> Vec<u8> {
        let mut packet = Vec::new();
        write_varint(
            i32::try_from(body.len()).expect("test packet length"),
            &mut packet,
        );
        packet.extend_from_slice(body);
        packet
    }

    fn handshake_frame(protocol_version: i32, intent: i32) -> Vec<u8> {
        let mut handshake = Vec::new();
        write_varint(0, &mut handshake);
        write_varint(protocol_version, &mut handshake);
        write_varint(9, &mut handshake);
        handshake.extend_from_slice(b"localhost");
        handshake.extend_from_slice(&25565_u16.to_be_bytes());
        write_varint(intent, &mut handshake);
        frame_packet(&handshake)
    }

    fn login_exchange_with_intent(intent: i32) -> Vec<u8> {
        let version = crate::minecraft::MinecraftVersion::latest();

        let mut login = Vec::new();
        write_varint(0, &mut login);
        write_varint(6, &mut login);
        login.extend_from_slice(b"player");
        login.extend_from_slice(&[7; 16]);

        let mut exchange = handshake_frame(version.protocol(), intent);
        exchange.extend_from_slice(&frame_packet(&login));
        exchange
    }

    fn login_exchange() -> Vec<u8> {
        login_exchange_with_intent(2)
    }

    fn non_minimal_unknown_handshake() -> Vec<u8> {
        let mut body = vec![0x80, 0x00, 0xfb, 0x00, 0x89, 0x00];
        body.extend_from_slice(b"localhost");
        body.extend_from_slice(&25_565_u16.to_be_bytes());
        body.extend_from_slice(&[0xcd, 0x00]);
        let body_length = u8::try_from(body.len()).expect("test body length fits in one byte");
        let mut framed = vec![body_length | 0x80, 0x80, 0x00];
        framed.extend_from_slice(&body);
        framed
    }

    async fn connected_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener should bind");
        let address = listener.local_addr().expect("listener has an address");
        let (client, accepted) = tokio::join!(TcpStream::connect(address), listener.accept());
        (
            client.expect("test client should connect"),
            accepted.expect("test listener should accept").0,
        )
    }

    async fn parsed_connection(
        framed_handshake: &[u8],
    ) -> (TcpStream, ConnectionEnvelope, std::net::SocketAddr) {
        let (mut client, server) = connected_pair().await;
        let peer = server.peer_addr().expect("client peer address");
        client
            .write_all(framed_handshake)
            .await
            .expect("framed handshake should write");
        let connection = ConnectionEnvelope::read(server, Instant::now() + Duration::from_secs(1))
            .await
            .expect("framed handshake should parse");
        (client, connection, peer)
    }

    async fn wait_for_active_sessions(backend_use: &BackendUseCoordinator, expected: usize) {
        timeout(Duration::from_secs(2), async {
            loop {
                let changed = backend_use.activity_notified();
                tokio::pin!(changed);
                if backend_use
                    .activity_snapshot()
                    .is_some_and(|snapshot| snapshot.active_login_transfer_sessions() == expected)
                {
                    return;
                }
                changed.await;
            }
        })
        .await
        .expect("proxy activity should reach the expected session count");
    }

    fn assert_no_wake(wake_receiver: &mut mpsc::Receiver<WakeRequest>) {
        assert!(matches!(
            wake_receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected)
        ));
    }

    fn client_context_with_lifecycle(
        phase: ServerPhase,
        wake_requests: mpsc::Sender<WakeRequest>,
        backend_port: u16,
    ) -> (ClientContext, watch::Sender<LifecycleState>) {
        let lifecycle_state = LifecycleState::test_phase(phase);
        let (lifecycle_sender, lifecycle) = watch::channel(lifecycle_state);
        let backend_use = Arc::new(BackendUseCoordinator::new());
        if let Some(cycle) = lifecycle_state.owned_running_cycle() {
            backend_use.begin_cycle(cycle, Instant::now());
        }
        let context = ClientContext {
            context: ServerContext::new("test".to_owned()),
            lifecycle,
            wake_requests,
            backend_use,
            responder: Arc::new(test_responder()),
            backend_endpoint: Endpoint::new("127.0.0.1", backend_port),
            handshake_timeout: Duration::from_secs(1),
            proxy_connect_timeout: Duration::from_secs(1),
        };
        (context, lifecycle_sender)
    }

    async fn assert_phase_proxies_exact_bytes(phase: ServerPhase, framed_handshake: Vec<u8>) {
        let backend_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("backend listener should bind");
        let backend_port = backend_listener
            .local_addr()
            .expect("backend listener has an address")
            .port();
        let (wake_sender, mut wake_receiver) = mpsc::channel(1);
        let (context, _lifecycle_authority) =
            client_context_with_lifecycle(phase, wake_sender, backend_port);
        let (mut client, server) = connected_pair().await;
        let peer = server.peer_addr().expect("client peer address");
        let handler = tokio::spawn(handle_client(server, peer, context));
        let following_bytes = vec![0, 1, 2, 0xff, 4, 5, 6];
        let mut expected = framed_handshake.clone();
        expected.extend_from_slice(&following_bytes);
        let backend = tokio::spawn(async move {
            let (mut stream, _) = backend_listener
                .accept()
                .await
                .expect("proxy should connect to backend");
            let mut received = vec![0; expected.len()];
            stream
                .read_exact(&mut received)
                .await
                .expect("proxy bytes should arrive");
            assert_eq!(received, expected);
            stream
                .write_all(b"backend")
                .await
                .expect("backend response should write");
        });

        let mut exchange = framed_handshake;
        exchange.extend_from_slice(&following_bytes);
        client
            .write_all(&exchange)
            .await
            .expect("client bytes should write");
        let mut response = [0; 7];
        client
            .read_exact(&mut response)
            .await
            .expect("backend response should arrive");
        assert_eq!(&response, b"backend");
        drop(client);
        backend.await.expect("backend task should not panic");
        timeout(Duration::from_secs(2), handler)
            .await
            .expect("proxy task should finish")
            .expect("proxy task should not panic")
            .expect("proxy should succeed");
        assert_no_wake(&mut wake_receiver);
    }

    #[tokio::test]
    async fn running_and_external_forward_exact_original_bytes() {
        let protocol = crate::minecraft::MinecraftVersion::latest().protocol();
        for intent in [1, 2, 3] {
            assert_phase_proxies_exact_bytes(
                ServerPhase::Running,
                handshake_frame(protocol, intent),
            )
            .await;
        }
        assert_phase_proxies_exact_bytes(ServerPhase::Running, non_minimal_unknown_handshake())
            .await;
        assert_phase_proxies_exact_bytes(ServerPhase::External, non_minimal_unknown_handshake())
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn status_writer_first_rejects_the_reader_after_stopping() {
        let backend_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("backend listener should bind");
        let backend_port = backend_listener.local_addr().unwrap().port();
        let (wake_sender, _wake_receiver) = mpsc::channel(1);
        let (context, lifecycle_authority) =
            client_context_with_lifecycle(ServerPhase::Running, wake_sender, backend_port);
        let cycle = context.lifecycle.borrow().owned_running_cycle().unwrap();
        let first_reader = context.backend_use.acquire_shared(cycle).await;
        let (writer_polling, writer_polled) = oneshot::channel();
        let writer = {
            let backend_use = Arc::clone(&context.backend_use);
            let writer_authority = lifecycle_authority.clone();
            tokio::spawn(async move {
                writer_polling.send(()).unwrap();
                let _exclusive = backend_use.acquire_exclusive().await;
                writer_authority.send_replace(LifecycleState::test_phase(ServerPhase::Stopping));
            })
        };
        writer_polled.await.unwrap();
        tokio::task::yield_now().await;

        let (mut client, server) = connected_pair().await;
        let peer = server.peer_addr().unwrap();
        let handler = tokio::spawn(handle_client(server, peer, context));
        client
            .write_all(&handshake_frame(12_345, 1))
            .await
            .expect("status handshake should write");
        tokio::task::yield_now().await;
        drop(first_reader);

        writer.await.unwrap();
        let error = handler
            .await
            .expect("client task should not panic")
            .expect_err("stale reader should reject the stopped cycle");
        assert!(error.to_string().contains("owned backend cycle changed"));
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        assert!(response.is_empty());
        let backend_listener = backend_listener.into_std().unwrap();
        assert!(matches!(
            backend_listener.accept(),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn status_reader_first_finishes_replay_before_stopping() {
        let (wake_sender, _wake_receiver) = mpsc::channel(1);
        let (context, lifecycle_authority) =
            client_context_with_lifecycle(ServerPhase::Running, wake_sender, 9);
        let backend_use = Arc::clone(&context.backend_use);
        let cycle = context.lifecycle.borrow().owned_running_cycle().unwrap();
        let lease = backend_use.acquire_shared(cycle).await;
        let handshake = handshake_frame(12_345, 1);
        let (mut client, connection, peer) = parsed_connection(&handshake).await;
        let (backend, mut backend_peer) = connected_pair().await;

        let (writer_polling, writer_polled) = oneshot::channel();
        let writer = {
            let backend_use = Arc::clone(&backend_use);
            let lifecycle_authority = lifecycle_authority.clone();
            tokio::spawn(async move {
                writer_polling.send(()).unwrap();
                let _exclusive = backend_use.acquire_exclusive().await;
                lifecycle_authority.send_replace(LifecycleState::test_phase(ServerPhase::Stopping));
            })
        };
        writer_polled.await.unwrap();

        let proxy = tokio::spawn(async move {
            proxy_connected_owned(connection, backend, peer, &context, cycle, lease).await
        });
        let mut replayed = vec![0; handshake.len()];
        backend_peer.read_exact(&mut replayed).await.unwrap();
        assert_eq!(replayed, handshake);
        writer.await.unwrap();

        backend_peer.write_all(b"x").await.unwrap();
        assert_eq!(client.read_u8().await.unwrap(), b'x');
        let snapshot = backend_use.activity_snapshot().unwrap();
        assert_eq!(snapshot.active_login_transfer_sessions(), 0);
        assert_eq!(snapshot.last_final_session_disconnect(), None);
        drop((client, backend_peer));
        proxy
            .await
            .expect("status proxy task should not panic")
            .expect("status proxy should continue after its replay lease is released");
    }

    #[tokio::test]
    async fn post_connect_revalidation_prevents_replay_to_a_changed_cycle() {
        let (wake_sender, _wake_receiver) = mpsc::channel(1);
        let (context, lifecycle_authority) =
            client_context_with_lifecycle(ServerPhase::Running, wake_sender, 9);
        let cycle = context.lifecycle.borrow().owned_running_cycle().unwrap();
        let lease = context.backend_use.acquire_shared(cycle).await;
        let (backend, mut backend_peer) = connected_pair().await;
        let handshake = handshake_frame(12_345, 1);
        let (mut client, connection, peer) = parsed_connection(&handshake).await;

        lifecycle_authority.send_replace(LifecycleState::test_phase(ServerPhase::Stopping));
        let error = proxy_connected_owned(connection, backend, peer, &context, cycle, lease)
            .await
            .expect_err("changed lifecycle should reject the connected backend");
        assert!(error.to_string().contains("owned backend cycle changed"));
        let mut replayed = Vec::new();
        backend_peer.read_to_end(&mut replayed).await.unwrap();
        assert!(replayed.is_empty());
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        assert!(response.is_empty());
    }

    #[tokio::test]
    async fn login_session_is_established_only_after_complete_replay() {
        let (wake_sender, _wake_receiver) = mpsc::channel(1);
        let (context, _lifecycle_authority) =
            client_context_with_lifecycle(ServerPhase::Running, wake_sender, 9);
        let cycle = context.lifecycle.borrow().owned_running_cycle().unwrap();
        let lease = context.backend_use.acquire_shared(cycle).await;
        let (mut backend, mut backend_peer) = connected_pair().await;
        let handshake = handshake_frame(12_345, 2);
        let mut replayed = vec![0; handshake.len()];

        let (write, read) = tokio::join!(
            backend.write_all(&handshake),
            backend_peer.read_exact(&mut replayed)
        );
        write.expect("complete handshake should replay");
        read.expect("backend should receive the complete handshake");
        assert_eq!(replayed, handshake);
        assert_eq!(
            context
                .backend_use
                .activity_snapshot()
                .unwrap()
                .active_login_transfer_sessions(),
            0
        );

        let session =
            establish_owned_proxy_activity(&context, cycle, HandshakeIntent::Login, lease)
                .expect("current replay should establish activity")
                .expect("login intent should create a proxy session");
        assert_eq!(
            context
                .backend_use
                .activity_snapshot()
                .unwrap()
                .active_login_transfer_sessions(),
            1
        );
        drop(session);
    }

    #[tokio::test]
    async fn cycle_change_at_replay_completion_rejects_session() {
        let (wake_sender, _wake_receiver) = mpsc::channel(1);
        let (context, lifecycle_authority) =
            client_context_with_lifecycle(ServerPhase::Running, wake_sender, 9);
        let cycle = context.lifecycle.borrow().owned_running_cycle().unwrap();
        let lease = context.backend_use.acquire_shared(cycle).await;
        let (mut backend, mut backend_peer) = connected_pair().await;
        let handshake = handshake_frame(12_345, 2);
        let mut replayed = vec![0; handshake.len()];
        let (write, read) = tokio::join!(
            backend.write_all(&handshake),
            backend_peer.read_exact(&mut replayed)
        );
        write.unwrap();
        read.unwrap();

        lifecycle_authority.send_replace(LifecycleState::test_phase(ServerPhase::Stopping));
        let result = establish_owned_proxy_activity(&context, cycle, HandshakeIntent::Login, lease);
        assert!(result.is_err());
        let snapshot = context.backend_use.activity_snapshot().unwrap();
        assert_eq!(snapshot.active_login_transfer_sessions(), 0);
        assert_eq!(snapshot.last_final_session_disconnect(), None);
    }

    #[tokio::test]
    async fn failed_replay_records_no_session_activity() {
        let (wake_sender, _wake_receiver) = mpsc::channel(1);
        let (context, _lifecycle_authority) =
            client_context_with_lifecycle(ServerPhase::Running, wake_sender, 9);
        let cycle = context.lifecycle.borrow().owned_running_cycle().unwrap();
        let lease = context.backend_use.acquire_shared(cycle).await;
        let handshake = handshake_frame(12_345, 2);
        let (_client, connection, peer) = parsed_connection(&handshake).await;
        let (mut backend, reset_peer) = connected_pair().await;
        reset_peer
            .set_zero_linger()
            .expect("test backend should support abortive close");
        drop(reset_peer);
        backend
            .read_u8()
            .await
            .expect_err("abortive close should reset the backend socket");

        let error = proxy_connected_owned(connection, backend, peer, &context, cycle, lease)
            .await
            .expect_err("reset backend should fail handshake replay");
        assert!(error.to_string().contains("failed to replay"));
        let snapshot = context.backend_use.activity_snapshot().unwrap();
        assert_eq!(snapshot.active_login_transfer_sessions(), 0);
        assert_eq!(snapshot.last_final_session_disconnect(), None);
    }

    #[tokio::test]
    async fn clean_close_proxy_error_and_task_abort_release_login_sessions() {
        enum Termination {
            CleanClose,
            ProxyError,
            TaskAbort,
        }

        for termination in [
            Termination::CleanClose,
            Termination::ProxyError,
            Termination::TaskAbort,
        ] {
            let backend_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let backend_port = backend_listener.local_addr().unwrap().port();
            let (wake_sender, _wake_receiver) = mpsc::channel(1);
            let (context, _lifecycle_authority) =
                client_context_with_lifecycle(ServerPhase::Running, wake_sender, backend_port);
            let backend_use = Arc::clone(&context.backend_use);
            let (mut client, server) = connected_pair().await;
            let peer = server.peer_addr().unwrap();
            let handler = tokio::spawn(handle_client(server, peer, context));
            let handshake = handshake_frame(12_345, 2);
            client.write_all(&handshake).await.unwrap();
            let (mut backend, _) = backend_listener.accept().await.unwrap();
            let mut replayed = vec![0; handshake.len()];
            backend.read_exact(&mut replayed).await.unwrap();
            assert_eq!(replayed, handshake);
            wait_for_active_sessions(&backend_use, 1).await;

            match termination {
                Termination::CleanClose => {
                    drop((client, backend));
                    handler
                        .await
                        .expect("client task should not panic")
                        .expect("clean socket close should end the proxy");
                }
                Termination::ProxyError => {
                    backend.set_zero_linger().unwrap();
                    drop(backend);
                    let result = handler.await.expect("client task should not panic");
                    assert!(result.is_err());
                    drop(client);
                }
                Termination::TaskAbort => {
                    handler.abort();
                    assert!(handler.await.unwrap_err().is_cancelled());
                    drop((client, backend));
                }
            }
            let snapshot = backend_use.activity_snapshot().unwrap();
            assert_eq!(snapshot.active_login_transfer_sessions(), 0);
            assert!(snapshot.last_final_session_disconnect().is_some());
        }
    }

    #[tokio::test]
    async fn status_releases_lease_after_replay_without_changing_activity() {
        let backend_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let backend_port = backend_listener.local_addr().unwrap().port();
        let (wake_sender, _wake_receiver) = mpsc::channel(1);
        let (context, _lifecycle_authority) =
            client_context_with_lifecycle(ServerPhase::Running, wake_sender, backend_port);
        let backend_use = Arc::clone(&context.backend_use);
        let (mut client, server) = connected_pair().await;
        let peer = server.peer_addr().unwrap();
        let handler = tokio::spawn(handle_client(server, peer, context));
        let handshake = handshake_frame(12_345, 1);
        client.write_all(&handshake).await.unwrap();
        let (mut backend, _) = backend_listener.accept().await.unwrap();
        let mut replayed = vec![0; handshake.len()];
        backend.read_exact(&mut replayed).await.unwrap();
        assert_eq!(replayed, handshake);

        let exclusive = timeout(Duration::from_secs(2), backend_use.acquire_exclusive())
            .await
            .expect("status lease should be released after replay");
        backend.write_all(b"x").await.unwrap();
        assert_eq!(client.read_u8().await.unwrap(), b'x');
        let snapshot = backend_use.activity_snapshot().unwrap();
        assert_eq!(snapshot.active_login_transfer_sessions(), 0);
        assert_eq!(snapshot.last_final_session_disconnect(), None);
        drop(exclusive);
        drop((client, backend));
        handler
            .await
            .expect("status proxy task should not panic")
            .expect("status proxy should close cleanly");
        assert_eq!(
            backend_use
                .activity_snapshot()
                .unwrap()
                .last_final_session_disconnect(),
            None
        );
    }

    #[tokio::test]
    async fn sleeping_unknown_intent_closes_without_response_or_wake() {
        let (wake_sender, mut wake_receiver) = mpsc::channel(1);
        let (context, _lifecycle_authority) =
            client_context_with_lifecycle(ServerPhase::Stopped, wake_sender, 9);
        let (mut client, server) = connected_pair().await;
        let peer = server.peer_addr().expect("client peer address");
        let handler = tokio::spawn(handle_client(server, peer, context));

        client
            .write_all(&handshake_frame(12_345, 77))
            .await
            .expect("unknown-intent handshake should write");
        let mut response = Vec::new();
        client
            .read_to_end(&mut response)
            .await
            .expect("connection should close cleanly");
        handler
            .await
            .expect("client task should not panic")
            .expect("unknown sleeping intent should close cleanly");

        assert!(response.is_empty());
        assert_no_wake(&mut wake_receiver);
    }

    #[tokio::test]
    async fn sleeping_incompatible_login_disconnects_without_login_start_or_wake() {
        let version = crate::minecraft::MinecraftVersion::latest();
        let unsupported_protocol = version.protocol() - 1;
        let (wake_sender, mut wake_receiver) = mpsc::channel(1);
        let (context, _lifecycle_authority) =
            client_context_with_lifecycle(ServerPhase::Stopped, wake_sender, 9);
        let (mut client, server) = connected_pair().await;
        let peer = server.peer_addr().expect("client peer address");
        let handler = tokio::spawn(handle_client(server, peer, context));

        client
            .write_all(&handshake_frame(unsupported_protocol, 2))
            .await
            .expect("incompatible handshake should write");
        let mut response = Vec::new();
        client
            .read_to_end(&mut response)
            .await
            .expect("incompatible disconnect should arrive");
        handler
            .await
            .expect("client task should not panic")
            .expect("incompatible login should be handled");

        assert!(String::from_utf8_lossy(&response).contains("This server requires"));
        assert_no_wake(&mut wake_receiver);
    }

    #[tokio::test]
    async fn sleeping_login_transfer_and_cooldown_queue_wakes() {
        for (phase, intent) in [
            (ServerPhase::Stopped, 2),
            (ServerPhase::Stopped, 3),
            (ServerPhase::Cooldown, 2),
        ] {
            let (wake_sender, mut wake_receiver) = mpsc::channel(1);
            let (context, _lifecycle_authority) =
                client_context_with_lifecycle(phase, wake_sender, 9);
            let observed = *context.lifecycle.borrow();
            let (mut client, server) = connected_pair().await;
            let peer = server.peer_addr().expect("client peer address");
            let handler = tokio::spawn(handle_client(server, peer, context));

            client
                .write_all(&login_exchange_with_intent(intent))
                .await
                .expect("login exchange should write");
            let mut response = Vec::new();
            client
                .read_to_end(&mut response)
                .await
                .expect("login disconnect should arrive");
            handler
                .await
                .expect("client task should not panic")
                .expect("sleeping login should be handled");

            assert!(!response.is_empty());
            assert_eq!(
                wake_receiver.try_recv().expect("wake should be queued"),
                WakeRequest::observed(observed)
            );
        }
    }

    #[tokio::test]
    async fn lifecycle_change_from_stopped_to_running_during_handshake_proxies() {
        let backend_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("backend listener should bind");
        let backend_port = backend_listener
            .local_addr()
            .expect("backend listener has an address")
            .port();
        let (wake_sender, mut wake_receiver) = mpsc::channel(1);
        let (context, lifecycle_sender) =
            client_context_with_lifecycle(ServerPhase::Stopped, wake_sender, backend_port);
        let backend_use = Arc::clone(&context.backend_use);
        let (mut client, server) = connected_pair().await;
        let peer = server.peer_addr().expect("client peer address");
        let handler = tokio::spawn(handle_client(server, peer, context));
        let handshake = handshake_frame(12_345, 77);
        let expected = handshake.clone();

        client
            .write_all(&handshake[..1])
            .await
            .expect("first handshake byte should write");
        let running = LifecycleState::test_phase(ServerPhase::Running);
        backend_use.begin_cycle(running.owned_running_cycle().unwrap(), Instant::now());
        lifecycle_sender.send_replace(running);
        client
            .write_all(&handshake[1..])
            .await
            .expect("remaining handshake should write");

        let (mut backend, _) = timeout(Duration::from_secs(1), backend_listener.accept())
            .await
            .expect("running lifecycle should connect to backend")
            .expect("backend should accept proxy");
        let mut replayed = vec![0; expected.len()];
        backend
            .read_exact(&mut replayed)
            .await
            .expect("handshake replay should arrive");
        assert_eq!(replayed, expected);
        backend
            .write_all(b"running")
            .await
            .expect("backend response should write");
        let mut response = [0; 7];
        client
            .read_exact(&mut response)
            .await
            .expect("backend response should arrive");
        assert_eq!(&response, b"running");
        drop((client, backend));
        handler
            .await
            .expect("client task should not panic")
            .expect("connection should proxy");
        assert_no_wake(&mut wake_receiver);
    }

    #[tokio::test]
    async fn lifecycle_change_from_running_to_stopped_during_handshake_sleeps() {
        let backend_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("backend listener should bind");
        let backend_port = backend_listener
            .local_addr()
            .expect("backend listener has an address")
            .port();
        let (wake_sender, mut wake_receiver) = mpsc::channel(1);
        let (context, lifecycle_sender) =
            client_context_with_lifecycle(ServerPhase::Running, wake_sender, backend_port);
        let (mut client, server) = connected_pair().await;
        let peer = server.peer_addr().expect("client peer address");
        let handler = tokio::spawn(handle_client(server, peer, context));
        let exchange = login_exchange();
        let stopped = LifecycleState::test_phase(ServerPhase::Stopped);

        client
            .write_all(&exchange[..1])
            .await
            .expect("first handshake byte should write");
        lifecycle_sender.send_replace(stopped);
        client
            .write_all(&exchange[1..])
            .await
            .expect("remaining exchange should write");
        let mut response = Vec::new();
        client
            .read_to_end(&mut response)
            .await
            .expect("sleeping disconnect should arrive");
        handler
            .await
            .expect("client task should not panic")
            .expect("connection should use sleeping behavior");

        assert!(!response.is_empty());
        assert_eq!(
            wake_receiver.try_recv().expect("wake should be queued"),
            WakeRequest::observed(stopped)
        );
        assert!(
            timeout(Duration::from_millis(100), backend_listener.accept())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn lifecycle_snapshot_remains_frozen_after_sampling() {
        let (wake_sender, mut wake_receiver) = mpsc::channel(1);
        let (context, lifecycle_sender) =
            client_context_with_lifecycle(ServerPhase::Stopped, wake_sender, 9);
        let (mut client, server) = connected_pair().await;
        let peer = server.peer_addr().expect("client peer address");
        let deadline = Instant::now() + context.handshake_timeout;
        client
            .write_all(&login_exchange())
            .await
            .expect("login exchange should write");
        let connection = ConnectionEnvelope::read(server, deadline)
            .await
            .expect("handshake should parse");
        let observed = *context.lifecycle.borrow();
        let running = LifecycleState::test_phase(ServerPhase::Running);
        context
            .backend_use
            .begin_cycle(running.owned_running_cycle().unwrap(), Instant::now());
        lifecycle_sender.send_replace(running);
        let handler = tokio::spawn(handle_sampled_connection(
            connection, peer, context, observed, deadline,
        ));

        let mut response = Vec::new();
        client
            .read_to_end(&mut response)
            .await
            .expect("frozen sleeping disconnect should arrive");
        handler
            .await
            .expect("client task should not panic")
            .expect("frozen snapshot should be handled");
        assert!(!response.is_empty());
        assert_eq!(
            wake_receiver.try_recv().expect("wake should be queued"),
            WakeRequest::observed(observed)
        );
    }

    #[tokio::test]
    async fn active_backend_failure_does_not_fall_back_or_wake() {
        let (wake_sender, mut wake_receiver) = mpsc::channel(1);
        let (mut context, _lifecycle_authority) =
            client_context_with_lifecycle(ServerPhase::Running, wake_sender, 0);
        context.proxy_connect_timeout = Duration::from_millis(25);
        let backend_use = Arc::clone(&context.backend_use);
        let (mut client, server) = connected_pair().await;
        let peer = server.peer_addr().expect("client peer address");
        let handler = tokio::spawn(handle_client(server, peer, context));

        client
            .write_all(&handshake_frame(
                crate::minecraft::MinecraftVersion::latest().protocol(),
                2,
            ))
            .await
            .expect("active handshake should write");
        let mut response = Vec::new();
        client
            .read_to_end(&mut response)
            .await
            .expect("failed proxy should close the client");
        let error = handler
            .await
            .expect("client task should not panic")
            .expect_err("backend connection should fail");

        assert!(error.to_string().contains("could not connect"));
        assert!(response.is_empty());
        let snapshot = backend_use.activity_snapshot().unwrap();
        assert_eq!(snapshot.active_login_transfer_sessions(), 0);
        assert_eq!(snapshot.last_final_session_disconnect(), None);
        assert_no_wake(&mut wake_receiver);
    }

    #[tokio::test]
    async fn conflict_disconnects_exactly_and_coalesces_demand() {
        let (wake_sender, mut wake_receiver) = mpsc::channel(1);
        let (context, _lifecycle_authority) =
            client_context_with_lifecycle(ServerPhase::Conflict, wake_sender, 9);
        let mut responses = Vec::new();

        for _ in 0..2 {
            let (mut client, server) = connected_pair().await;
            let peer = server.peer_addr().expect("client peer address");
            let handler = tokio::spawn(handle_client(server, peer, context.clone()));
            client
                .write_all(&login_exchange())
                .await
                .expect("login exchange should write");
            let mut response = Vec::new();
            client
                .read_to_end(&mut response)
                .await
                .expect("conflict disconnect should arrive");
            handler
                .await
                .expect("client task should not panic")
                .expect("conflict handling should succeed");
            responses.push(response);
        }

        assert_eq!(responses[0], responses[1]);
        let text = String::from_utf8_lossy(&responses[0]);
        assert!(text.contains("MCServerNap cannot safely start the server"));
        assert!(wake_receiver.try_recv().is_ok());
        assert!(matches!(
            wake_receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn binding_is_inert_until_activation() {
        let rcon_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let backend_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let launch_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = construction_config(
            rcon_listener.local_addr().unwrap(),
            backend_listener.local_addr().unwrap(),
            launch_listener.local_addr().unwrap(),
        );
        let bound = ServerRuntime::prepare(config, test_responder(), Arc::new(Semaphore::new(1)))
            .expect("runtime should prepare")
            .bind()
            .await
            .expect("runtime should bind without activating");

        let no_rcon = timeout(Duration::from_millis(50), rcon_listener.accept());
        let no_backend = timeout(Duration::from_millis(50), backend_listener.accept());
        let no_launch = timeout(Duration::from_millis(50), launch_listener.accept());
        let (no_rcon, no_backend, no_launch) = tokio::join!(no_rcon, no_backend, no_launch);
        assert!(no_rcon.is_err(), "binding must not probe RCON");
        assert!(no_backend.is_err(), "binding must not probe the backend");
        assert!(no_launch.is_err(), "binding must not launch a process");

        let runtime = bound.activate();
        assert_eq!(
            runtime.client_context.lifecycle.borrow().phase(),
            ServerPhase::Reconciling
        );
        let rcon_accept = timeout(Duration::from_secs(2), rcon_listener.accept());
        let backend_accept = timeout(Duration::from_secs(2), backend_listener.accept());
        let (rcon_accept, backend_accept) = tokio::join!(rcon_accept, backend_accept);
        let _rcon_connection = rcon_accept
            .expect("activation should start RCON reconciliation")
            .expect("RCON reconciliation connection should be accepted");
        let _backend_connection = backend_accept
            .expect("activation should start backend reconciliation")
            .expect("backend reconciliation connection should be accepted");
        assert!(!runtime.supervisor.is_finished());
        assert_eq!(
            runtime.client_context.lifecycle.borrow().phase(),
            ServerPhase::Reconciling
        );

        let second_rcon = timeout(Duration::from_millis(50), rcon_listener.accept());
        let second_backend = timeout(Duration::from_millis(50), backend_listener.accept());
        let process_start = timeout(Duration::from_millis(50), launch_listener.accept());
        let (second_rcon, second_backend, process_start) =
            tokio::join!(second_rcon, second_backend, process_start);
        assert!(second_rcon.is_err(), "activation must start one supervisor");
        assert!(
            second_backend.is_err(),
            "activation must run one startup probe"
        );
        assert!(
            process_start.is_err(),
            "reconciliation must not launch without demand"
        );

        runtime
            .run_until(async { Ok(()) })
            .await
            .expect("activated runtime should shut down cleanly");
    }

    #[tokio::test]
    async fn dropping_a_bound_runtime_releases_its_listener() {
        let executable = std::env::current_exe().expect("test executable path should be known");
        let bound = ServerRuntime::prepare(
            runtime_config(
                executable.to_string_lossy().into_owned(),
                Vec::new(),
                Endpoint::new("127.0.0.1", 9),
            ),
            test_responder(),
            Arc::new(Semaphore::new(1)),
        )
        .expect("runtime should prepare")
        .bind()
        .await
        .expect("runtime should bind");
        let address = bound.listener.local_addr().unwrap();

        drop(bound);
        TcpListener::bind(address)
            .await
            .expect("dropping the bound runtime should release its listener");
    }

    #[tokio::test]
    async fn bind_failure_has_no_runtime_work_to_clean_up() {
        let public_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let rcon_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let backend_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let launch_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = construction_config(
            rcon_listener.local_addr().unwrap(),
            backend_listener.local_addr().unwrap(),
            launch_listener.local_addr().unwrap(),
        );
        config.bind_endpoint = endpoint(public_listener.local_addr().unwrap());
        let prepared =
            ServerRuntime::prepare(config, test_responder(), Arc::new(Semaphore::new(1)))
                .expect("runtime should prepare before binding");

        assert!(prepared.bind().await.is_err());
        let no_rcon = timeout(Duration::from_millis(50), rcon_listener.accept());
        let no_backend = timeout(Duration::from_millis(50), backend_listener.accept());
        let no_launch = timeout(Duration::from_millis(50), launch_listener.accept());
        let (no_rcon, no_backend, no_launch) = tokio::join!(no_rcon, no_backend, no_launch);
        assert!(no_rcon.is_err());
        assert!(no_backend.is_err());
        assert!(no_launch.is_err());
    }

    #[tokio::test]
    async fn shutdown_watchdog_is_validated_before_listener_binding() {
        let reserved_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let reserved_address = reserved_listener.local_addr().unwrap();
        let executable = std::env::current_exe().expect("test executable path should be known");
        let mut config = runtime_config(
            executable.to_string_lossy().into_owned(),
            Vec::new(),
            Endpoint::new("127.0.0.1", 9),
        );
        config.bind_endpoint = endpoint(reserved_address);
        config.supervisor.command_timeout = Duration::MAX;

        let Err(error) =
            ServerRuntime::prepare(config, test_responder(), Arc::new(Semaphore::new(1)))
        else {
            panic!("overflowing watchdog should fail preparation");
        };
        assert!(error.to_string().contains("command_timeout_seconds"));

        drop(reserved_listener);
        TcpListener::bind(reserved_address)
            .await
            .expect("preparation must not have bound the public listener");
    }

    #[tokio::test]
    async fn activated_runtimes_share_connection_limit() {
        let executable = std::env::current_exe().expect("test executable path should be known");
        let connection_limit = Arc::new(Semaphore::new(1));
        let first = ServerRuntime::prepare(
            runtime_config(
                executable.to_string_lossy().into_owned(),
                Vec::new(),
                Endpoint::new("127.0.0.1", 9),
            ),
            test_responder(),
            Arc::clone(&connection_limit),
        )
        .unwrap()
        .bind()
        .await
        .unwrap()
        .activate();
        let second = ServerRuntime::prepare(
            runtime_config(
                executable.to_string_lossy().into_owned(),
                Vec::new(),
                Endpoint::new("127.0.0.1", 9),
            ),
            test_responder(),
            Arc::clone(&connection_limit),
        )
        .unwrap()
        .bind()
        .await
        .unwrap()
        .activate();

        assert!(Arc::ptr_eq(
            &first.connection_limit,
            &second.connection_limit
        ));
        let permit = Arc::clone(&first.connection_limit)
            .try_acquire_owned()
            .expect("shared limiter should have one permit");
        assert_eq!(second.connection_limit.available_permits(), 0);
        drop(permit);
        assert_eq!(first.connection_limit.available_permits(), 1);

        let (first_result, second_result) = tokio::join!(
            first.run_until(async { Ok(()) }),
            second.run_until(async { Ok(()) })
        );
        first_result.expect("first runtime should shut down");
        second_result.expect("second runtime should shut down");
    }

    async fn assert_structured_endpoint_connects(listener: TcpListener, endpoint: Endpoint) {
        let (connection, accepted) = timeout(Duration::from_secs(3), async {
            tokio::join!(
                connect_backend(&endpoint, Duration::from_secs(2)),
                listener.accept()
            )
        })
        .await
        .expect("structured endpoint test should finish");
        connection.expect("structured endpoint should connect");
        accepted.expect("listener should accept the structured endpoint connection");
    }

    #[tokio::test]
    async fn structured_endpoints_connect_over_ipv4_hostname_and_ipv6() {
        let ipv4 = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let ipv4_endpoint = Endpoint::new("127.0.0.1", ipv4.local_addr().unwrap().port());
        assert_structured_endpoint_connects(ipv4, ipv4_endpoint).await;

        let hostname = TcpListener::bind(("localhost", 0)).await.unwrap();
        let hostname_endpoint = Endpoint::new("localhost", hostname.local_addr().unwrap().port());
        assert_structured_endpoint_connects(hostname, hostname_endpoint).await;

        let ipv6 = TcpListener::bind(("::1", 0)).await.unwrap();
        let ipv6_endpoint = Endpoint::new("[::1]", ipv6.local_addr().unwrap().port());
        assert_structured_endpoint_connects(ipv6, ipv6_endpoint).await;
    }

    #[tokio::test]
    async fn activation_starts_in_reconciling_phase() {
        let rcon_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test RCON listener should bind");
        let backend_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test backend listener should bind");
        let executable = std::env::current_exe().expect("test executable path should be known");
        let mut config = runtime_config(
            executable.to_string_lossy().into_owned(),
            Vec::new(),
            Endpoint::new(
                "127.0.0.1",
                rcon_listener
                    .local_addr()
                    .expect("RCON listener has an address")
                    .port(),
            ),
        );
        config.backend_endpoint = Endpoint::new(
            "127.0.0.1",
            backend_listener
                .local_addr()
                .expect("backend listener has an address")
                .port(),
        );
        let runtime = bind_test_runtime(config).await;

        assert_eq!(
            runtime.client_context.lifecycle.borrow().phase(),
            ServerPhase::Reconciling
        );
        runtime
            .run_until(async { Ok(()) })
            .await
            .expect("runtime shutdown should succeed");
        drop((rcon_listener, backend_listener));
    }

    #[tokio::test]
    async fn completed_client_task_is_reaped_before_ready_listener() {
        let executable = std::env::current_exe().expect("test executable path should be known");
        let mut runtime = bind_test_runtime(runtime_config(
            executable.to_string_lossy().into_owned(),
            Vec::new(),
            Endpoint::new("127.0.0.1", 9),
        ))
        .await;
        let address = runtime
            .listener
            .local_addr()
            .expect("test listener should have an address");
        let completed_task = runtime.connections.spawn(async {});
        timeout(Duration::from_secs(2), async {
            while !completed_task.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("test client task should complete");
        let _client = TcpStream::connect(address)
            .await
            .expect("listener should have a connection ready to accept");

        let connection_limit = Arc::clone(&runtime.connection_limit);
        let (observation_sender, observation_receiver) = oneshot::channel();
        runtime
            .run_until(async move {
                tokio::task::yield_now().await;
                observation_sender
                    .send(connection_limit.available_permits())
                    .expect("test should still await the permit observation");
                Ok(())
            })
            .await
            .expect("runtime shutdown should succeed");

        assert_eq!(
            observation_receiver
                .await
                .expect("shutdown should report available permits"),
            1
        );
    }

    #[tokio::test]
    async fn client_task_panic_is_fatal_after_normal_runtime_cleanup() {
        let executable = std::env::current_exe().unwrap();
        let mut runtime = bind_test_runtime(runtime_config(
            executable.to_string_lossy().into_owned(),
            Vec::new(),
            Endpoint::new("127.0.0.1", 9),
        ))
        .await;
        runtime.connections.spawn(async {
            panic!("intentional client task panic");
        });

        let error = timeout(
            Duration::from_secs(2),
            runtime.run_until(std::future::pending()),
        )
        .await
        .expect("fatal client panic cleanup should be bounded")
        .expect_err("client panic must fail the runtime");
        assert!(error.to_string().contains("client task panicked"));
    }

    #[tokio::test]
    async fn client_panic_racing_shutdown_remains_fatal() {
        let executable = std::env::current_exe().unwrap();
        let mut runtime = bind_test_runtime(runtime_config(
            executable.to_string_lossy().into_owned(),
            Vec::new(),
            Endpoint::new("127.0.0.1", 9),
        ))
        .await;
        let (panic_sender, panic_receiver) = oneshot::channel();
        let client_task = runtime.connections.spawn(async {
            panic_receiver.await.unwrap();
            panic!("intentional shutdown-race panic");
        });

        let error = runtime
            .run_until(async move {
                panic_sender.send(()).unwrap();
                while !client_task.is_finished() {
                    tokio::task::yield_now().await;
                }
                Ok(())
            })
            .await
            .expect_err("a client panic concurrent with shutdown must remain fatal");
        assert!(error.to_string().contains("client task panicked"));
    }

    #[tokio::test]
    async fn supervisor_panic_reports_uncertain_cleanup() {
        let executable = std::env::current_exe().unwrap();
        let mut runtime = bind_test_runtime(runtime_config(
            executable.to_string_lossy().into_owned(),
            Vec::new(),
            Endpoint::new("127.0.0.1", 9),
        ))
        .await;
        runtime.supervisor.abort();
        let _ = (&mut runtime.supervisor).await;
        runtime.supervisor = tokio::spawn(async {
            panic!("intentional supervisor panic");
        });

        let error = timeout(
            Duration::from_secs(2),
            runtime.run_until(std::future::pending()),
        )
        .await
        .expect("supervisor panic should be observed")
        .expect_err("supervisor panic must fail the runtime");
        assert!(error.to_string().contains("supervisor panicked"));
        assert!(
            error
                .to_string()
                .contains("cleanup of this runtime is uncertain")
        );
    }

    #[tokio::test]
    async fn caller_shutdown_releases_listener_and_client_tasks() {
        let executable = std::env::current_exe().expect("test executable path should be known");
        let runtime = bind_test_runtime(runtime_config(
            executable.to_string_lossy().into_owned(),
            Vec::new(),
            Endpoint::new("127.0.0.1", 9),
        ))
        .await;
        let address = runtime
            .listener
            .local_addr()
            .expect("test listener should have an address");
        let connection_limit = Arc::clone(&runtime.connection_limit);
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let runtime_task = tokio::spawn(runtime.run_until(async move {
            shutdown_receiver
                .await
                .context("test shutdown sender was dropped")?;
            Ok(())
        }));

        let mut client = TcpStream::connect(address)
            .await
            .expect("test client should connect");
        timeout(Duration::from_secs(2), async {
            while connection_limit.available_permits() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("runtime should start a client task");

        shutdown_sender
            .send(())
            .expect("runtime should still await shutdown");
        timeout(Duration::from_secs(2), runtime_task)
            .await
            .expect("runtime shutdown should be bounded")
            .expect("runtime task should not panic")
            .expect("runtime shutdown should succeed");

        assert_eq!(connection_limit.available_permits(), 1);
        assert!(
            timeout(Duration::from_secs(2), client.read_u8())
                .await
                .expect("client connection should close")
                .is_err()
        );
        drop(client);
        TcpListener::bind(address)
            .await
            .expect("runtime should release its listener");
    }

    #[test]
    fn supervisor_watchdog_includes_all_shutdown_budgets() {
        let command_delivery = Duration::from_secs(7);
        let graceful_exit = Duration::from_secs(11);

        assert_eq!(
            supervisor_shutdown_wait_limit(command_delivery, graceful_exit).unwrap(),
            command_delivery + graceful_exit + SUPERVISOR_SHUTDOWN_MARGIN
        );
    }

    #[test]
    fn default_cleanup_fits_watchdog() {
        let command_delivery = Duration::from_secs(10);
        let graceful_exit = Duration::from_secs(30);
        let forced_reaping = Duration::from_secs(5);
        let watchdog = supervisor_shutdown_wait_limit(command_delivery, graceful_exit).unwrap();

        assert_eq!(watchdog, Duration::from_secs(50));
        assert!(command_delivery + graceful_exit + forced_reaping <= watchdog);
    }

    #[test]
    fn supervisor_watchdog_rejects_duration_overflow() {
        let error = supervisor_shutdown_wait_limit(Duration::MAX, Duration::from_nanos(1))
            .expect_err("overflowing shutdown budgets must be rejected");
        let message = error.to_string();

        assert!(message.contains("command_timeout_seconds"));
        assert!(message.contains("shutdown_timeout_seconds"));
        assert!(message.contains("reduce"));
    }

    #[tokio::test]
    async fn supervisor_timeout_aborts_stalled_cleanup() {
        let mut supervisor = tokio::spawn(std::future::pending::<Result<()>>());

        let error = timeout(
            Duration::from_secs(1),
            await_supervisor_shutdown(&mut supervisor, Duration::from_millis(1)),
        )
        .await
        .expect("supervisor cleanup failure should be bounded")
        .expect_err("stalled supervisor cleanup should fail");
        assert!(error.to_string().contains("did not stop within"));
        assert!(supervisor.is_finished());
    }

    #[tokio::test]
    async fn shutdown_failure_waits_for_supervisor_cleanup() {
        let executable = std::env::current_exe().expect("test executable path should be known");
        let mut runtime = bind_test_runtime(runtime_config(
            executable.to_string_lossy().into_owned(),
            Vec::new(),
            Endpoint::new("127.0.0.1", 9),
        ))
        .await;
        runtime.supervisor.abort();
        let _ = (&mut runtime.supervisor).await;

        let (supervisor_shutdown, supervisor_shutdown_receiver) = oneshot::channel();
        let (cleanup_started_sender, cleanup_started_receiver) = oneshot::channel();
        let (cleanup_release_sender, cleanup_release_receiver) = oneshot::channel();
        runtime.supervisor_shutdown = supervisor_shutdown;
        runtime.supervisor = tokio::spawn(async move {
            supervisor_shutdown_receiver
                .await
                .expect("runtime should request supervisor shutdown");
            cleanup_started_sender
                .send(())
                .expect("test should await cleanup startup");
            cleanup_release_receiver
                .await
                .expect("test should release supervisor cleanup");
            Ok(())
        });

        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let mut runtime_task = tokio::spawn(runtime.run_until(async move {
            shutdown_receiver
                .await
                .context("test shutdown sender was dropped")?;
            Err(anyhow!("test shutdown source failed"))
        }));

        shutdown_sender
            .send(())
            .expect("runtime should still await shutdown");
        cleanup_started_receiver
            .await
            .expect("supervisor cleanup should start");
        assert!(!runtime_task.is_finished());
        cleanup_release_sender
            .send(())
            .expect("supervisor cleanup should still be pending");
        let error = timeout(Duration::from_secs(12), &mut runtime_task)
            .await
            .expect("runtime should finish after process cleanup")
            .expect("runtime task should not panic")
            .expect_err("caller shutdown failure should be returned");
        assert_eq!(error.to_string(), "test shutdown source failed");
    }
}
