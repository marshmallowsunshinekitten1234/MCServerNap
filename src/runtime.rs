use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, TryAcquireError, mpsc, oneshot, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{Instant, sleep, sleep_until, timeout};

use crate::minecraft::{ClientRequest, MinecraftResponder};
use crate::supervisor::{
    self, BackendEndpoint, LifecycleState, ServerPhase, SupervisorConfig, WakeRequest,
};

const PENDING_WAKE_CAPACITY: usize = 1;
const SUPERVISOR_SHUTDOWN_MARGIN: Duration = Duration::from_secs(10);

pub struct ServerRuntimeConfig {
    pub bind_host: String,
    pub bind_port: u16,
    pub server_host: String,
    pub server_port: u16,
    pub handshake_timeout: Duration,
    pub proxy_connect_timeout: Duration,
    pub max_connections: usize,
    pub supervisor: SupervisorConfig,
}

pub struct ServerRuntime {
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
    lifecycle: watch::Receiver<LifecycleState>,
    wake_requests: mpsc::Sender<WakeRequest>,
    responder: Arc<MinecraftResponder>,
    server_host: Arc<str>,
    server_port: u16,
    handshake_timeout: Duration,
    proxy_connect_timeout: Duration,
}

enum RunExit {
    Shutdown(Result<()>),
    Supervisor(std::result::Result<Result<()>, tokio::task::JoinError>),
}

impl ServerRuntime {
    pub async fn bind(config: ServerRuntimeConfig, responder: MinecraftResponder) -> Result<Self> {
        let ServerRuntimeConfig {
            bind_host,
            bind_port,
            server_host,
            server_port,
            handshake_timeout,
            proxy_connect_timeout,
            max_connections,
            supervisor: supervisor_config,
        } = config;
        let listener = TcpListener::bind((bind_host.as_str(), bind_port))
            .await
            .with_context(|| format!("failed to listen on {bind_host}:{bind_port}"))?;
        log::info!("Listening for Minecraft clients on {bind_host}:{bind_port}");

        let (wake_requests, wake_receiver) = mpsc::channel(PENDING_WAKE_CAPACITY);
        let (supervisor_shutdown, shutdown_receiver) = oneshot::channel();
        let (lifecycle_sender, lifecycle) = watch::channel(LifecycleState::reconciling());
        let supervisor_wait_limit = supervisor_config.shutdown_timeout + SUPERVISOR_SHUTDOWN_MARGIN;
        let server_host: Arc<str> = Arc::from(server_host);
        let supervisor = tokio::spawn(supervisor::run(
            wake_receiver,
            shutdown_receiver,
            lifecycle_sender,
            supervisor_config,
            BackendEndpoint::network(Arc::clone(&server_host), server_port, proxy_connect_timeout),
        ));
        let client_context = ClientContext {
            lifecycle,
            wake_requests,
            responder: Arc::new(responder),
            server_host,
            server_port,
            handshake_timeout,
            proxy_connect_timeout,
        };

        Ok(Self {
            listener,
            client_context,
            connection_limit: Arc::new(Semaphore::new(max_connections)),
            connections: JoinSet::new(),
            supervisor,
            supervisor_shutdown,
            supervisor_wait_limit,
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
                        log::warn!("Client task failed: {error}");
                    }
                }
                accepted = self.listener.accept() => {
                    let (socket, peer) = match accepted {
                        Ok(connection) => connection,
                        Err(error) => {
                            log::warn!("Failed to accept a client connection: {error}");
                            sleep(Duration::from_millis(100)).await;
                            continue;
                        }
                    };

                    let permit = match Arc::clone(&self.connection_limit).try_acquire_owned() {
                        Ok(permit) => permit,
                        Err(TryAcquireError::NoPermits) => {
                            log::warn!("Dropping connection from {peer}: connection limit reached");
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
                        if let Err(error) = handle_client(socket, peer, context).await {
                            log::debug!("Connection from {peer} ended: {error:#}");
                        }
                    });
                }
            }
        };

        self.connections.abort_all();
        while self.connections.join_next().await.is_some() {}

        match exit {
            RunExit::Supervisor(result) => {
                result.context("server supervisor task failed")??;
                bail!("server supervisor stopped unexpectedly")
            }
            RunExit::Shutdown(shutdown_result) => {
                let shutdown_requested = self.supervisor_shutdown.send(()).is_ok();
                let mut cleanup_result =
                    await_supervisor_shutdown(&mut self.supervisor, self.supervisor_wait_limit)
                        .await;
                if !shutdown_requested && cleanup_result.is_ok() {
                    cleanup_result = Err(anyhow::anyhow!(
                        "server supervisor stopped before shutdown was requested"
                    ));
                }

                match (shutdown_result, cleanup_result) {
                    (Ok(()), Ok(())) => Ok(()),
                    (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
                    (Err(shutdown_error), Err(cleanup_error)) => Err(shutdown_error
                        .context(format!("runtime cleanup also failed: {cleanup_error:#}"))),
                }
            }
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
    mut socket: TcpStream,
    peer: std::net::SocketAddr,
    context: ClientContext,
) -> Result<()> {
    socket
        .set_nodelay(true)
        .context("failed to enable TCP_NODELAY for client")?;
    let observed_lifecycle = *context.lifecycle.borrow();
    let current_phase = observed_lifecycle.phase();

    if matches!(current_phase, ServerPhase::Running | ServerPhase::External) {
        return proxy_connection(
            socket,
            peer,
            &context.server_host,
            context.server_port,
            context.proxy_connect_timeout,
        )
        .await;
    }

    let operation_timeout = context.handshake_timeout;
    let request = timeout(
        operation_timeout,
        context.responder.read_client_request(&mut socket),
    )
    .await
    .context("initial Minecraft exchange timed out")??;

    match request {
        ClientRequest::Status => {
            context
                .responder
                .serve_status(&mut socket, operation_timeout)
                .await?;
        }
        ClientRequest::UnsupportedProtocol { protocol_version } => {
            log::info!("Rejected login from {peer}: protocol {protocol_version} is not supported");
            context
                .responder
                .send_incompatible_disconnect(&mut socket, operation_timeout)
                .await?;
        }
        ClientRequest::Login { intent } => {
            log::info!("Minecraft {intent:?} request from {peer}");
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
                    Ok(()) => log::info!("Queued server wake-up request from {peer}"),
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        log::debug!("Wake-up request already pending");
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        bail!("server supervisor wake channel is closed");
                    }
                }
            }
            if current_phase == ServerPhase::Conflict {
                context
                    .responder
                    .send_conflict_disconnect(&mut socket, operation_timeout)
                    .await?;
            } else {
                context
                    .responder
                    .send_login_disconnect(&mut socket, operation_timeout)
                    .await?;
            }
        }
    }

    Ok(())
}

async fn proxy_connection(
    mut client: TcpStream,
    peer: std::net::SocketAddr,
    server_host: &str,
    server_port: u16,
    connect_timeout: Duration,
) -> Result<()> {
    let mut backend = connect_backend(server_host, server_port, connect_timeout).await?;
    backend
        .set_nodelay(true)
        .context("failed to enable TCP_NODELAY for backend")?;
    log::debug!("Proxying {peer} to {server_host}:{server_port}");

    let (client_to_server, server_to_client) =
        tokio::io::copy_bidirectional(&mut client, &mut backend)
            .await
            .with_context(|| format!("proxy I/O failed for {peer}"))?;
    log::debug!(
        "Proxy for {peer} closed ({client_to_server} bytes upstream, {server_to_client} downstream)"
    );
    Ok(())
}

async fn connect_backend(host: &str, port: u16, connect_timeout: Duration) -> Result<TcpStream> {
    let deadline = Instant::now() + connect_timeout;
    let mut last_error = None;

    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        let attempt_timeout = remaining.min(Duration::from_secs(1));
        match timeout(attempt_timeout, TcpStream::connect((host, port))).await {
            Ok(Ok(stream)) => return Ok(stream),
            Ok(Err(error)) => last_error = Some(error.to_string()),
            Err(_) => last_error = Some("connection attempt timed out".to_owned()),
        }
        let retry_at = (Instant::now() + Duration::from_millis(100)).min(deadline);
        sleep_until(retry_at).await;
    }

    let detail = last_error.unwrap_or_else(|| "timeout elapsed".to_owned());
    bail!("could not connect to Minecraft backend {host}:{port}: {detail}")
}

#[cfg(test)]
mod tests {
    use anyhow::anyhow;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::sync::oneshot;

    use super::*;
    use crate::config::Config;

    fn runtime_config(
        command: String,
        arguments: Vec<String>,
        rcon_address: String,
    ) -> ServerRuntimeConfig {
        ServerRuntimeConfig {
            bind_host: "127.0.0.1".to_owned(),
            bind_port: 0,
            server_host: "127.0.0.1".to_owned(),
            server_port: 25566,
            handshake_timeout: Duration::from_secs(30),
            proxy_connect_timeout: Duration::from_secs(1),
            max_connections: 1,
            supervisor: SupervisorConfig {
                command,
                arguments,
                rcon_address,
                rcon_password: "secret".to_owned(),
                poll_interval: Duration::from_secs(30),
                idle_timeout: Duration::from_secs(30),
                startup_timeout: Duration::from_secs(30),
                retry_interval: Duration::from_millis(10),
                command_timeout: Duration::from_secs(5),
                shutdown_timeout: Duration::from_millis(100),
            },
        }
    }

    async fn bind_test_runtime(config: ServerRuntimeConfig) -> ServerRuntime {
        let responder = MinecraftResponder::new(&Config::default(), None)
            .expect("test responder should be constructed");
        ServerRuntime::bind(config, responder)
            .await
            .expect("test runtime should bind")
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

    fn login_exchange() -> Vec<u8> {
        let version = crate::minecraft::MinecraftVersion::latest();
        let mut handshake = Vec::new();
        write_varint(0, &mut handshake);
        write_varint(version.protocol(), &mut handshake);
        write_varint(9, &mut handshake);
        handshake.extend_from_slice(b"localhost");
        handshake.extend_from_slice(&25565_u16.to_be_bytes());
        write_varint(2, &mut handshake);

        let mut login = Vec::new();
        write_varint(0, &mut login);
        write_varint(6, &mut login);
        login.extend_from_slice(b"player");
        login.extend_from_slice(&[7; 16]);

        let mut exchange = frame_packet(&handshake);
        exchange.extend_from_slice(&frame_packet(&login));
        exchange
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

    fn client_context(
        phase: ServerPhase,
        wake_requests: mpsc::Sender<WakeRequest>,
        backend_port: u16,
    ) -> ClientContext {
        let (_lifecycle_sender, lifecycle) = watch::channel(LifecycleState::test_phase(phase));
        ClientContext {
            lifecycle,
            wake_requests,
            responder: Arc::new(
                MinecraftResponder::new(&Config::default(), None)
                    .expect("test responder should be constructed"),
            ),
            server_host: Arc::from("127.0.0.1"),
            server_port: backend_port,
            handshake_timeout: Duration::from_secs(1),
            proxy_connect_timeout: Duration::from_secs(1),
        }
    }

    async fn assert_phase_proxies_exact_bytes(phase: ServerPhase) {
        let backend_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("backend listener should bind");
        let backend_port = backend_listener
            .local_addr()
            .expect("backend listener has an address")
            .port();
        let (wake_sender, _wake_receiver) = mpsc::channel(1);
        let context = client_context(phase, wake_sender, backend_port);
        let (mut client, server) = connected_pair().await;
        let peer = server.peer_addr().expect("client peer address");
        let handler = tokio::spawn(handle_client(server, peer, context));
        let backend = tokio::spawn(async move {
            let (mut stream, _) = backend_listener
                .accept()
                .await
                .expect("proxy should connect to backend");
            let mut received = [0; 7];
            stream
                .read_exact(&mut received)
                .await
                .expect("proxy bytes should arrive");
            assert_eq!(received, [0, 1, 2, 0xff, 4, 5, 6]);
            stream
                .write_all(b"backend")
                .await
                .expect("backend response should write");
        });

        client
            .write_all(&[0, 1, 2, 0xff, 4, 5, 6])
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
    }

    #[tokio::test]
    async fn running_and_external_forward_exact_original_bytes() {
        assert_phase_proxies_exact_bytes(ServerPhase::Running).await;
        assert_phase_proxies_exact_bytes(ServerPhase::External).await;
    }

    #[tokio::test]
    async fn conflict_disconnects_exactly_and_coalesces_demand() {
        let (wake_sender, mut wake_receiver) = mpsc::channel(1);
        let context = client_context(ServerPhase::Conflict, wake_sender, 9);
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
    async fn lifecycle_is_reconciling_before_the_supervisor_can_publish() {
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
            rcon_listener
                .local_addr()
                .expect("RCON listener has an address")
                .to_string(),
        );
        config.server_port = backend_listener
            .local_addr()
            .expect("backend listener has an address")
            .port();
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
            "127.0.0.1:9".to_owned(),
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
    async fn caller_shutdown_releases_listener_and_client_tasks() {
        let executable = std::env::current_exe().expect("test executable path should be known");
        let runtime = bind_test_runtime(runtime_config(
            executable.to_string_lossy().into_owned(),
            Vec::new(),
            "127.0.0.1:9".to_owned(),
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
            "127.0.0.1:9".to_owned(),
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
