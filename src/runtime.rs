use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use tokio::io::AsyncWriteExt as _;
use tokio::net::{TcpListener, TcpStream, lookup_host};
use tokio::sync::{Semaphore, TryAcquireError, mpsc, oneshot, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{Instant, sleep, sleep_until, timeout, timeout_at};

use crate::backend_use::{
    BackendCycle, BackendUseCoordinator, BackendUseLease, LoginTransferProxySession,
};
use crate::context::ServerContext;
use crate::endpoint::Endpoint;
use crate::minecraft::{ClientRequest, ConnectionEnvelope, HandshakeIntent, MinecraftResponder};
use crate::supervisor::{
    self, BackendEndpoint, FatalSignal, LifecycleState, LifecycleStatusSnapshot,
    MUTATION_QUEUE_CAPACITY, MutationEnvelope, QuiescenceRequest, ServerPhase, SupervisorConfig,
    WakeLatchRoot, WakeLatchSender, WakeRequest,
};

const SUPERVISOR_SHUTDOWN_MARGIN: Duration = Duration::from_secs(10);
const BACKEND_CONNECT_ATTEMPT_LIMIT: Duration = Duration::from_secs(1);
const BACKEND_CONNECT_ROUND_SPACING: [Duration; 5] = [
    Duration::from_millis(100),
    Duration::from_millis(200),
    Duration::from_millis(400),
    Duration::from_millis(800),
    Duration::from_secs(1),
];

fn supervisor_shutdown_wait_limit(
    command_timeout: Duration,
    shutdown_timeout: Duration,
) -> Result<Duration> {
    command_timeout
        .checked_add(command_timeout)
        .and_then(|combined| combined.checked_add(shutdown_timeout))
        .and_then(|combined| combined.checked_add(SUPERVISOR_SHUTDOWN_MARGIN))
        .context(
            "twice command_timeout_seconds plus shutdown_timeout_seconds and the supervisor shutdown margin exceed the supported duration; reduce one of the timeout settings",
        )
}

pub(crate) struct ServerRuntimeConfig {
    pub(crate) bind_endpoint: Endpoint,
    pub(crate) backend_endpoint: Endpoint,
    pub(crate) handshake_timeout: Duration,
    pub(crate) proxy_connect_timeout: Duration,
    pub(crate) supervisor: SupervisorConfig,
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

pub(crate) struct PreparedServerRuntime {
    bind_endpoint: Endpoint,
    inputs: PreparedRuntimeInputs,
}

pub(crate) struct BoundServerRuntime {
    bind_endpoint: Endpoint,
    listener: TcpListener,
    inputs: PreparedRuntimeInputs,
}

pub(crate) struct ServerRuntime {
    context: ServerContext,
    listener: TcpListener,
    client_context: ClientContext,
    connection_limit: Arc<Semaphore>,
    connections: JoinSet<()>,
    wake_latch: WakeLatchRoot,
    quiescence: mpsc::Receiver<QuiescenceRequest>,
    #[cfg(test)]
    quiescence_requests: mpsc::Sender<QuiescenceRequest>,
    quiescence_closed: bool,
    fatal: watch::Receiver<bool>,
    fatal_signal: FatalSignal,
    runtime_drained: watch::Sender<bool>,
    supervisor: JoinHandle<Result<()>>,
    supervisor_shutdown: oneshot::Sender<()>,
    supervisor_wait_limit: Duration,
}

pub(crate) struct ActivatedServerRuntime {
    pub(crate) runtime: ServerRuntime,
    pub(crate) control: RuntimeControlHandle,
}

#[derive(Clone)]
pub(crate) struct RuntimeControlHandle {
    context: ServerContext,
    lifecycle: watch::Receiver<LifecycleState>,
    mutations: mpsc::Sender<MutationEnvelope>,
}

impl RuntimeControlHandle {
    pub(crate) fn server_id(&self) -> &str {
        self.context.id()
    }

    pub(crate) fn snapshot(&self) -> Option<LifecycleStatusSnapshot> {
        self.lifecycle.has_changed().ok()?;
        Some(self.lifecycle.borrow().status_snapshot())
    }

    pub(crate) fn mutation_sender(&self) -> mpsc::Sender<MutationEnvelope> {
        self.mutations.clone()
    }

    #[cfg(test)]
    pub(crate) fn test(
        server_id: &str,
        lifecycle: LifecycleState,
    ) -> (Self, watch::Sender<LifecycleState>) {
        let (sender, receiver) = watch::channel(lifecycle);
        let (mutations, _receiver) = mpsc::channel(MUTATION_QUEUE_CAPACITY);
        (
            Self {
                context: ServerContext::new(server_id.to_owned()),
                lifecycle: receiver,
                mutations,
            },
            sender,
        )
    }

    #[cfg(test)]
    pub(crate) fn test_with_mutations(
        server_id: &str,
        lifecycle: LifecycleState,
    ) -> (
        Self,
        watch::Sender<LifecycleState>,
        mpsc::Receiver<MutationEnvelope>,
    ) {
        let (sender, receiver) = watch::channel(lifecycle);
        let (mutations, mutation_receiver) = mpsc::channel(MUTATION_QUEUE_CAPACITY);
        (
            Self {
                context: ServerContext::new(server_id.to_owned()),
                lifecycle: receiver,
                mutations,
            },
            sender,
            mutation_receiver,
        )
    }
}

#[derive(Clone)]
struct ClientContext {
    context: ServerContext,
    lifecycle: watch::Receiver<LifecycleState>,
    wake_requests: WakeLatchSender,
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
    Fatal,
    QuiescenceFailure(anyhow::Error),
}

impl ServerRuntime {
    pub(crate) fn fatal_observer(&self) -> watch::Receiver<bool> {
        self.fatal.clone()
    }

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

    #[cfg(test)]
    pub(crate) fn spawn_client_task<F>(&mut self, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.connections.spawn(task);
    }

    pub(crate) fn prepare(
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

    // Keeping the prioritized runtime select in one place makes listener and drain ownership clear.
    #[allow(clippy::too_many_lines)]
    pub(crate) async fn run_until<F>(mut self, shutdown: F) -> Result<()>
    where
        F: Future<Output = Result<()>>,
    {
        tokio::pin!(shutdown);

        let exit = 'runtime: loop {
            tokio::select! {
                biased;

                result = &mut self.supervisor => break RunExit::Supervisor(result),
                result = &mut shutdown => break RunExit::Shutdown(result),
                changed = self.fatal.changed() => {
                    if changed.is_ok() && *self.fatal.borrow() {
                        break RunExit::Fatal;
                    }
                }
                request = self.quiescence.recv(), if !self.quiescence_closed => {
                    let Some(mut request) = request else {
                        self.quiescence_closed = true;
                        continue;
                    };
                    self.connections.abort_all();
                    let mut interruption = None;
                    let mut shutdown_result = None;
                    let mut drain_panic = None;
                    while !self.connections.is_empty() {
                        if interruption.is_some() {
                            if record_client_panic(
                                self.connections.join_next().await,
                                &mut drain_panic,
                                "client task panicked during runtime quiescence",
                            ) {
                                self.fatal_signal.raise();
                            }
                            continue;
                        }
                        tokio::select! {
                            biased;

                            result = &mut self.supervisor => {
                                interruption = Some(RunExit::Supervisor(result));
                            }
                            result = &mut shutdown, if shutdown_result.is_none() => {
                                shutdown_result = Some(result);
                            }
                            changed = self.fatal.changed() => {
                                match changed {
                                    Ok(()) if *self.fatal.borrow_and_update() => {
                                        interruption = Some(RunExit::Fatal);
                                    }
                                    Ok(()) => {}
                                    Err(_) => {
                                        self.fatal_signal.raise();
                                        interruption = Some(RunExit::QuiescenceFailure(anyhow::anyhow!(
                                            "fatal runtime signal closed during quiescence"
                                        )));
                                    }
                                }
                            }
                            () = request.acknowledged.closed() => {
                                self.fatal_signal.raise();
                                interruption = Some(RunExit::QuiescenceFailure(anyhow::anyhow!(
                                    "runtime quiescence acknowledgement receiver disappeared"
                                )));
                            }
                            completed = self.connections.join_next() => {
                                if record_client_panic(
                                    completed,
                                    &mut drain_panic,
                                    "client task panicked during runtime quiescence",
                                ) {
                                    self.fatal_signal.raise();
                                }
                            }
                        }
                    }
                    if interruption.is_none() && self.supervisor.is_finished() {
                        interruption = Some(RunExit::Supervisor((&mut self.supervisor).await));
                    }
                    if interruption.is_none() && *self.fatal.borrow() {
                        interruption = Some(RunExit::Fatal);
                    }
                    if interruption.is_none() && request.acknowledged.is_closed() {
                        self.fatal_signal.raise();
                        interruption = Some(RunExit::QuiescenceFailure(anyhow::anyhow!(
                            "runtime quiescence acknowledgement receiver disappeared"
                        )));
                    }
                    if interruption.is_none() && shutdown_result.is_none() {
                        let shutdown_ready = std::future::poll_fn(|task| {
                            Poll::Ready(match shutdown.as_mut().poll(task) {
                                Poll::Ready(result) => Some(result),
                                Poll::Pending => None,
                            })
                        })
                        .await;
                        if let Some(result) = shutdown_ready {
                            shutdown_result = Some(result);
                        }
                    }
                    if let Some(error) = drain_panic {
                        self.fatal_signal.raise();
                        let _ = request.acknowledged.send(Err(anyhow::anyhow!(error.to_string())));
                        break 'runtime RunExit::QuiescenceFailure(error);
                    }
                    if let Some(exit) = interruption {
                        break 'runtime exit;
                    }
                    if request.acknowledged.send(Ok(())).is_err()
                        && !*self.fatal.borrow()
                    {
                        self.fatal_signal.raise();
                        break 'runtime RunExit::QuiescenceFailure(anyhow::anyhow!(
                            "runtime quiescence acknowledgement receiver disappeared"
                        ));
                    }
                    if let Some(result) = shutdown_result {
                        break 'runtime RunExit::Shutdown(result);
                    }
                }
                completed = self.connections.join_next(), if !self.connections.is_empty() => {
                    if let Some(Err(error)) = completed {
                        self.fatal_signal.raise();
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

        self.wake_latch.close();
        self.connections.abort_all();
        let mut cleanup_client_panic = None;
        while let Some(completed) = self.connections.join_next().await {
            if record_client_panic(
                Some(completed),
                &mut cleanup_client_panic,
                "client task panicked",
            ) {
                self.fatal_signal.raise();
            }
        }
        self.runtime_drained.send_replace(true);

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
            RunExit::QuiescenceFailure(error) => self.finish_shutdown(Err(error)).await,
            RunExit::Fatal => {
                let cleanup =
                    await_supervisor_shutdown(&mut self.supervisor, self.supervisor_wait_limit)
                        .await;
                match cleanup {
                    Ok(()) => Err(anyhow::anyhow!("server supervisor entered fatal shutdown")),
                    Err(error) => {
                        Err(error).context("server supervisor failed after fatal escalation")
                    }
                }
            }
        }
    }

    async fn finish_shutdown(mut self, shutdown_result: Result<()>) -> Result<()> {
        let _ = self.supervisor_shutdown.send(());
        let cleanup_result =
            await_supervisor_shutdown(&mut self.supervisor, self.supervisor_wait_limit).await;
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

fn record_client_panic(
    completed: Option<std::result::Result<(), tokio::task::JoinError>>,
    panic: &mut Option<anyhow::Error>,
    context: &'static str,
) -> bool {
    if let Some(Err(error)) = completed
        && error.is_panic()
        && panic.is_none()
    {
        *panic = Some(anyhow::anyhow!("{context}: {error}"));
        return true;
    }
    false
}

impl PreparedServerRuntime {
    pub(crate) async fn bind(self) -> Result<BoundServerRuntime> {
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
    pub(crate) fn activate(self) -> ActivatedServerRuntime {
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

        let (wake_latch, wake_requests, wake_receiver) = supervisor::wake_latch();
        let (mutations, mutation_receiver) = mpsc::channel(MUTATION_QUEUE_CAPACITY);
        let (quiescence_requests, quiescence) = mpsc::channel(1);
        #[cfg(test)]
        let test_quiescence_requests = quiescence_requests.clone();
        let (fatal_signal, fatal) = FatalSignal::new();
        let (runtime_drained, runtime_drained_receiver) = watch::channel(false);
        let (supervisor_shutdown, shutdown_receiver) = oneshot::channel();
        let (lifecycle_sender, lifecycle) = watch::channel(LifecycleState::reconciling());
        let backend_use = Arc::new(BackendUseCoordinator::new());
        let supervisor = tokio::spawn(supervisor::run(
            wake_receiver,
            mutation_receiver,
            quiescence_requests,
            fatal_signal.clone(),
            runtime_drained_receiver,
            shutdown_receiver,
            lifecycle_sender,
            supervisor_config,
            BackendEndpoint::network(backend_endpoint.clone(), proxy_connect_timeout),
            Arc::clone(&backend_use),
        ));
        let control = RuntimeControlHandle {
            context: context.clone(),
            lifecycle: lifecycle.clone(),
            mutations,
        };
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

        ActivatedServerRuntime {
            control,
            runtime: ServerRuntime {
                context,
                listener,
                client_context,
                connection_limit,
                connections: JoinSet::new(),
                wake_latch,
                quiescence,
                #[cfg(test)]
                quiescence_requests: test_quiescence_requests,
                quiescence_closed: false,
                fatal,
                fatal_signal: fatal_signal.clone(),
                runtime_drained,
                supervisor,
                supervisor_shutdown,
                supervisor_wait_limit,
            },
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
                    .enqueue(WakeRequest::observed(observed_lifecycle))
                {
                    Ok(()) => log::info!(
                        "[server={}] Queued server wake-up request from {peer}",
                        context.context
                    ),
                    Err(supervisor::WakeLatchError::Closed) => {
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
    let resolution = async {
        lookup_host((endpoint.host(), endpoint.port()))
            .await
            .map(Iterator::collect)
    };
    connect_backend_with(endpoint, deadline, resolution, TcpStream::connect).await
}

async fn connect_backend_with<R, C, F>(
    endpoint: &Endpoint,
    deadline: Instant,
    resolution: R,
    connect: C,
) -> Result<TcpStream>
where
    R: Future<Output = io::Result<Vec<SocketAddr>>>,
    C: FnMut(SocketAddr) -> F,
    F: Future<Output = io::Result<TcpStream>>,
{
    let addresses = match timeout_at(deadline, resolution).await {
        Ok(Ok(addresses)) => addresses,
        Ok(Err(error)) => {
            bail!("could not resolve Minecraft backend {endpoint}: {error}")
        }
        Err(_) => bail!("could not resolve Minecraft backend {endpoint}: deadline expired"),
    };
    let mut unique_addresses = Vec::with_capacity(addresses.len());
    for address in addresses {
        if !unique_addresses.contains(&address) {
            unique_addresses.push(address);
        }
    }
    ensure!(
        !unique_addresses.is_empty(),
        "could not resolve Minecraft backend {endpoint}: no addresses returned"
    );
    connect_resolved_backend(endpoint, &unique_addresses, deadline, connect).await
}

enum BackendConnectFailure {
    Transport(io::Error),
    TimedOut,
}

async fn connect_resolved_backend<C, F>(
    endpoint: &Endpoint,
    addresses: &[SocketAddr],
    deadline: Instant,
    mut connect: C,
) -> Result<TcpStream>
where
    C: FnMut(SocketAddr) -> F,
    F: Future<Output = io::Result<TcpStream>>,
{
    let mut first_address = 0;
    let mut failed_rounds = 0usize;
    let mut last_failure = None;

    'rounds: loop {
        let round_started_at = Instant::now();
        if round_started_at >= deadline {
            break;
        }

        for offset in 0..addresses.len() {
            let attempt_started_at = Instant::now();
            if attempt_started_at >= deadline {
                break 'rounds;
            }
            let address = addresses[(first_address + offset) % addresses.len()];
            let attempt_deadline =
                (attempt_started_at + BACKEND_CONNECT_ATTEMPT_LIMIT).min(deadline);
            match timeout_at(attempt_deadline, connect(address)).await {
                Ok(Ok(stream)) => return Ok(stream),
                Ok(Err(error)) => {
                    last_failure = Some(BackendConnectFailure::Transport(error));
                }
                Err(_) => last_failure = Some(BackendConnectFailure::TimedOut),
            }
        }

        first_address = (first_address + 1) % addresses.len();
        let spacing = BACKEND_CONNECT_ROUND_SPACING
            [failed_rounds.min(BACKEND_CONNECT_ROUND_SPACING.len() - 1)];
        failed_rounds = failed_rounds.saturating_add(1);
        let next_round_at = (round_started_at + spacing).min(deadline);
        if Instant::now() < next_round_at {
            sleep_until(next_round_at).await;
        }
    }

    match last_failure {
        Some(BackendConnectFailure::Transport(error)) => {
            bail!("could not connect to Minecraft backend {endpoint}: {error}")
        }
        Some(BackendConnectFailure::TimedOut) => {
            bail!("could not connect to Minecraft backend {endpoint}: connection attempt timed out")
        }
        None => bail!("could not connect to Minecraft backend {endpoint}: deadline expired"),
    }
}

#[cfg(test)]
mod tests;
