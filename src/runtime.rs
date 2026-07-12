use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, TryAcquireError, mpsc, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{Instant, sleep, sleep_until, timeout};

use crate::minecraft::{ClientRequest, MinecraftResponder};
use crate::supervisor::{self, ServerPhase, SupervisorCommand, SupervisorConfig};

const SUPERVISOR_COMMAND_CAPACITY: usize = 16;
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
    supervisor_wait_limit: Duration,
}

#[derive(Clone)]
struct ClientContext {
    phase: watch::Receiver<ServerPhase>,
    commands: mpsc::Sender<SupervisorCommand>,
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

        let (commands, command_receiver) = mpsc::channel(SUPERVISOR_COMMAND_CAPACITY);
        let (phase_sender, phase) = watch::channel(ServerPhase::Stopped);
        let supervisor_wait_limit = supervisor_config.shutdown_timeout + SUPERVISOR_SHUTDOWN_MARGIN;
        let supervisor = tokio::spawn(supervisor::run(
            command_receiver,
            phase_sender,
            supervisor_config,
        ));
        let client_context = ClientContext {
            phase,
            commands,
            responder: Arc::new(responder),
            server_host: Arc::from(server_host),
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
                let shutdown_requested = self
                    .client_context
                    .commands
                    .send(SupervisorCommand::Shutdown)
                    .await
                    .is_ok();
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
    let current_phase = *context.phase.borrow();

    if current_phase == ServerPhase::Running {
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
            if matches!(current_phase, ServerPhase::Stopped | ServerPhase::Stopping) {
                match context.commands.try_send(SupervisorCommand::Start) {
                    Ok(()) => log::info!("Queued server wake-up request from {peer}"),
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        log::debug!("Wake-up request already queued");
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        bail!("server supervisor command channel is closed");
                    }
                }
            }
            context
                .responder
                .send_login_disconnect(&mut socket, operation_timeout)
                .await?;
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
    use std::path::PathBuf;

    use anyhow::anyhow;
    use tokio::io::AsyncReadExt as _;
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
        TcpListener::bind(address)
            .await
            .expect("runtime should release its listener");
    }

    #[test]
    #[ignore = "launched as the runtime's test server process"]
    fn server_process_fixture() {
        std::thread::sleep(Duration::from_secs(30));
    }

    #[tokio::test]
    async fn shutdown_failure_waits_for_active_supervisor_cleanup() {
        let rcon_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test RCON listener should bind");
        let rcon_address = rcon_listener
            .local_addr()
            .expect("test RCON listener should have an address")
            .to_string();
        let executable: PathBuf =
            std::env::current_exe().expect("test executable path should be known");
        let runtime = bind_test_runtime(runtime_config(
            executable.to_string_lossy().into_owned(),
            vec![
                "--ignored".to_owned(),
                "--exact".to_owned(),
                "runtime::tests::server_process_fixture".to_owned(),
                "--quiet".to_owned(),
            ],
            rcon_address,
        ))
        .await;
        let mut phase = runtime.client_context.phase.clone();
        let commands = runtime.client_context.commands.clone();
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let mut runtime_task = tokio::spawn(runtime.run_until(async move {
            shutdown_receiver
                .await
                .context("test shutdown sender was dropped")?;
            Err(anyhow!("test shutdown source failed"))
        }));

        commands
            .send(SupervisorCommand::Start)
            .await
            .expect("runtime supervisor should accept a start command");
        drop(commands);
        timeout(
            Duration::from_secs(5),
            phase.wait_for(|phase| *phase == ServerPhase::Starting),
        )
        .await
        .expect("server process should start")
        .expect("supervisor should remain available during startup");

        shutdown_sender
            .send(())
            .expect("runtime should still await shutdown");
        let error = timeout(Duration::from_secs(12), &mut runtime_task)
            .await
            .expect("runtime should finish after process cleanup")
            .expect("runtime task should not panic")
            .expect_err("caller shutdown failure should be returned");
        assert_eq!(error.to_string(), "test shutdown source failed");
        assert_eq!(*phase.borrow(), ServerPhase::Stopped);
    }
}
