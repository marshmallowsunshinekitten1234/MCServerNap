use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use mcservernap::config::{self, Config};
use mcservernap::minecraft::{ClientRequest, MinecraftResponder};
use mcservernap::rcon::RconClient;
use mcservernap::supervisor::{self, ServerPhase, SupervisorCommand, SupervisorConfig};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, TryAcquireError, mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep, sleep_until, timeout};

#[derive(Parser)]
#[command(
    name = "mcservernap",
    version,
    about = "Wake a Minecraft Java server on demand and stop it when idle"
)]
struct Cli {
    /// Configuration file used by the listener.
    #[arg(
        long,
        global = true,
        default_value = "config/cfg.toml",
        env = "MCSERVERNAP_CONFIG"
    )]
    config: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Listen for Minecraft clients and manage the backend server.
    Listen {
        /// Host or IP on which the public Minecraft proxy listens.
        host: String,
        /// Public Minecraft proxy port.
        port: u16,
        /// Command or script used to launch the Minecraft server.
        command: String,
        /// Host or IP of the actual Minecraft server.
        #[arg(long, default_value = "127.0.0.1")]
        server_host: String,
        /// Port of the actual Minecraft server.
        #[arg(long)]
        server_port: u16,
        /// Host or IP of the Minecraft RCON endpoint.
        #[arg(long, default_value = "127.0.0.1")]
        rcon_host: String,
        /// Minecraft RCON port.
        #[arg(long)]
        rcon_port: u16,
        /// Minecraft RCON password. Prefer the `MCSERVERNAP_RCON_PASSWORD` environment variable.
        #[arg(long, env = "MCSERVERNAP_RCON_PASSWORD", hide_env_values = true)]
        rcon_pass: String,
        /// Arguments passed verbatim to the server command, after `--`.
        #[arg(last = true, allow_hyphen_values = true)]
        arguments: Vec<String>,
    },
    /// Send `stop` to an already-running Minecraft server via RCON.
    Stop {
        /// Host or IP of the Minecraft RCON endpoint.
        #[arg(long, default_value = "127.0.0.1")]
        rcon_host: String,
        /// Minecraft RCON port.
        #[arg(long)]
        rcon_port: u16,
        /// Minecraft RCON password. Prefer the `MCSERVERNAP_RCON_PASSWORD` environment variable.
        #[arg(long, env = "MCSERVERNAP_RCON_PASSWORD", hide_env_values = true)]
        rcon_pass: String,
    },
}

struct ListenOptions {
    bind_host: String,
    bind_port: u16,
    server_host: String,
    server_port: u16,
    rcon_address: String,
    rcon_password: String,
    command: String,
    arguments: Vec<String>,
    config_path: PathBuf,
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

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    let cli = Cli::parse();
    match cli.command {
        Commands::Listen {
            host,
            port,
            command,
            server_host,
            server_port,
            rcon_host,
            rcon_port,
            rcon_pass,
            arguments,
        } => {
            run_listener(ListenOptions {
                bind_host: host,
                bind_port: port,
                server_host,
                server_port,
                rcon_address: socket_address(&rcon_host, rcon_port),
                rcon_password: rcon_pass,
                command,
                arguments,
                config_path: cli.config,
            })
            .await
        }
        Commands::Stop {
            rcon_host,
            rcon_port,
            rcon_pass,
        } => stop_via_rcon(&socket_address(&rcon_host, rcon_port), &rcon_pass).await,
    }
}

async fn run_listener(options: ListenOptions) -> Result<()> {
    let loaded = config::load_or_create(&options.config_path)?;
    let settings = loaded.settings;
    let responder = Arc::new(MinecraftResponder::new(
        &settings,
        loaded.server_icon.as_deref(),
    )?);
    let listener = TcpListener::bind((options.bind_host.as_str(), options.bind_port))
        .await
        .with_context(|| {
            format!(
                "failed to listen on {}:{}",
                options.bind_host, options.bind_port
            )
        })?;
    log::info!(
        "Listening for Minecraft clients on {}:{}",
        options.bind_host,
        options.bind_port
    );

    let (command_sender, command_receiver) = mpsc::channel(16);
    let (phase_sender, phase_receiver) = watch::channel(ServerPhase::Stopped);
    let supervisor_config = supervisor_config(&options, &settings);
    let mut supervisor = tokio::spawn(supervisor::run(
        command_receiver,
        phase_sender,
        supervisor_config,
    ));

    let connection_limit = Arc::new(Semaphore::new(settings.max_connections));
    let client_context = ClientContext {
        phase: phase_receiver,
        commands: command_sender.clone(),
        responder,
        server_host: Arc::from(options.server_host.as_str()),
        server_port: options.server_port,
        handshake_timeout: Duration::from_secs(settings.handshake_timeout_seconds),
        proxy_connect_timeout: Duration::from_secs(settings.proxy_connect_timeout_seconds),
    };
    let mut connections = JoinSet::new();
    let shutdown_signal = tokio::signal::ctrl_c();
    tokio::pin!(shutdown_signal);

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (socket, peer) = match accepted {
                    Ok(connection) => connection,
                    Err(error) => {
                        log::warn!("Failed to accept a client connection: {error}");
                        sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                };

                let permit = match Arc::clone(&connection_limit).try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(TryAcquireError::NoPermits) => {
                        log::warn!("Dropping connection from {peer}: connection limit reached");
                        continue;
                    }
                    Err(TryAcquireError::Closed) => bail!("connection limiter unexpectedly closed"),
                };

                let context = client_context.clone();
                connections.spawn(async move {
                    let _permit = permit;
                    if let Err(error) = handle_client(socket, peer, context).await {
                        log::debug!("Connection from {peer} ended: {error:#}");
                    }
                });
            }
            signal = &mut shutdown_signal => {
                signal.context("failed to listen for Ctrl+C")?;
                log::info!("Shutdown signal received");
                let _ = command_sender.send(SupervisorCommand::Shutdown).await;
                break;
            }
            result = &mut supervisor => {
                result.context("server supervisor task failed")??;
                bail!("server supervisor stopped unexpectedly");
            }
            completed = connections.join_next(), if !connections.is_empty() => {
                if let Some(Err(error)) = completed {
                    log::warn!("Client task failed: {error}");
                }
            }
        }
    }

    connections.abort_all();
    while connections.join_next().await.is_some() {}

    await_supervisor_shutdown(&mut supervisor, settings.shutdown_timeout_seconds).await
}

async fn await_supervisor_shutdown(
    supervisor: &mut tokio::task::JoinHandle<Result<()>>,
    shutdown_timeout_seconds: u64,
) -> Result<()> {
    let wait_limit = Duration::from_secs(shutdown_timeout_seconds) + Duration::from_secs(10);
    if let Ok(result) = timeout(wait_limit, &mut *supervisor).await {
        result.context("server supervisor task failed")??;
        return Ok(());
    }

    supervisor.abort();
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

async fn stop_via_rcon(address: &str, password: &str) -> Result<()> {
    let mut client = timeout(
        Duration::from_secs(10),
        RconClient::connect(address, password),
    )
    .await
    .context("RCON connection timed out")??;
    timeout(Duration::from_secs(5), client.stop())
        .await
        .context("RCON stop command timed out")??;
    log::info!("Sent stop command to RCON at {address}");
    Ok(())
}

fn supervisor_config(options: &ListenOptions, settings: &Config) -> SupervisorConfig {
    SupervisorConfig {
        command: options.command.clone(),
        arguments: options.arguments.clone(),
        rcon_address: options.rcon_address.clone(),
        rcon_password: options.rcon_password.clone(),
        poll_interval: Duration::from_secs(settings.rcon_poll_interval_seconds),
        idle_timeout: Duration::from_secs(settings.rcon_idle_timeout_seconds),
        startup_timeout: Duration::from_secs(settings.rcon_startup_timeout_seconds),
        retry_interval: Duration::from_secs(settings.rcon_retry_interval_seconds),
        command_timeout: Duration::from_secs(settings.rcon_command_timeout_seconds),
        shutdown_timeout: Duration::from_secs(settings.shutdown_timeout_seconds),
    }
}

fn socket_address(host: &str, port: u16) -> String {
    if host.contains(':') && !(host.starts_with('[') && host.ends_with(']')) {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_ipv4_hostnames_and_ipv6_addresses() {
        assert_eq!(socket_address("127.0.0.1", 25575), "127.0.0.1:25575");
        assert_eq!(socket_address("localhost", 25575), "localhost:25575");
        assert_eq!(socket_address("::1", 25575), "[::1]:25575");
        assert_eq!(socket_address("[::1]", 25575), "[::1]:25575");
    }

    #[test]
    fn parses_documented_listener_invocation() {
        let cli = Cli::try_parse_from([
            "mcservernap",
            "listen",
            "0.0.0.0",
            "25565",
            "--server-port",
            "25566",
            "--rcon-port",
            "25575",
            "--rcon-pass",
            "secret",
            "java",
            "--",
            "-Xmx5G",
            "-jar",
            "server.jar",
            "nogui",
        ])
        .expect("documented command should parse");

        let Commands::Listen {
            host,
            port,
            command,
            arguments,
            ..
        } = cli.command
        else {
            panic!("expected listen command");
        };
        assert_eq!(host, "0.0.0.0");
        assert_eq!(port, 25565);
        assert_eq!(command, "java");
        assert_eq!(arguments, ["-Xmx5G", "-jar", "server.jar", "nogui"]);
    }
}
