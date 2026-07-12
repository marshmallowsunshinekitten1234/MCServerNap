use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use mcservernap::config::{self, Config};
use mcservernap::minecraft::MinecraftResponder;
use mcservernap::rcon::RconClient;
use mcservernap::runtime::{ServerRuntime, ServerRuntimeConfig};
use mcservernap::supervisor::SupervisorConfig;
use tokio::time::timeout;

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
    let responder = MinecraftResponder::new(&settings, loaded.server_icon.as_deref())?;
    let runtime = ServerRuntime::bind(runtime_config(options, &settings), responder).await?;
    let shutdown = async {
        tokio::signal::ctrl_c()
            .await
            .context("failed to listen for Ctrl+C")?;
        log::info!("Shutdown signal received");
        Ok(())
    };
    runtime.run_until(shutdown).await
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

fn runtime_config(options: ListenOptions, settings: &Config) -> ServerRuntimeConfig {
    ServerRuntimeConfig {
        bind_host: options.bind_host,
        bind_port: options.bind_port,
        server_host: options.server_host,
        server_port: options.server_port,
        handshake_timeout: Duration::from_secs(settings.handshake_timeout_seconds),
        proxy_connect_timeout: Duration::from_secs(settings.proxy_connect_timeout_seconds),
        max_connections: settings.max_connections,
        supervisor: SupervisorConfig {
            command: options.command,
            arguments: options.arguments,
            rcon_address: options.rcon_address,
            rcon_password: options.rcon_password,
            poll_interval: Duration::from_secs(settings.rcon_poll_interval_seconds),
            idle_timeout: Duration::from_secs(settings.rcon_idle_timeout_seconds),
            startup_timeout: Duration::from_secs(settings.rcon_startup_timeout_seconds),
            retry_interval: Duration::from_secs(settings.rcon_retry_interval_seconds),
            command_timeout: Duration::from_secs(settings.rcon_command_timeout_seconds),
            shutdown_timeout: Duration::from_secs(settings.shutdown_timeout_seconds),
        },
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
