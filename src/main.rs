use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use mcservernap::config::{self, Config};
use mcservernap::endpoint::Endpoint;
use mcservernap::minecraft::{MinecraftResponder, MinecraftResponderSettings};
use mcservernap::process::LaunchCommand;
use mcservernap::rcon::RconClient;
use mcservernap::runtime::{ServerRuntime, ServerRuntimeConfig};
use mcservernap::supervisor::{RconConfig, SupervisorConfig};
use tokio::sync::Semaphore;
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
    bind_endpoint: Endpoint,
    backend_endpoint: Endpoint,
    rcon_endpoint: Endpoint,
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
                bind_endpoint: Endpoint::new(host, port),
                backend_endpoint: Endpoint::new(server_host, server_port),
                rcon_endpoint: Endpoint::new(rcon_host, rcon_port),
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
        } => stop_via_rcon(&Endpoint::new(rcon_host, rcon_port), &rcon_pass).await,
    }
}

async fn run_listener(options: ListenOptions) -> Result<()> {
    let loaded = config::load_or_create(&options.config_path)?;
    let settings = loaded.settings;
    let responder = MinecraftResponder::new(responder_settings(&settings, loaded.server_icon))?;
    let connection_limit = Arc::new(Semaphore::new(settings.max_connections));
    let working_directory =
        std::env::current_dir().context("failed to determine MCServerNap's working directory")?;
    let runtime = ServerRuntime::prepare(
        runtime_config(options, &settings, working_directory),
        responder,
        connection_limit,
    )?
    .bind()
    .await?
    .activate();
    let shutdown = async {
        tokio::signal::ctrl_c()
            .await
            .context("failed to listen for Ctrl+C")?;
        log::info!("Shutdown signal received");
        Ok(())
    };
    runtime.run_until(shutdown).await
}

async fn stop_via_rcon(endpoint: &Endpoint, password: &str) -> Result<()> {
    let mut client = timeout(
        Duration::from_secs(10),
        RconClient::connect(endpoint, password),
    )
    .await
    .context("RCON connection timed out")??;
    timeout(Duration::from_secs(5), client.stop())
        .await
        .context("RCON stop command timed out")??;
    log::info!("Sent stop command to RCON at {endpoint}");
    Ok(())
}

fn runtime_config(
    options: ListenOptions,
    settings: &Config,
    working_directory: PathBuf,
) -> ServerRuntimeConfig {
    ServerRuntimeConfig {
        bind_endpoint: options.bind_endpoint,
        backend_endpoint: options.backend_endpoint,
        handshake_timeout: Duration::from_secs(settings.handshake_timeout_seconds),
        proxy_connect_timeout: Duration::from_secs(settings.proxy_connect_timeout_seconds),
        supervisor: SupervisorConfig {
            launch: LaunchCommand::new(
                OsString::from(options.command),
                options.arguments.into_iter().map(OsString::from).collect(),
                working_directory,
            ),
            rcon: Some(RconConfig {
                endpoint: options.rcon_endpoint,
                password: options.rcon_password,
            }),
            poll_interval: Duration::from_secs(settings.rcon_poll_interval_seconds),
            idle_timeout: Duration::from_secs(settings.rcon_idle_timeout_seconds),
            startup_timeout: Duration::from_secs(settings.rcon_startup_timeout_seconds),
            retry_interval: Duration::from_secs(settings.rcon_retry_interval_seconds),
            command_timeout: Duration::from_secs(settings.rcon_command_timeout_seconds),
            shutdown_timeout: Duration::from_secs(settings.shutdown_timeout_seconds),
        },
    }
}

fn responder_settings(
    settings: &Config,
    server_icon: Option<String>,
) -> MinecraftResponderSettings {
    MinecraftResponderSettings {
        minecraft_version: settings.minecraft_version,
        sleeping_motd_text: settings.motd_text.clone(),
        sleeping_motd_color: settings.motd_color.clone(),
        sleeping_motd_bold: settings.motd_bold,
        startup_message_text: settings.connection_msg_text.clone(),
        startup_message_color: settings.connection_msg_color.clone(),
        startup_message_bold: settings.connection_msg_bold,
        server_icon,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn runtime_translation_preserves_endpoints_and_exact_cli_launch_values() {
        let working_directory = PathBuf::from("minecraft server");
        let config = runtime_config(
            ListenOptions {
                bind_endpoint: Endpoint::new("::", 25_565),
                backend_endpoint: Endpoint::new("localhost", 25_566),
                rcon_endpoint: Endpoint::new("::1", 25_575),
                rcon_password: "secret".to_owned(),
                command: "java".to_owned(),
                arguments: vec![
                    "-Xmx5G".to_owned(),
                    "argument with spaces".to_owned(),
                    "$UNCHANGED".to_owned(),
                ],
                config_path: PathBuf::from("config/cfg.toml"),
            },
            &Config::default(),
            working_directory.clone(),
        );

        assert_eq!(config.bind_endpoint, Endpoint::new("::", 25_565));
        assert_eq!(config.backend_endpoint, Endpoint::new("localhost", 25_566));
        assert_eq!(config.supervisor.launch.program, OsString::from("java"));
        assert_eq!(
            config.supervisor.launch.arguments,
            ["-Xmx5G", "argument with spaces", "$UNCHANGED"]
                .map(OsString::from)
                .to_vec()
        );
        assert_eq!(
            config.supervisor.launch.working_directory,
            working_directory
        );
        assert_eq!(
            config.supervisor.rcon.as_ref().unwrap().endpoint,
            Endpoint::new("::1", 25_575)
        );
    }

    #[test]
    fn schema_v2_translation_owns_only_responder_inputs() {
        let settings = Config {
            motd_text: "Sleeping".to_owned(),
            motd_color: "blue".to_owned(),
            motd_bold: false,
            connection_msg_text: "Starting".to_owned(),
            connection_msg_color: "gold".to_owned(),
            connection_msg_bold: true,
            ..Config::default()
        };
        let responder = responder_settings(&settings, Some("prepared-icon".to_owned()));

        assert_eq!(responder.minecraft_version, settings.minecraft_version);
        assert_eq!(responder.sleeping_motd_text, "Sleeping");
        assert_eq!(responder.sleeping_motd_color, "blue");
        assert!(!responder.sleeping_motd_bold);
        assert_eq!(responder.startup_message_text, "Starting");
        assert_eq!(responder.startup_message_color, "gold");
        assert!(responder.startup_message_bold);
        assert_eq!(responder.server_icon.as_deref(), Some("prepared-icon"));
    }
}
