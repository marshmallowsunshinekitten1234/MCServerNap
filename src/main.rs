use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use mcservernap::coordinator::DaemonCoordinator;
use mcservernap::endpoint::Endpoint;
use mcservernap::rcon::RconClient;
use tokio::time::timeout;

#[derive(Parser)]
#[command(
    name = "mcservernap",
    version,
    about = "Wake Minecraft Java servers on demand and stop them when idle"
)]
struct Cli {
    /// Schema-v3 configuration file used only by the listener daemon.
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
    /// Listen on every configured public endpoint and manage its backend.
    Listen,
    /// Send `stop` directly to an already-running Minecraft server via RCON.
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

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    let cli = Cli::parse();
    match cli.command {
        Commands::Listen => DaemonCoordinator::run(&cli.config).await,
        Commands::Stop {
            rcon_host,
            rcon_port,
            rcon_pass,
        } => stop_via_rcon(&Endpoint::new(rcon_host, rcon_port), &rcon_pass).await,
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_configuration_only_listener_invocation() {
        let cli = Cli::try_parse_from(["mcservernap", "--config", "config/cfg.toml", "listen"])
            .expect("configuration-only listen command should parse");
        assert!(matches!(cli.command, Commands::Listen));
        assert_eq!(cli.config, PathBuf::from("config/cfg.toml"));
    }

    #[test]
    fn listener_rejects_obsolete_positional_arguments() {
        assert!(Cli::try_parse_from(["mcservernap", "listen", "0.0.0.0", "25565"]).is_err());
    }

    #[test]
    fn stop_parsing_does_not_access_the_listener_configuration() {
        let cli = Cli::try_parse_from([
            "mcservernap",
            "--config",
            "definitely-missing.toml",
            "stop",
            "--rcon-port",
            "25575",
            "--rcon-pass",
            "secret",
        ])
        .expect("standalone stop command should not load listener configuration");
        assert!(matches!(cli.command, Commands::Stop { .. }));
    }
}
