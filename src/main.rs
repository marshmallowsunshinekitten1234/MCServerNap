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
    run(cli).await
}

async fn run(cli: Cli) -> Result<()> {
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
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::{TcpListener, TcpStream};

    use super::*;

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn parses_configuration_only_listener_invocation() {
        let cli = Cli::try_parse_from(["mcservernap", "--config", "config/cfg.toml", "listen"])
            .expect("configuration-only listen command should parse");
        assert!(matches!(cli.command, Commands::Listen));
        assert_eq!(cli.config, PathBuf::from("config/cfg.toml"));
    }

    #[tokio::test]
    async fn standalone_stop_executes_without_loading_listener_configuration() {
        let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let directory =
            std::env::temp_dir().join(format!("mcservernap-stop-test-{}-{id}", std::process::id()));
        fs::create_dir_all(&directory).unwrap();
        let missing = directory.join("missing.toml");
        let invalid = directory.join("invalid.toml");
        fs::write(&invalid, "schema_version = [").unwrap();

        execute_stop(&missing).await;
        execute_stop(&invalid).await;

        fs::remove_dir_all(directory).ok();
    }

    async fn execute_stop(config: &std::path::Path) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (auth_id, auth_kind, auth_body) = receive_packet(&mut stream).await;
            assert_eq!(auth_kind, 3);
            assert_eq!(auth_body, b"secret");
            send_packet(&mut stream, auth_id, 2).await;

            let (_, command_kind, command_body) = receive_packet(&mut stream).await;
            assert_eq!(command_kind, 2);
            assert_eq!(command_body, b"stop");
        });
        let cli = Cli::try_parse_from(vec![
            "mcservernap".to_owned(),
            "--config".to_owned(),
            config.to_string_lossy().into_owned(),
            "stop".to_owned(),
            "--rcon-host".to_owned(),
            "127.0.0.1".to_owned(),
            "--rcon-port".to_owned(),
            port.to_string(),
            "--rcon-pass".to_owned(),
            "secret".to_owned(),
        ])
        .unwrap();

        run(cli).await.unwrap();
        server.await.unwrap();
    }

    async fn receive_packet(stream: &mut TcpStream) -> (i32, i32, Vec<u8>) {
        let length = stream.read_i32_le().await.unwrap();
        let mut payload = vec![0; usize::try_from(length).unwrap()];
        stream.read_exact(&mut payload).await.unwrap();
        assert!(payload.len() >= 10);
        assert_eq!(&payload[payload.len() - 2..], &[0, 0]);
        let id = i32::from_le_bytes(payload[0..4].try_into().unwrap());
        let kind = i32::from_le_bytes(payload[4..8].try_into().unwrap());
        let body = payload[8..payload.len() - 2].to_vec();
        (id, kind, body)
    }

    async fn send_packet(stream: &mut TcpStream, id: i32, kind: i32) {
        stream.write_i32_le(10).await.unwrap();
        stream.write_i32_le(id).await.unwrap();
        stream.write_i32_le(kind).await.unwrap();
        stream.write_all(&[0, 0]).await.unwrap();
    }
}
