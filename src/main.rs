use std::path::PathBuf;
use std::time::Duration;
use std::{fmt::Write as _, io::Write as _};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use mcservernap::control::{self, ClientMutationOperation};
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
    /// Inspect or mutate servers through the local control daemon.
    Server {
        #[command(subcommand)]
        command: ServerCommand,
    },
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

#[derive(Subcommand)]
enum ServerCommand {
    /// List the server IDs exposed by the daemon for this OS principal.
    List,
    /// Show the daemon's current lifecycle status for one server.
    Status { server_id: String },
    /// Admit asynchronous start intent for one server.
    Start { server_id: String },
    /// Explicitly stop one owned server process.
    Stop { server_id: String },
    /// Asynchronously stop, reap, reconcile, and relaunch one owned server process.
    Restart { server_id: String },
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
        Commands::Server {
            command: ServerCommand::List,
        } => {
            print_output(&format_list(&control::list().await?))?;
            Ok(())
        }
        Commands::Server {
            command: ServerCommand::Status { server_id },
        } => {
            let status = control::status(server_id).await?;
            print_output(&format_status(
                status.server_id(),
                status.phase(),
                status.failure_category(),
                status.failure_streak(),
                status.retry_after_ms(),
                status.command_revision(),
            ))?;
            Ok(())
        }
        Commands::Server {
            command: ServerCommand::Start { server_id },
        } => mutate_server(server_id, ClientMutationOperation::Start).await,
        Commands::Server {
            command: ServerCommand::Stop { server_id },
        } => mutate_server(server_id, ClientMutationOperation::Stop).await,
        Commands::Server {
            command: ServerCommand::Restart { server_id },
        } => mutate_server(server_id, ClientMutationOperation::Restart).await,
        Commands::Stop {
            rcon_host,
            rcon_port,
            rcon_pass,
        } => stop_via_rcon(&Endpoint::new(rcon_host, rcon_port), &rcon_pass).await,
    }
}

fn print_output(output: &str) -> Result<()> {
    std::io::stdout()
        .write_all(output.as_bytes())
        .context("failed to write command output")
}

fn format_list(server_ids: &[String]) -> String {
    let mut output = String::new();
    for server_id in server_ids {
        writeln!(output, "{server_id}").expect("writing to a String cannot fail");
    }
    output
}

fn format_status(
    server_id: &str,
    phase: &str,
    failure_category: Option<&str>,
    failure_streak: u32,
    retry_after_ms: Option<u64>,
    command_revision: u64,
) -> String {
    let mut output = format!("server: {server_id}\nphase: {phase}\n");
    if let Some(failure) = failure_category {
        writeln!(output, "failure: {failure}").expect("writing to a String cannot fail");
    }
    writeln!(output, "failure streak: {failure_streak}").expect("writing to a String cannot fail");
    if let Some(retry_after_ms) = retry_after_ms {
        writeln!(
            output,
            "retry after: {:?}",
            Duration::from_millis(retry_after_ms)
        )
        .expect("writing to a String cannot fail");
    }
    writeln!(output, "command revision: {command_revision}")
        .expect("writing to a String cannot fail");
    output
}

async fn mutate_server(server_id: String, operation: ClientMutationOperation) -> Result<()> {
    let status = control::status(server_id.clone()).await?;
    let expected_revision = status.command_revision();
    let request_id =
        generate_request_id().context("failed to obtain OS randomness for request ID")?;
    let result = operation
        .submit(server_id.clone(), request_id.clone(), expected_revision)
        .await;
    match result {
        Ok(result) => {
            print_output(&format_mutation_result(
                operation,
                result.disposition(),
                result.request_id(),
                result.command_revision(),
                result.server_id(),
            ))?;
            Ok(())
        }
        Err(control::ClientError::SubmissionUncertain) => {
            let diagnostic = control::status(server_id).await;
            let diagnostic_text = match diagnostic {
                Ok(status) if status.command_revision() != expected_revision => format!(
                    "The current command revision is {}; that change may belong to this request or another mutation.",
                    status.command_revision()
                ),
                Ok(_) => "The command revision is unchanged, but that still cannot prove absence while server-side cancellation is racing.".to_owned(),
                Err(_) => "A diagnostic status query also failed.".to_owned(),
            };
            Err(anyhow::anyhow!(
                "{operation} submission has an ambiguous outcome (request ID {request_id}, expected command revision {expected_revision}). {diagnostic_text} The request was not resent."
            ))
        }
        Err(error) => Err(error.into()),
    }
}

fn format_mutation_result(
    operation: ClientMutationOperation,
    disposition: &str,
    request_id: &str,
    command_revision: u64,
    server_id: &str,
) -> String {
    format!(
        "{operation} {disposition} (request {request_id}, command revision {command_revision}). Use `mcservernap server status {server_id}` to observe eventual state.\n"
    )
}

fn generate_request_id() -> Result<String, getrandom::Error> {
    generate_request_id_with(|bytes| getrandom::fill(bytes))
}

fn generate_request_id_with<E>(
    fill: impl FnOnce(&mut [u8; 16]) -> std::result::Result<(), E>,
) -> std::result::Result<String, E> {
    let mut bytes = [0_u8; 16];
    fill(&mut bytes)?;
    Ok(request_id_from_bytes(bytes))
}

fn request_id_from_bytes(bytes: [u8; 16]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = [0_u8; 32];
    for (index, byte) in bytes.into_iter().enumerate() {
        encoded[index * 2] = HEX[usize::from(byte >> 4)];
        encoded[index * 2 + 1] = HEX[usize::from(byte & 0x0f)];
    }
    String::from_utf8(encoded.to_vec()).expect("lowercase hexadecimal is valid UTF-8")
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

    #[test]
    fn parses_grouped_control_commands_and_retains_clap_usage_exit_two() {
        let list = Cli::try_parse_from([
            "mcservernap",
            "--config",
            "definitely-invalid.toml",
            "server",
            "list",
        ])
        .unwrap();
        assert!(matches!(
            list.command,
            Commands::Server {
                command: ServerCommand::List
            }
        ));

        let status = Cli::try_parse_from([
            "mcservernap",
            "--config",
            "definitely-invalid.toml",
            "server",
            "status",
            "survival",
        ])
        .unwrap();
        assert!(matches!(
            status.command,
            Commands::Server {
                command: ServerCommand::Status { ref server_id }
            } if server_id == "survival"
        ));
        assert_eq!(
            Cli::try_parse_from(["mcservernap", "server", "status"])
                .err()
                .unwrap()
                .exit_code(),
            2
        );
        assert!(Cli::try_parse_from(["mcservernap", "list"]).is_err());
        assert!(Cli::try_parse_from(["mcservernap", "status", "survival"]).is_err());
        assert!(Cli::try_parse_from(["mcservernap", "restart", "survival"]).is_err());
        for operation in ["start", "stop", "restart"] {
            let cli =
                Cli::try_parse_from(["mcservernap", "server", operation, "survival"]).unwrap();
            assert!(matches!(
                cli.command,
                Commands::Server {
                    command: ServerCommand::Start { ref server_id }
                        | ServerCommand::Stop { ref server_id }
                        | ServerCommand::Restart { ref server_id }
                } if server_id == "survival"
            ));
        }
    }

    #[test]
    fn request_id_encoding_is_exact_lowercase_hexadecimal() {
        assert_eq!(
            request_id_from_bytes([
                0x00, 0x01, 0x0f, 0x10, 0x2a, 0x7f, 0x80, 0xab, 0xcd, 0xef, 0x11, 0x22, 0x33, 0x44,
                0x55, 0xff,
            ]),
            "00010f102a7f80abcdef1122334455ff"
        );
        let generated = generate_request_id().unwrap();
        assert_eq!(generated.len(), 32);
        assert!(
            generated
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        );
        assert_eq!(
            generate_request_id_with(|_| Err("entropy unavailable")),
            Err("entropy unavailable")
        );
    }

    #[test]
    fn list_and_status_human_output_is_deterministic() {
        assert_eq!(
            format_list(&["creative".to_owned(), "survival".to_owned()]),
            "creative\nsurvival\n"
        );
        assert_eq!(
            format_status(
                "survival",
                "cooldown",
                Some("launch_failed"),
                2,
                Some(8_400),
                12,
            ),
            "server: survival\nphase: cooldown\nfailure: launch_failed\nfailure streak: 2\nretry after: 8.4s\ncommand revision: 12\n"
        );
        assert_eq!(
            format_status("creative", "running", None, 0, None, 0),
            "server: creative\nphase: running\nfailure streak: 0\ncommand revision: 0\n"
        );
        assert_eq!(
            format_mutation_result(
                ClientMutationOperation::Restart,
                "accepted",
                "11111111111111111111111111111111",
                13,
                "survival",
            ),
            "restart accepted (request 11111111111111111111111111111111, command revision 13). Use `mcservernap server status survival` to observe eventual state.\n"
        );
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
