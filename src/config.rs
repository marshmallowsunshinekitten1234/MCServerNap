use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{OsStr, OsString};
use std::fs;
use std::io::Cursor;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use image::imageops::FilterType;
use image::{GenericImageView as _, ImageFormat, ImageReader};
use serde::Deserialize;
use tokio::sync::Semaphore;

use crate::context::ServerContext;
use crate::endpoint::Endpoint;
use crate::minecraft::{MinecraftResponder, MinecraftResponderSettings, MinecraftVersion};
use crate::process::LaunchCommand;
use crate::rcon::RconSecret;
use crate::runtime::ServerRuntimeConfig;
use crate::supervisor::{RconConfig, SupervisorConfig};

const MAX_ICON_FILE_SIZE: usize = 8 * 1024 * 1024;
const MAX_ICON_DIMENSION: u32 = 4_096;
const ICON_SIZE: u32 = 64;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigDocument {
    schema_version: u32,
    max_connections: usize,
    servers: BTreeMap<String, ServerSchema>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ServerSchema {
    minecraft_version: MinecraftVersion,
    icon_path: Option<String>,
    listener: ListenerSchema,
    backend: TargetSchema,
    command: CommandSchema,
    timeouts: TimeoutsSchema,
    motd: StyledTextSchema,
    startup_message: StyledTextSchema,
    rcon: Option<RconSchema>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ListenerSchema {
    address: String,
    port: u16,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TargetSchema {
    host: String,
    port: u16,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CommandSchema {
    program: String,
    arguments: Vec<String>,
    working_directory: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(
    clippy::struct_field_names,
    reason = "the schema's required unit suffixes keep operator-facing duration units explicit"
)]
struct TimeoutsSchema {
    poll_interval_seconds: u64,
    idle_timeout_seconds: u64,
    startup_timeout_seconds: u64,
    retry_interval_seconds: u64,
    command_timeout_seconds: u64,
    shutdown_timeout_seconds: u64,
    handshake_timeout_seconds: u64,
    proxy_connect_timeout_seconds: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StyledTextSchema {
    text: String,
    color: String,
    bold: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RconSchema {
    host: String,
    port: u16,
    password_env: String,
}

pub(crate) struct PreparedConfig {
    pub(crate) max_connections: usize,
    pub(crate) servers: BTreeMap<String, PreparedServerConfig>,
}

pub(crate) struct PreparedServerConfig {
    pub(crate) context: ServerContext,
    pub(crate) runtime: ServerRuntimeConfig,
    pub(crate) responder: MinecraftResponder,
}

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
enum CanonicalHost {
    Ip(IpAddr),
    Dns(String),
}

struct ListenerRecord<'a> {
    server_id: &'a str,
    address: IpAddr,
    port: u16,
}

struct TargetRecord<'a> {
    server_id: &'a str,
    role: &'static str,
    host: CanonicalHost,
    port: u16,
}

pub(crate) fn load(path: &Path) -> Result<PreparedConfig> {
    let base = std::env::current_dir().context(
        "failed to determine MCServerNap's current directory for the configuration path",
    )?;
    load_with(path, &base, |name| std::env::var_os(name))
}

fn load_with<F>(path: &Path, base: &Path, environment: F) -> Result<PreparedConfig>
where
    F: Fn(&str) -> Option<OsString>,
{
    ensure!(
        base.is_absolute(),
        "configuration path base must be absolute"
    );
    reject_ambiguous_windows_path(path, "configuration path")?;
    let absolute_path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    };
    let contents = match fs::read_to_string(&absolute_path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            bail!(
                "listener configuration {} is missing; create a schema-v3 configuration from config.example.toml",
                absolute_path.display()
            )
        }
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "failed to read configuration at {}",
                    absolute_path.display()
                )
            });
        }
    };

    inspect_schema_version(&contents)
        .with_context(|| format!("invalid configuration at {}", absolute_path.display()))?;
    let document: ConfigDocument = toml::from_str(&contents)
        .with_context(|| format!("invalid configuration at {}", absolute_path.display()))?;
    debug_assert_eq!(document.schema_version, 3);
    validate_document(&document)?;

    let config_directory = absolute_path
        .parent()
        .context("configuration path must name a file")?
        .canonicalize()
        .with_context(|| {
            format!(
                "failed to resolve configuration directory for {}",
                absolute_path.display()
            )
        })?;
    prepare_document(document, &config_directory, environment)
}

fn inspect_schema_version(contents: &str) -> Result<()> {
    let table: toml::Table = toml::from_str(contents).context("configuration is not valid TOML")?;
    match table
        .get("schema_version")
        .and_then(toml::Value::as_integer)
    {
        Some(3) => Ok(()),
        Some(version) => bail!("unsupported schema_version {version}; expected 3"),
        None => bail!("unsupported schema_version: missing or non-integer; expected 3"),
    }
}

fn validate_document(document: &ConfigDocument) -> Result<()> {
    ensure!(
        !document.servers.is_empty(),
        "servers must contain at least one configured server"
    );
    ensure!(
        (1..=Semaphore::MAX_PERMITS).contains(&document.max_connections),
        "max_connections must be between 1 and {}",
        Semaphore::MAX_PERMITS
    );
    for server_id in document.servers.keys() {
        validate_server_id(server_id)?;
    }
    validate_endpoints(&document.servers)
}

fn validate_server_id(server_id: &str) -> Result<()> {
    let bytes = server_id.as_bytes();
    let edge_is_valid = |byte: u8| byte.is_ascii_lowercase() || byte.is_ascii_digit();
    ensure!(
        bytes.first().is_some_and(|byte| edge_is_valid(*byte))
            && bytes.last().is_some_and(|byte| edge_is_valid(*byte))
            && bytes
                .iter()
                .all(|byte| edge_is_valid(*byte) || *byte == b'-'),
        "invalid server ID {server_id:?}; expected nonempty lowercase ASCII kebab case"
    );
    Ok(())
}

fn prepare_document<F>(
    document: ConfigDocument,
    config_directory: &Path,
    environment: F,
) -> Result<PreparedConfig>
where
    F: Fn(&str) -> Option<OsString>,
{
    let secret_names: Arc<[OsString]> = document
        .servers
        .values()
        .filter_map(|server| server.rcon.as_ref())
        .map(|rcon| rcon.password_env.clone().into())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>()
        .into();
    let mut working_directories = BTreeMap::<PathBuf, String>::new();
    let mut servers = BTreeMap::new();

    for (server_id, server) in document.servers {
        let context = ServerContext::new(server_id.clone());
        let working_directory = resolve_working_directory(
            Path::new(&server.command.working_directory),
            config_directory,
            &server_id,
        )?;
        if let Some(existing) =
            working_directories.insert(working_directory.clone(), server_id.clone())
        {
            bail!(
                "servers {existing:?} and {server_id:?} use the same canonical working directory {}",
                working_directory.display()
            );
        }
        let program = resolve_program(
            OsStr::new(&server.command.program),
            config_directory,
            &server_id,
        )?;
        let icon = match server.icon_path {
            Some(path) => {
                let path = resolve_config_path(Path::new(&path), config_directory, "icon_path")?;
                Some(load_server_icon(&path, &context)?)
            }
            None => None,
        };
        let responder = MinecraftResponder::new(MinecraftResponderSettings {
            minecraft_version: server.minecraft_version,
            sleeping_motd_text: server.motd.text,
            sleeping_motd_color: server.motd.color,
            sleeping_motd_bold: server.motd.bold,
            startup_message_text: server.startup_message.text,
            startup_message_color: server.startup_message.color,
            startup_message_bold: server.startup_message.bold,
            server_icon: icon,
        })
        .with_context(|| format!("[server={server_id}] failed to construct Minecraft responder"))?;
        let timeouts = prepare_timeouts(&server_id, &server.timeouts)?;
        let rcon = server
            .rcon
            .map(|raw| prepare_rcon(&server_id, raw, &environment))
            .transpose()?;
        let launch = LaunchCommand::new(
            program,
            server
                .command
                .arguments
                .into_iter()
                .map(OsString::from)
                .collect(),
            working_directory,
        )
        .with_removed_environment(Arc::clone(&secret_names));
        let runtime = ServerRuntimeConfig {
            bind_endpoint: listener_endpoint(&server.listener)?,
            backend_endpoint: target_endpoint(&server.backend)?,
            handshake_timeout: timeouts.handshake,
            proxy_connect_timeout: timeouts.proxy_connect,
            supervisor: SupervisorConfig {
                context: context.clone(),
                launch,
                rcon,
                poll_interval: timeouts.poll,
                idle_timeout: timeouts.idle,
                startup_timeout: timeouts.startup,
                retry_interval: timeouts.retry,
                command_timeout: timeouts.command,
                shutdown_timeout: timeouts.shutdown,
            },
        };
        servers.insert(
            server_id,
            PreparedServerConfig {
                context,
                runtime,
                responder,
            },
        );
    }

    Ok(PreparedConfig {
        max_connections: document.max_connections,
        servers,
    })
}

struct PreparedTimeouts {
    poll: Duration,
    idle: Duration,
    startup: Duration,
    retry: Duration,
    command: Duration,
    shutdown: Duration,
    handshake: Duration,
    proxy_connect: Duration,
}

fn prepare_timeouts(server_id: &str, raw: &TimeoutsSchema) -> Result<PreparedTimeouts> {
    let duration = |name, seconds| {
        validate_duration(server_id, name, seconds).map(|()| Duration::from_secs(seconds))
    };
    Ok(PreparedTimeouts {
        poll: duration("poll_interval_seconds", raw.poll_interval_seconds)?,
        idle: duration("idle_timeout_seconds", raw.idle_timeout_seconds)?,
        startup: duration("startup_timeout_seconds", raw.startup_timeout_seconds)?,
        retry: duration("retry_interval_seconds", raw.retry_interval_seconds)?,
        command: duration("command_timeout_seconds", raw.command_timeout_seconds)?,
        shutdown: duration("shutdown_timeout_seconds", raw.shutdown_timeout_seconds)?,
        handshake: duration("handshake_timeout_seconds", raw.handshake_timeout_seconds)?,
        proxy_connect: duration(
            "proxy_connect_timeout_seconds",
            raw.proxy_connect_timeout_seconds,
        )?,
    })
}

fn validate_duration(server_id: &str, name: &str, seconds: u64) -> Result<()> {
    ensure!(
        seconds > 0,
        "[server={server_id}] {name} must be greater than zero"
    );
    ensure!(
        tokio::time::Instant::now()
            .checked_add(Duration::from_secs(seconds))
            .is_some(),
        "[server={server_id}] {name} is too large for Tokio timing on this platform"
    );
    Ok(())
}

fn prepare_rcon<F>(server_id: &str, raw: RconSchema, environment: &F) -> Result<RconConfig>
where
    F: Fn(&str) -> Option<OsString>,
{
    validate_environment_name(&raw.password_env)
        .with_context(|| format!("[server={server_id}] invalid RCON password_env"))?;
    let value = environment(&raw.password_env).with_context(|| {
        format!(
            "[server={server_id}] RCON password environment variable {} is missing",
            raw.password_env
        )
    })?;
    let value = value.into_string().map_err(|_| {
        anyhow::anyhow!(
            "[server={server_id}] RCON password environment variable {} is not Unicode",
            raw.password_env
        )
    })?;
    let password = RconSecret::new(value).with_context(|| {
        format!(
            "[server={server_id}] invalid RCON password in environment variable {}",
            raw.password_env
        )
    })?;
    Ok(RconConfig {
        endpoint: target_endpoint(&TargetSchema {
            host: raw.host,
            port: raw.port,
        })?,
        password,
    })
}

fn validate_environment_name(name: &str) -> Result<()> {
    let mut bytes = name.bytes();
    ensure!(
        bytes
            .next()
            .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
            && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
        "password_env must match [A-Za-z_][A-Za-z0-9_]*"
    );
    Ok(())
}

fn resolve_working_directory(path: &Path, base: &Path, server_id: &str) -> Result<PathBuf> {
    let resolved = resolve_config_path(path, base, "working_directory")?;
    let metadata = fs::metadata(&resolved).with_context(|| {
        format!(
            "[server={server_id}] working_directory {} does not exist or is unreadable",
            resolved.display()
        )
    })?;
    ensure!(
        metadata.is_dir(),
        "[server={server_id}] working_directory {} is not a directory",
        resolved.display()
    );
    resolved.canonicalize().with_context(|| {
        format!(
            "[server={server_id}] failed to canonicalize working_directory {}",
            resolved.display()
        )
    })
}

fn resolve_program(program: &OsStr, base: &Path, server_id: &str) -> Result<OsString> {
    ensure!(
        !program.is_empty(),
        "[server={server_id}] command.program must not be empty"
    );
    let path = Path::new(program);
    reject_ambiguous_windows_path(path, "command.program")
        .with_context(|| format!("[server={server_id}] invalid command.program"))?;
    if !is_path_like(program) {
        return Ok(program.to_os_string());
    }
    let resolved = if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    };
    let metadata = fs::metadata(&resolved).with_context(|| {
        format!(
            "[server={server_id}] path-like command.program {} does not resolve to a file",
            resolved.display()
        )
    })?;
    ensure!(
        metadata.is_file(),
        "[server={server_id}] path-like command.program {} is not a file",
        resolved.display()
    );
    Ok(resolved
        .canonicalize()
        .with_context(|| {
            format!(
                "failed to canonicalize command.program {}",
                resolved.display()
            )
        })?
        .into_os_string())
}

fn is_path_like(program: &OsStr) -> bool {
    let value = program.to_string_lossy();
    value.contains('/') || value.contains('\\') || Path::new(program).is_absolute()
}

fn resolve_config_path(path: &Path, base: &Path, description: &str) -> Result<PathBuf> {
    reject_ambiguous_windows_path(path, description)?;
    Ok(if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    })
}

fn reject_ambiguous_windows_path(path: &Path, description: &str) -> Result<()> {
    let value = path.as_os_str().to_string_lossy();
    let bytes = value.as_bytes();
    let drive_relative = bytes.len() >= 2
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && bytes
            .get(2)
            .is_none_or(|byte| !matches!(*byte, b'/' | b'\\'));
    let root_relative = cfg!(windows)
        && ((value.starts_with('\\') && !value.starts_with("\\\\"))
            || (value.starts_with('/') && !value.starts_with("//")));
    ensure!(
        !drive_relative && !root_relative,
        "{description} must not use a Windows drive-relative or root-relative path"
    );
    Ok(())
}

fn listener_endpoint(listener: &ListenerSchema) -> Result<Endpoint> {
    let address = validate_listener(listener)?;
    Ok(Endpoint::new(address.to_string(), listener.port))
}

fn target_endpoint(target: &TargetSchema) -> Result<Endpoint> {
    let host = validate_target(target)?;
    let host = match host {
        CanonicalHost::Ip(address) => address.to_string(),
        CanonicalHost::Dns(hostname) => hostname,
    };
    Ok(Endpoint::new(host, target.port))
}

fn validate_listener(listener: &ListenerSchema) -> Result<IpAddr> {
    ensure!(listener.port != 0, "listener port must not be zero");
    ensure!(
        !listener.address.starts_with('[') && !listener.address.ends_with(']'),
        "listener address must be an unbracketed numeric IP literal"
    );
    listener
        .address
        .parse()
        .context("listener address must be an unbracketed numeric IPv4 or IPv6 literal")
}

fn validate_target(target: &TargetSchema) -> Result<CanonicalHost> {
    ensure!(target.port != 0, "target port must not be zero");
    ensure!(!target.host.is_empty(), "target host must not be empty");
    ensure!(
        target.host.is_ascii()
            && !target.host.bytes().any(|byte| byte.is_ascii_whitespace())
            && !target.host.starts_with('[')
            && !target.host.ends_with(']'),
        "target host must be an unbracketed IP literal or portable ASCII hostname"
    );
    if let Ok(address) = target.host.parse::<IpAddr>() {
        ensure!(
            !address.is_unspecified() && !address.is_multicast(),
            "target IP address must not be unspecified or multicast"
        );
        return Ok(CanonicalHost::Ip(address));
    }
    validate_hostname(&target.host)?;
    Ok(CanonicalHost::Dns(target.host.to_ascii_lowercase()))
}

fn validate_hostname(host: &str) -> Result<()> {
    ensure!(
        host.len() <= 253 && !host.ends_with('.'),
        "target hostname must be at most 253 characters and must not have a trailing dot"
    );
    for label in host.split('.') {
        ensure!(
            (1..=63).contains(&label.len())
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
                && label
                    .as_bytes()
                    .first()
                    .is_some_and(u8::is_ascii_alphanumeric)
                && label
                    .as_bytes()
                    .last()
                    .is_some_and(u8::is_ascii_alphanumeric),
            "target host is not a portable ASCII hostname"
        );
    }
    Ok(())
}

fn validate_endpoints(servers: &BTreeMap<String, ServerSchema>) -> Result<()> {
    let mut listeners = Vec::new();
    let mut targets = Vec::new();
    for (server_id, server) in servers {
        listeners.push(ListenerRecord {
            server_id,
            address: validate_listener(&server.listener)
                .with_context(|| format!("[server={server_id}] invalid listener endpoint"))?,
            port: server.listener.port,
        });
        targets.push(TargetRecord {
            server_id,
            role: "backend",
            host: validate_target(&server.backend)
                .with_context(|| format!("[server={server_id}] invalid backend endpoint"))?,
            port: server.backend.port,
        });
        if let Some(rcon) = &server.rcon {
            targets.push(TargetRecord {
                server_id,
                role: "rcon",
                host: validate_target(&TargetSchema {
                    host: rcon.host.clone(),
                    port: rcon.port,
                })
                .with_context(|| format!("[server={server_id}] invalid RCON endpoint"))?,
                port: rcon.port,
            });
            validate_environment_name(&rcon.password_env)
                .with_context(|| format!("[server={server_id}] invalid RCON password_env"))?;
        }
    }

    for (index, left) in listeners.iter().enumerate() {
        for right in &listeners[index + 1..] {
            if left.port == right.port
                && (left.address == right.address
                    || (left.address.is_unspecified()
                        && same_ip_family(left.address, right.address))
                    || (right.address.is_unspecified()
                        && same_ip_family(left.address, right.address)))
            {
                bail!(
                    "endpoint conflict: [server={}] listener and [server={}] listener both own port {} on overlapping addresses",
                    left.server_id,
                    right.server_id,
                    left.port
                );
            }
        }
    }
    for (index, left) in targets.iter().enumerate() {
        for right in &targets[index + 1..] {
            if left.port == right.port && left.host == right.host {
                bail!(
                    "endpoint conflict: [server={}] {} and [server={}] {} resolve to the same endpoint",
                    left.server_id,
                    left.role,
                    right.server_id,
                    right.role
                );
            }
        }
    }
    for target in &targets {
        for listener in &listeners {
            if target.port != listener.port {
                continue;
            }
            let conflicts = match &target.host {
                CanonicalHost::Dns(_) => true,
                CanonicalHost::Ip(address) => {
                    *address == listener.address || listener.address.is_unspecified()
                }
            };
            if conflicts {
                bail!(
                    "endpoint conflict: [server={}] {} may target [server={}] listener on port {}",
                    target.server_id,
                    target.role,
                    listener.server_id,
                    listener.port
                );
            }
        }
    }
    Ok(())
}

const fn same_ip_family(left: IpAddr, right: IpAddr) -> bool {
    matches!(
        (left, right),
        (IpAddr::V4(_), IpAddr::V4(_)) | (IpAddr::V6(_), IpAddr::V6(_))
    )
}

fn load_server_icon(path: &Path, context: &ServerContext) -> Result<String> {
    let bytes = fs::read(path).with_context(|| {
        format!(
            "[server={context}] failed to read configured icon at {}",
            path.display()
        )
    })?;
    ensure!(
        bytes.len() <= MAX_ICON_FILE_SIZE,
        "[server={context}] icon at {} exceeds the {} MiB limit",
        path.display(),
        MAX_ICON_FILE_SIZE / (1024 * 1024)
    );
    let dimensions = ImageReader::with_format(Cursor::new(&bytes), ImageFormat::Png)
        .into_dimensions()
        .with_context(|| {
            format!(
                "[server={context}] icon at {} is not a valid PNG",
                path.display()
            )
        })?;
    ensure!(
        dimensions.0 <= MAX_ICON_DIMENSION && dimensions.1 <= MAX_ICON_DIMENSION,
        "[server={context}] icon at {} exceeds the {MAX_ICON_DIMENSION}x{MAX_ICON_DIMENSION} dimension limit",
        path.display()
    );
    let image =
        image::load_from_memory_with_format(&bytes, ImageFormat::Png).with_context(|| {
            format!(
                "[server={context}] icon at {} is not a valid PNG",
                path.display()
            )
        })?;
    debug_assert_eq!(image.dimensions(), dimensions);
    let image = if dimensions == (ICON_SIZE, ICON_SIZE) {
        image
    } else {
        log::info!(
            "[server={context}] Resizing icon in memory from {}x{} to {ICON_SIZE}x{ICON_SIZE}",
            dimensions.0,
            dimensions.1
        );
        image.resize_exact(ICON_SIZE, ICON_SIZE, FilterType::CatmullRom)
    };
    let mut output = Cursor::new(Vec::new());
    image
        .write_to(&mut output, ImageFormat::Png)
        .context("failed to encode the server icon")?;
    Ok(format!(
        "data:image/png;base64,{}",
        STANDARD.encode(output.into_inner())
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

    fn temporary_directory() -> PathBuf {
        let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "mcservernap-config-test-{}-{id}",
            std::process::id()
        ))
    }

    fn complete_config(max_connections: usize, rcon: bool) -> String {
        let rcon = if rcon {
            r#"
[servers.survival.rcon]
host = "127.0.0.1"
port = 25575
password_env = "TEST_RCON_PASSWORD"
"#
        } else {
            ""
        };
        format!(
            r#"schema_version = 3
max_connections = {max_connections}

[servers.survival]
minecraft_version = "26.2"

[servers.survival.listener]
address = "127.0.0.1"
port = 25565

[servers.survival.backend]
host = "127.0.0.1"
port = 25566

[servers.survival.command]
program = "java"
arguments = ["-jar", "server.jar", "argument with spaces"]
working_directory = "."

[servers.survival.timeouts]
poll_interval_seconds = 60
idle_timeout_seconds = 600
startup_timeout_seconds = 600
retry_interval_seconds = 2
command_timeout_seconds = 10
shutdown_timeout_seconds = 30
handshake_timeout_seconds = 5
proxy_connect_timeout_seconds = 10

[servers.survival.motd]
text = "Sleeping"
color = "aqua"
bold = true

[servers.survival.startup_message]
text = "Starting"
color = "light_purple"
bold = true
{rcon}"#
        )
    }

    fn write_config(contents: &str) -> (PathBuf, PathBuf) {
        let directory = temporary_directory();
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("cfg.toml");
        fs::write(&path, contents).unwrap();
        (directory, path)
    }

    fn two_server_config() -> String {
        let first = complete_config(1, false);
        let second = complete_config(1, false)
            .replace("survival", "creative")
            .replace("25565", "25567")
            .replace("25566", "25568");
        format!(
            "{first}\n{}",
            second.lines().skip(2).collect::<Vec<_>>().join("\n")
        )
    }

    fn load_test(path: &Path) -> Result<PreparedConfig> {
        let base = path.parent().unwrap();
        load_with(path, base, |name| {
            (name == "TEST_RCON_PASSWORD").then(|| OsString::from("secret"))
        })
    }

    #[test]
    fn accepts_complete_schema_with_optional_rcon_present_or_absent() {
        for rcon in [false, true] {
            let (directory, path) = write_config(&complete_config(512, rcon));
            let prepared = load_test(&path).expect("schema v3 should prepare");
            assert_eq!(prepared.servers.len(), 1);
            assert_eq!(
                prepared.servers["survival"]
                    .runtime
                    .supervisor
                    .rcon
                    .is_some(),
                rcon
            );
            fs::remove_dir_all(directory).ok();
        }
    }

    #[test]
    fn schema_version_is_checked_before_complete_deserialization() {
        for contents in [
            "max_connections = 1",
            "schema_version = 2\nunknown = true",
            "schema_version = 4",
        ] {
            let error = inspect_schema_version(contents).expect_err("version must be rejected");
            assert!(error.to_string().contains("schema_version"));
            assert!(error.to_string().contains("expected 3"));
        }
    }

    #[test]
    fn rejects_unknown_root_and_nested_fields_and_missing_required_fields() {
        let current = complete_config(1, false);
        for invalid in [
            current.replace("max_connections = 1", "max_connections = 1\nunknown = true"),
            current.replace("port = 25565", "port = 25565\nunknown = true"),
            current.replace(
                "arguments = [\"-jar\", \"server.jar\", \"argument with spaces\"]\n",
                "",
            ),
        ] {
            assert!(toml::from_str::<ConfigDocument>(&invalid).is_err());
        }
    }

    #[test]
    fn rejects_empty_servers_and_invalid_ids() {
        let empty: ConfigDocument =
            toml::from_str("schema_version=3\nmax_connections=1\nservers={}").unwrap();
        assert!(validate_document(&empty).is_err());
        for id in ["", "Upper", "-start", "end-", "two--parts", "under_score"] {
            let expected = id != "two--parts";
            assert_eq!(validate_server_id(id).is_err(), expected, "{id}");
        }
    }

    #[test]
    fn validates_connection_limit_protocol_bounds() {
        for (value, accepted) in [
            (0, false),
            (Semaphore::MAX_PERMITS, true),
            (Semaphore::MAX_PERMITS + 1, false),
        ] {
            let document: ConfigDocument = toml::from_str(&complete_config(value, false)).unwrap();
            assert_eq!(validate_document(&document).is_ok(), accepted);
        }
    }

    #[test]
    fn missing_configuration_is_actionable_and_creates_nothing() {
        let directory = temporary_directory();
        let path = directory.join("cfg.toml");
        let error = load_with(&path, &std::env::temp_dir(), |_| None)
            .err()
            .expect("missing configuration must fail");
        let message = error.to_string();
        assert!(message.contains("schema-v3"));
        assert!(message.contains("config.example.toml"));
        assert!(!directory.exists());
    }

    #[test]
    fn resolves_paths_from_configuration_directory_and_preserves_bare_program_and_arguments() {
        let (directory, path) = write_config(&complete_config(1, false));
        let prepared = load_with(Path::new("cfg.toml"), &directory, |_| None).unwrap();
        let runtime = &prepared.servers["survival"].runtime;
        assert_eq!(runtime.supervisor.launch.program, OsString::from("java"));
        assert_eq!(
            runtime.supervisor.launch.arguments,
            ["-jar", "server.jar", "argument with spaces"].map(OsString::from)
        );
        assert_eq!(
            runtime.supervisor.launch.working_directory,
            directory.canonicalize().unwrap()
        );
        assert_ne!(directory, std::env::current_dir().unwrap());
        assert_eq!(path, directory.join("cfg.toml"));
        fs::remove_dir_all(directory).ok();
    }

    #[test]
    fn endpoint_validation_rejects_normalized_duplicates_and_listener_loops() {
        let mut servers: BTreeMap<String, ServerSchema> =
            toml::from_str::<ConfigDocument>(&complete_config(1, false))
                .unwrap()
                .servers;
        let mut second: ServerSchema = toml::from_str::<ConfigDocument>(
            &complete_config(1, false)
                .replace("survival", "creative")
                .replace("25565", "25567"),
        )
        .unwrap()
        .servers
        .remove("creative")
        .unwrap();
        second.backend.host = "127.0.0.1".to_owned();
        servers.insert("creative".to_owned(), second);
        let error = validate_endpoints(&servers).expect_err("duplicate backends must fail");
        assert!(error.to_string().contains("survival"));
        assert!(error.to_string().contains("creative"));

        let only = servers.get_mut("creative").unwrap();
        only.backend.port = 25_567;
        only.backend.host = "LOCALHOST".to_owned();
        only.listener.port = 25_567;
        assert!(validate_endpoints(&servers).is_err());
    }

    #[test]
    fn path_like_program_missing_workdir_icon_and_duplicate_workdirs_fail_preparation() {
        let cases = [
            complete_config(1, false).replace(
                "working_directory = \".\"",
                "working_directory = \"missing\"",
            ),
            complete_config(1, false).replace("program = \"java\"", "program = \"./missing-java\""),
            complete_config(1, false).replace(
                "minecraft_version = \"26.2\"",
                "minecraft_version = \"26.2\"\nicon_path = \"missing.png\"",
            ),
            two_server_config(),
        ];
        for contents in cases {
            let (directory, path) = write_config(&contents);
            assert!(load_test(&path).is_err(), "{contents}");
            fs::remove_dir_all(directory).ok();
        }
    }

    #[test]
    fn configured_icon_is_resized_without_modifying_the_source() {
        let contents = complete_config(1, false).replace(
            "minecraft_version = \"26.2\"",
            "minecraft_version = \"26.2\"\nicon_path = \"icon.png\"",
        );
        let (directory, path) = write_config(&contents);
        let icon_path = directory.join("icon.png");
        image::DynamicImage::new_rgba8(128, 32)
            .save_with_format(&icon_path, ImageFormat::Png)
            .unwrap();

        let prepared = load_test(&path).expect("configured icon should prepare");
        assert_eq!(prepared.servers.len(), 1);
        assert_eq!(image::image_dimensions(&icon_path).unwrap(), (128, 32));
        fs::remove_dir_all(directory).ok();
    }

    #[test]
    fn absent_rcon_performs_no_environment_lookup() {
        let (directory, path) = write_config(&complete_config(1, false));
        load_with(&path, &directory, |_| {
            panic!("no-RCON configuration must not read secrets")
        })
        .expect("no-RCON configuration should prepare");
        fs::remove_dir_all(directory).ok();
    }

    #[test]
    fn secret_validation_rejects_missing_empty_nul_and_oversized_without_exposure() {
        let raw = || RconSchema {
            host: "127.0.0.1".to_owned(),
            port: 25_575,
            password_env: "TEST_SECRET".to_owned(),
        };
        let sentinel = "sentinel-secret-that-must-not-appear";
        for value in [
            None,
            Some(OsString::from("")),
            Some(OsString::from(format!("{sentinel}\0"))),
            Some(OsString::from("x".repeat(1_414))),
        ] {
            let error = prepare_rcon("survival", raw(), &|_| value.clone())
                .err()
                .expect("invalid secret must fail preparation");
            let message = format!("{error:#}");
            assert!(message.contains("survival"));
            assert!(message.contains("TEST_SECRET"));
            assert!(!message.contains(sentinel));
        }
    }

    #[test]
    fn secret_validation_rejects_non_unicode_values() {
        #[cfg(windows)]
        let invalid = {
            use std::os::windows::ffi::OsStringExt as _;
            OsString::from_wide(&[0xd800])
        };
        #[cfg(unix)]
        let invalid = {
            use std::os::unix::ffi::OsStringExt as _;
            OsString::from_vec(vec![0xff])
        };
        let error = prepare_rcon(
            "survival",
            RconSchema {
                host: "127.0.0.1".to_owned(),
                port: 25_575,
                password_env: "TEST_SECRET".to_owned(),
            },
            &|_| Some(invalid.clone()),
        )
        .err()
        .expect("non-Unicode secret must fail");
        assert!(error.to_string().contains("TEST_SECRET"));
    }

    #[test]
    fn malformed_hosts_zero_ports_cross_roles_wildcards_and_dns_listener_ports_fail() {
        for target in [
            TargetSchema {
                host: String::new(),
                port: 1,
            },
            TargetSchema {
                host: "[::1]".to_owned(),
                port: 1,
            },
            TargetSchema {
                host: "bad_host".to_owned(),
                port: 1,
            },
            TargetSchema {
                host: "127.0.0.1:25566".to_owned(),
                port: 1,
            },
            TargetSchema {
                host: "https://host".to_owned(),
                port: 1,
            },
            TargetSchema {
                host: "host".to_owned(),
                port: 0,
            },
            TargetSchema {
                host: "0.0.0.0".to_owned(),
                port: 1,
            },
            TargetSchema {
                host: "224.0.0.1".to_owned(),
                port: 1,
            },
        ] {
            assert!(validate_target(&target).is_err(), "{}", target.host);
        }

        let mut document: ConfigDocument = toml::from_str(&two_server_config()).unwrap();
        document.servers.get_mut("creative").unwrap().rcon = Some(RconSchema {
            host: "127.0.0.1".to_owned(),
            port: 25_566,
            password_env: "SECRET".to_owned(),
        });
        assert!(validate_endpoints(&document.servers).is_err());

        let creative = document.servers.get_mut("creative").unwrap();
        creative.rcon = None;
        creative.backend.host = "backend.local".to_owned();
        creative.backend.port = 25_565;
        assert!(validate_endpoints(&document.servers).is_err());

        let creative = document.servers.get_mut("creative").unwrap();
        creative.backend.host = "127.0.0.2".to_owned();
        creative.listener.address = "0.0.0.0".to_owned();
        creative.listener.port = 25_565;
        assert!(validate_endpoints(&document.servers).is_err());
    }

    #[test]
    fn wildcard_listener_rejects_targets_from_either_ip_family() {
        let mut document: ConfigDocument = toml::from_str(&complete_config(1, false)).unwrap();
        let server = document.servers.get_mut("survival").unwrap();
        server.listener.address = "::".to_owned();
        server.listener.port = 25_566;
        server.backend.host = "127.0.0.1".to_owned();
        assert!(validate_endpoints(&document.servers).is_err());

        let server = document.servers.get_mut("survival").unwrap();
        server.listener.address = "0.0.0.0".to_owned();
        server.backend.host = "::1".to_owned();
        assert!(validate_endpoints(&document.servers).is_err());
    }

    #[tokio::test]
    async fn distinct_specific_listener_addresses_can_share_a_port_when_binding_succeeds() {
        let first = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let port = first.local_addr().unwrap().port();
        let second = tokio::net::TcpListener::bind(("127.0.0.2", port))
            .await
            .expect("distinct loopback addresses should bind the same port");
        assert_ne!(
            first.local_addr().unwrap().ip(),
            second.local_addr().unwrap().ip()
        );
    }
}
