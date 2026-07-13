use std::fs;
use std::io::Cursor;
use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use image::imageops::FilterType;
use image::{GenericImageView as _, ImageFormat, ImageReader};
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

use crate::minecraft::MinecraftVersion;

const MAX_ICON_FILE_SIZE: usize = 8 * 1024 * 1024;
const MAX_ICON_DIMENSION: u32 = 4_096;
const ICON_SIZE: u32 = 64;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Exact configuration schema understood by this release.
    pub schema_version: u32,
    /// Minecraft Java release served by the backend.
    pub minecraft_version: MinecraftVersion,
    /// Seconds between successful RCON player-count polls.
    pub rcon_poll_interval_seconds: u64,
    /// Continuous confirmed-empty seconds before the server is stopped.
    pub rcon_idle_timeout_seconds: u64,
    /// Maximum seconds to wait for RCON during server startup.
    pub rcon_startup_timeout_seconds: u64,
    /// Seconds between RCON reconnection attempts.
    pub rcon_retry_interval_seconds: u64,
    /// Per-operation timeout for RCON connections and commands.
    pub rcon_command_timeout_seconds: u64,
    /// Seconds to wait after `stop` before forcefully terminating the process.
    pub shutdown_timeout_seconds: u64,
    /// Total seconds to receive the handshake and any required sleeping packet.
    pub handshake_timeout_seconds: u64,
    /// Seconds allowed for a proxy connection to reach the backend server.
    pub proxy_connect_timeout_seconds: u64,
    /// Maximum simultaneous client/proxy tasks.
    pub max_connections: usize,
    pub motd_text: String,
    pub motd_color: String,
    pub motd_bold: bool,
    pub connection_msg_text: String,
    pub connection_msg_color: String,
    pub connection_msg_bold: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            schema_version: 2,
            minecraft_version: MinecraftVersion::latest(),
            rcon_poll_interval_seconds: 60,
            rcon_idle_timeout_seconds: 600,
            rcon_startup_timeout_seconds: 600,
            rcon_retry_interval_seconds: 2,
            rcon_command_timeout_seconds: 10,
            shutdown_timeout_seconds: 30,
            handshake_timeout_seconds: 5,
            proxy_connect_timeout_seconds: 10,
            max_connections: 512,
            motd_text: "Napping... Join to start server".to_owned(),
            motd_color: "aqua".to_owned(),
            motd_bold: true,
            connection_msg_text: "Server is starting. Please wait a moment, then reconnect."
                .to_owned(),
            connection_msg_color: "light_purple".to_owned(),
            connection_msg_bold: true,
        }
    }
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.schema_version == 2,
            "unsupported schema_version {}; expected 2",
            self.schema_version
        );
        for (name, seconds) in [
            (
                "rcon_poll_interval_seconds",
                self.rcon_poll_interval_seconds,
            ),
            ("rcon_idle_timeout_seconds", self.rcon_idle_timeout_seconds),
            (
                "rcon_startup_timeout_seconds",
                self.rcon_startup_timeout_seconds,
            ),
            (
                "rcon_retry_interval_seconds",
                self.rcon_retry_interval_seconds,
            ),
            (
                "rcon_command_timeout_seconds",
                self.rcon_command_timeout_seconds,
            ),
            ("shutdown_timeout_seconds", self.shutdown_timeout_seconds),
            ("handshake_timeout_seconds", self.handshake_timeout_seconds),
            (
                "proxy_connect_timeout_seconds",
                self.proxy_connect_timeout_seconds,
            ),
        ] {
            validate_duration(name, seconds)?;
        }
        ensure!(
            (1..=Semaphore::MAX_PERMITS).contains(&self.max_connections),
            "max_connections must be between 1 and {}",
            Semaphore::MAX_PERMITS
        );
        Ok(())
    }
}

fn validate_duration(name: &str, seconds: u64) -> Result<()> {
    ensure!(seconds > 0, "{name} must be greater than zero");
    let duration = Duration::from_secs(seconds);
    ensure!(
        Instant::now().checked_add(duration).is_some(),
        "{name} is too large for this platform"
    );
    Ok(())
}

#[derive(Debug)]
pub struct LoadedConfig {
    pub settings: Config,
    /// Complete `data:image/png;base64,...` favicon value for a status response.
    pub server_icon: Option<String>,
}

pub fn load_or_create(path: &Path) -> Result<LoadedConfig> {
    let settings = match fs::read_to_string(path) {
        Ok(contents) => toml::from_str(&contents)
            .with_context(|| format!("invalid configuration at {}", path.display()))?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => write_default(path)?,
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to read configuration at {}", path.display()));
        }
    };

    settings.validate()?;

    let icon_path = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .join("server-icon.png");
    let server_icon = load_server_icon(&icon_path)?;

    Ok(LoadedConfig {
        settings,
        server_icon,
    })
}

fn write_default(path: &Path) -> Result<Config> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create configuration directory {}",
                parent.display()
            )
        })?;
    }

    let settings = Config::default();
    let contents = toml::to_string_pretty(&settings).context("failed to serialize defaults")?;
    fs::write(path, contents)
        .with_context(|| format!("failed to create configuration at {}", path.display()))?;
    log::info!("Created default configuration at {}", path.display());
    Ok(settings)
}

fn load_server_icon(path: &Path) -> Result<Option<String>> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            log::debug!("No server icon found at {}", path.display());
            return Ok(None);
        }
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to read server icon at {}", path.display()));
        }
    };

    ensure!(
        bytes.len() <= MAX_ICON_FILE_SIZE,
        "server icon at {} exceeds the {} MiB limit",
        path.display(),
        MAX_ICON_FILE_SIZE / (1024 * 1024)
    );

    let dimensions = ImageReader::with_format(Cursor::new(&bytes), ImageFormat::Png)
        .into_dimensions()
        .with_context(|| format!("server icon at {} is not a valid PNG", path.display()))?;
    ensure!(
        dimensions.0 <= MAX_ICON_DIMENSION && dimensions.1 <= MAX_ICON_DIMENSION,
        "server icon at {} exceeds the {MAX_ICON_DIMENSION}x{MAX_ICON_DIMENSION} dimension limit",
        path.display()
    );

    let image = image::load_from_memory_with_format(&bytes, ImageFormat::Png)
        .with_context(|| format!("server icon at {} is not a valid PNG", path.display()))?;
    debug_assert_eq!(image.dimensions(), dimensions);
    let image = if dimensions == (ICON_SIZE, ICON_SIZE) {
        image
    } else {
        log::info!(
            "Resizing server icon in memory from {}x{} to {ICON_SIZE}x{ICON_SIZE}",
            dimensions.0,
            dimensions.1
        );
        image.resize_exact(ICON_SIZE, ICON_SIZE, FilterType::CatmullRom)
    };

    // Re-encoding strips unnecessary metadata and bounds the status response size.
    let mut output = Cursor::new(Vec::new());
    image
        .write_to(&mut output, ImageFormat::Png)
        .context("failed to encode the server icon")?;

    Ok(Some(format!(
        "data:image/png;base64,{}",
        STANDARD.encode(output.into_inner())
    )))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

    fn temporary_config_path() -> PathBuf {
        let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "mcservernap-config-test-{}-{id}/cfg.toml",
            std::process::id()
        ))
    }

    #[test]
    fn creates_and_reloads_default_configuration() {
        let path = temporary_config_path();

        let first = load_or_create(&path).expect("default configuration should be created");
        let second = load_or_create(&path).expect("configuration should be reloadable");

        assert_eq!(first.settings.rcon_idle_timeout_seconds, 600);
        assert_eq!(second.settings.max_connections, 512);
        assert!(first.server_icon.is_none());
        fs::remove_dir_all(path.parent().expect("temporary path has a parent")).ok();
    }

    #[test]
    fn rejects_zero_intervals() {
        let settings = Config {
            rcon_poll_interval_seconds: 0,
            ..Config::default()
        };

        assert!(settings.validate().is_err());
    }

    #[test]
    fn rejects_unrepresentable_resource_limits() {
        let unrepresentable_duration = Config {
            rcon_idle_timeout_seconds: u64::MAX,
            ..Config::default()
        };
        assert!(unrepresentable_duration.validate().is_err());

        let unsupported_connection_count = Config {
            max_connections: Semaphore::MAX_PERMITS + 1,
            ..Config::default()
        };
        assert!(unsupported_connection_count.validate().is_err());
    }

    #[test]
    fn rejects_obsolete_incomplete_and_unknown_schemas() {
        let current = toml::to_string(&Config::default()).expect("defaults should serialize");
        let without_version = current
            .lines()
            .filter(|line| !line.starts_with("schema_version"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(toml::from_str::<Config>(&without_version).is_err());

        let with_obsolete_field = format!("{current}\nconfig_directory_name = \"config\"\n");
        assert!(toml::from_str::<Config>(&with_obsolete_field).is_err());

        let unsupported = Config {
            schema_version: 1,
            ..Config::default()
        };
        assert!(unsupported.validate().is_err());
    }

    #[test]
    fn resizes_icon_in_memory_without_modifying_source() {
        let path = temporary_config_path();
        let directory = path.parent().expect("temporary path has a parent");
        fs::create_dir_all(directory).expect("temporary directory should be created");
        let icon_path = directory.join("server-icon.png");
        image::DynamicImage::new_rgba8(128, 32)
            .save_with_format(&icon_path, ImageFormat::Png)
            .expect("source icon should be written");

        let loaded = load_or_create(&path).expect("configuration and icon should load");
        let encoded = loaded
            .server_icon
            .expect("loaded configuration should contain an icon");
        let png = STANDARD
            .decode(
                encoded
                    .strip_prefix("data:image/png;base64,")
                    .expect("icon has the data URL prefix"),
            )
            .expect("icon should contain valid base64");
        let served = image::load_from_memory_with_format(&png, ImageFormat::Png)
            .expect("served icon should be a PNG");

        assert_eq!(served.dimensions(), (64, 64));
        assert_eq!(
            image::image_dimensions(&icon_path).expect("source dimensions should remain readable"),
            (128, 32)
        );
        fs::remove_dir_all(directory).ok();
    }
}
