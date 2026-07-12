# MCServerNap

MCServerNap lets a local Minecraft Java server stop when it is empty and start again when somebody joins. While the server is asleep, it remains visible in the server list. The first join attempt starts it and asks the player to reconnect once it is ready.

MCServerNap supports every stable Minecraft Java release from **1.20.1 through 26.2**: 1.20.1–1.20.6, 1.21–1.21.11, and 26.1–26.2.

Vanilla, Forge, NeoForge, and Fabric servers all use the same wake-up flow. While the Minecraft server is stopped, MCServerNap shows the configured sleeping message and Minecraft version, plus the server icon if one is configured. MCServerNap does not inspect the server installation, so this temporary entry does not identify the mod loader or installed mods. Once the real server is running, MCServerNap forwards connections unchanged and the server supplies its normal server-list entry.

## Requirements

- A supported Minecraft Java server with RCON enabled.
- Rust 1.88 or newer to build MCServerNap.
- Separate ports for MCServerNap and the Minecraft server.

Only expose the MCServerNap port publicly. Keep the backend and RCON ports on a trusted interface.

Example `server.properties`:

```properties
server-port=25566
enable-rcon=true
rcon.port=25575
rcon.password=replace-with-a-long-random-password
```

## Build

```console
git clone https://github.com/marshmallowsunshinekitten1234/MCServerNap.git
cd MCServerNap
cargo build --locked --release
```

The compiled binary is placed in `target/release`.

## Run

Pass the RCON password through the environment so it does not end up in shell history.

PowerShell:

```powershell
$env:MCSERVERNAP_RCON_PASSWORD = "replace-with-a-long-random-password"
target\release\mcservernap.exe listen 0.0.0.0 25565 `
  --server-port 25566 `
  --rcon-port 25575 `
  java -- -Xms2G -Xmx5G -jar server.jar nogui
```

Bash:

```bash
export MCSERVERNAP_RCON_PASSWORD='replace-with-a-long-random-password'
target/release/mcservernap listen 0.0.0.0 25565 \
  --server-port 25566 \
  --rcon-port 25575 \
  java -- -Xms2G -Xmx5G -jar server.jar nogui
```

Everything after `--` is passed to the server command. The backend and RCON hosts default to `127.0.0.1`; use `--server-host` or `--rcon-host` if needed.

The first login attempt starts the server and receives the configured startup message. The player reconnects once the server is ready.

Launch commands inherit MCServerNap's working directory. A launch script should change to the server directory and remain attached to Java until it exits. On Unix, finish with `exec java ...`; on Windows, do not use `start` or `Start-Process`.

## Configuration

The first run creates `config/cfg.toml`. Use `--config <path>` or `MCSERVERNAP_CONFIG` to put it elsewhere. A complete example is available in [config.example.toml](config.example.toml).

Existing configurations must set `schema_version = 2` and add the matching `minecraft_version`.

| Setting | Default | Purpose |
| --- | ---: | --- |
| `schema_version` | `2` | Configuration format required by this release. |
| `minecraft_version` | `"26.2"` | Exact backend release, from `"1.20.1"` through `"26.2"`. |
| `rcon_poll_interval_seconds` | `60` | Time between successful player-count checks. |
| `rcon_idle_timeout_seconds` | `600` | Confirmed-empty time before shutdown. |
| `rcon_startup_timeout_seconds` | `600` | Maximum time to wait for RCON during startup. |
| `rcon_retry_interval_seconds` | `2` | Delay between failed RCON connections. |
| `rcon_command_timeout_seconds` | `10` | Timeout for an RCON connection or command. |
| `shutdown_timeout_seconds` | `30` | Graceful shutdown time before forced termination. |
| `handshake_timeout_seconds` | `5` | Timeout for a sleeping client's initial packets. |
| `proxy_connect_timeout_seconds` | `10` | Time allowed to connect to the backend. |
| `max_connections` | `512` | Maximum concurrent client tasks. |
| `motd_*` | — | Sleeping server-list message and style. |
| `connection_msg_*` | — | Message shown while starting the server. |

Every duration must be greater than zero. Configuration is strict: missing fields, unknown fields, and unsupported schema versions are rejected instead of being guessed or migrated.

To use a server icon, place `server-icon.png` beside the configuration file. PNG files up to 8 MiB and 4096×4096 are accepted. The icon is resized to 64×64 in memory; the source file is not modified.

## Stop command

To stop an already-running server through RCON:

```console
target/release/mcservernap stop --rcon-port 25575
```

`MCSERVERNAP_RCON_PASSWORD` is used here as well.

## Logging

Logging defaults to `info`. Set `RUST_LOG=debug` when troubleshooting:

```console
RUST_LOG=debug target/release/mcservernap listen ...
```

MCServerNap will not stop the server when RCON is unavailable or its `list` response cannot be understood. This is deliberate: an unknown player count is never treated as an empty server.

## License

MIT. See [LICENSE](LICENSE).
