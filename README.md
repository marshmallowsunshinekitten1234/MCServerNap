# MCServerNap

MCServerNap is a native daemon that keeps one or more local Minecraft Java servers reachable while stopped, wakes each server when a player connects to its public port, proxies traffic while it runs, and stops owned servers after a confirmed idle period. It supports stable Java releases from 1.20.1 through 26.2.

Each configured server has its own public listener port, backend process, lifecycle, cooldown, and idle policy. This release does not route several servers through one port.

## Requirements

- Rust 1.88 or newer to build MCServerNap.
- A separate public listener and backend port for each server.
- A launch command that remains attached for the backend's complete lifetime.
- Optional RCON access when RCON-backed readiness, player counts, and commands are desired.

Keep backend and RCON endpoints on trusted, non-public interfaces. Prefer IP literals for local backend and RCON targets so configuration conflicts can be proven statically.

## Build

```console
git clone https://github.com/marshmallowsunshinekitten1234/MCServerNap.git
cd MCServerNap
cargo build --locked --release
```

The compiled binary is placed in `target/release`.

## Configure

Copy [config.example.toml](config.example.toml), then adjust its paths, ports, messages, and RCON environment-variable name. MCServerNap does not create a configuration automatically. The configuration is strict: schema version 3 is the only accepted version, all documented tables and fields are required except `icon_path` and the complete `rcon` table, and unknown fields are rejected.

Repeat the complete `[servers.<id>]` hierarchy for every additional server. IDs use lowercase ASCII kebab case. `max_connections` is a daemon-wide limit shared by all listeners.

Relative `icon_path`, `working_directory`, and path-like `command.program` values resolve from the directory containing the configuration. A bare program such as `java` uses normal `PATH` lookup. Arguments are passed exactly as configured without a shell, interpolation, splitting, or rewriting; relative arguments such as `server.jar` are interpreted by the launched process from `working_directory`.

Configured icons must be readable PNG files no larger than 8 MiB or 4096×4096. MCServerNap resizes them to 64×64 in memory without modifying the source file. Canonical working directories must be unique across servers.

When RCON is configured, set every referenced secret before starting the daemon. Values are read once during preparation, before any listener binds, and environment changes take effect only after a daemon restart.

PowerShell:

```powershell
$env:MCSERVERNAP_SURVIVAL_RCON_PASSWORD = "replace-with-a-long-random-password"
target\release\mcservernap.exe --config config\cfg.toml listen
```

Bash:

```bash
export MCSERVERNAP_SURVIVAL_RCON_PASSWORD='replace-with-a-long-random-password'
target/release/mcservernap --config config/cfg.toml listen
```

Only after every server prepares successfully and every public listener binds does MCServerNap activate the runtimes. Ctrl+C and, on Unix, SIGTERM stop all runtimes concurrently. A lifecycle failure such as a launch failure, readiness timeout, cooldown, or inconclusive reconciliation remains isolated to that server; infrastructure, task panic, process ownership, or cleanup failures shut down the daemon.

## Migrating schema v2

Schema v2 is rejected and is not upgraded automatically:

1. Set `schema_version = 3`.
2. Choose a stable lower-kebab server ID.
3. Move behavioral settings into `[servers.<id>]` and its nested tables.
4. Keep `max_connections` at the root.
5. Move the old listener CLI host and port into `listener`.
6. Move the old backend CLI values into `backend`.
7. Move the launch command and arguments into `command`.
8. Set `working_directory` explicitly.
9. Add `icon_path` explicitly if an icon is desired.
10. Add the complete RCON table with `password_env`, or omit it deliberately.
11. Repeat the hierarchy for every additional server.

## Lifecycle and idle behavior

When a backend is confirmed stopped, the first Login or Transfer attempt starts it and receives the configured startup message. The player reconnects after the server is ready. Status traffic does not wake the server or reset its activity timer.

With RCON, automatic idle stopping requires both proxy-session idleness and continuous RCON evidence of zero players, followed by a fresh final RCON check. An unknown player count is never treated as zero. If startup or runtime failure occurs, MCServerNap waits through the server's cooldown and only retries after new player demand.

Without RCON:

- TCP readiness proves only that the backend accepted a connection; it does not prove world initialization is complete.
- MCServerNap cannot observe players who bypass its public listener, so direct backend access can invalidate proxy-only idle decisions. All player traffic must use the public listener and the backend should remain on a trusted, non-public interface.
- Graceful stopping uses the owned process console by sending `stop\n`.
- An externally started backend remains unowned and is never stopped automatically.

The configured launch command must remain attached for the backend's complete lifetime and preserve stdin through to Java. On Unix, a wrapper should finish with `exec java ...`; on Windows, do not detach with `start` or `Start-Process`. Java stdout and stderr remain inherited, so output from several servers may interleave.

## Standalone stop command

The standalone command is an out-of-band direct RCON client:

```console
target/release/mcservernap stop --rcon-host 127.0.0.1 --rcon-port 25575
```

Set `MCSERVERNAP_RCON_PASSWORD` or pass `--rcon-pass`. This command does not select a configured server, contact the listener daemon, or load the listener configuration; it still works if `--config` points to a missing or invalid file.

## Logging and troubleshooting

Logging defaults to `info`; set `RUST_LOG=debug` for connection and reconciliation details. MCServerNap-owned server messages include `[server=<id>]`. Java output is inherited unchanged.

If configuration validation reports a listener/target ownership conflict, use distinct ports or more specific IP literals. Actual socket binding remains authoritative for local-address availability, IPv4/IPv6 dual-stack interaction, and OS-specific exclusive-bind behavior.

## License

MIT. See [LICENSE](LICENSE).
