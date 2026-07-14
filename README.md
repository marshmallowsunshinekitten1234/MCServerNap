# MCServerNap

MCServerNap lets local Minecraft Java servers sleep when nobody is using them.
While a server is stopped, MCServerNap keeps its public port online and answers
server-list requests. A player joining that port starts the server and is asked
to reconnect shortly. Once the backend is ready, MCServerNap proxies connections
to it and stops servers it started after they have been idle long enough.

It supports stable Java releases from 1.20.1 through 26.2. Multiple servers can
run under one daemon, but each needs its own public listener port.

## Requirements

- Rust 1.88 or newer to build MCServerNap.
- An existing Minecraft Java server and a command that starts it.
- Separate ports for the public listener and the Minecraft backend.
- Optional RCON access for stronger readiness and player-count checks.

Only expose the MCServerNap listener publicly. Keep the backend and RCON ports
on a trusted local interface.

## Quick start

Build the release binary:

```console
git clone https://github.com/marshmallowsunshinekitten1234/MCServerNap.git
cd MCServerNap
cargo build --locked --release
```

Copy [config.example.toml](config.example.toml) to `config/cfg.toml`, then edit
the server version, ports, launch command, working directory, messages, and
timeouts. Also point `icon_path` to a PNG you have, or remove that line.

The example includes RCON. Set its password environment variable before
starting MCServerNap:

```powershell
$env:MCSERVERNAP_SURVIVAL_RCON_PASSWORD = "replace-with-your-password"
target\release\mcservernap.exe listen
```

```bash
export MCSERVERNAP_SURVIVAL_RCON_PASSWORD='replace-with-your-password'
target/release/mcservernap listen
```

If you do not use RCON, remove the complete `[servers.survival.rcon]` table
instead. Use `--config <path>` or `MCSERVERNAP_CONFIG` when the file is not at
`config/cfg.toml`.

For multiple RCON-enabled servers, give each `rcon` table its own `password_env`
name and set each named environment variable before starting the daemon.

Stop the daemon with Ctrl+C. On Unix, SIGTERM is also supported.

## How it behaves

- Server-list status requests show the configured sleeping MOTD and do not wake
  the backend.
- A Login or supported Transfer connection wakes a stopped backend. That
  connection and any further attempts receive the configured startup message;
  the player reconnects after the server is ready.
- MCServerNap only stops server processes that it started. An already-running
  backend can be proxied, but remains under the operator's control.
- Startup failures are isolated to the affected server. A later player attempt
  can retry after its cooldown.

With RCON configured, MCServerNap uses it to confirm readiness, check the player
count, and send the graceful `stop` command. It only stops after both RCON and
proxy activity indicate that the server is idle.

Without RCON, readiness means that the backend accepted a TCP connection, idle
tracking relies on connections through the proxy, and graceful shutdown sends
`stop` to the owned process's standard input. All player traffic must therefore
go through the public listener. The launch command must stay attached to Java
and preserve standard input for the server's full lifetime.

## Configuration notes

Start from the example file and leave `schema_version = 3` unchanged. For
another server, copy the complete `[servers.<id>]` section and use a different
listener port, backend port, and working directory. Relative paths start from
the directory containing the configuration file.

## Direct RCON stop

MCServerNap also includes a small standalone RCON stop command:

```console
target/release/mcservernap stop --rcon-port 25575
```

It uses `127.0.0.1` by default and reads the password from
`MCSERVERNAP_RCON_PASSWORD`, or from `--rcon-pass`. This command talks directly
to RCON and does not use the daemon configuration or its per-server
`password_env` names.

## Troubleshooting

Logging defaults to `info`. Set `RUST_LOG=debug` for connection, startup, and
reconciliation details. Logs produced by MCServerNap include the server ID;
Minecraft's own output is inherited unchanged.

If startup fails, first check the paths relative to the configuration file, the
backend and listener ports, and any configured RCON password environment
variable.

## License

MIT. See [LICENSE](LICENSE).
