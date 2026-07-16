# MCServerNap

MCServerNap lets local Minecraft Java servers sleep when nobody is using them.
While a server is stopped, MCServerNap keeps its public port online and answers
server-list requests. A player joining that port starts the Minecraft server and
is asked to reconnect shortly. Once the server is ready, MCServerNap proxies
connections to it and stops servers it started after they have been idle long
enough.

It supports stable Java releases from 1.20.1 through 26.2. Multiple servers can
run under one daemon, but each needs its own public port.

## Requirements

- Rust 1.88 or newer to build MCServerNap.
- An existing Minecraft Java server and a command that starts it.
- One public port for players and a separate private port for the Minecraft
  server.
- Optional RCON access for stronger readiness and player-count checks.

Only expose the MCServerNap port publicly. Keep the Minecraft server and RCON
ports on a trusted local interface.

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

## Server control commands

Run control commands as the same operating-system account that started the
daemon. If a system service runs the daemon under a dedicated account, run
these commands as that account too.

List configured servers, check their status, or request a start, stop, or
restart. These commands talk to the running MCServerNap process. The daemon
uses the configuration it loaded at startup, so the commands do not reread the
configuration file:

```console
target/release/mcservernap server list
target/release/mcservernap server status survival
target/release/mcservernap server start survival
target/release/mcservernap server stop survival
target/release/mcservernap server restart survival
```

`server status` reports the current phase and recent failure or retry
information. Start, stop, and restart are asynchronous: a successful command
means the daemon accepted the request or no action was needed, not necessarily
that Minecraft has finished starting or stopping. Use `server status` to follow
progress.

If the connection to the daemon fails before the result is known, the CLI
reports an uncertain outcome and does not automatically resend the command.
The command may already have been accepted, so check `server status` before
deciding what to do next.

`server stop` disconnects current players before stopping a Minecraft process
started by MCServerNap. It does not disable future wake-ups: a later player can
start the server again. MCServerNap never stops, restarts, or takes ownership of
a Minecraft process it did not start.

`server restart` disconnects current players, stops the old Minecraft process
that MCServerNap started, waits for it to exit, checks the server port again,
and then starts one replacement. Use `server status` and reconnect after the
phase returns to `running`. A process that MCServerNap did not start, or whose
ownership is uncertain, is left untouched.

Sending `server stop` during a restart can cancel it only while the old process
is still being cleaned up. If the replacement has already begun, wait until its
phase is `starting` or `running`, then send `server stop` again.

Control commands are available only on the local machine. On Unix, one daemon
is supported per user account. On Windows, access is controlled by that
account's Windows permissions; machine administrators and LocalSystem remain
privileged.

## How it behaves

- Server-list status requests show the configured sleeping MOTD and do not wake
  the Minecraft server.
- A player connecting to a stopped server wakes it. That connection and any
  further attempts receive the configured startup message; the player
  reconnects after the server is ready.
- MCServerNap only stops server processes that it started. An already-running
  Minecraft server can be proxied, but remains under the operator's control.
- Startup failures are isolated to the affected server. A later player attempt
  can retry after its cooldown.

With RCON configured, MCServerNap uses it to confirm readiness, check the player
count, and send the graceful `stop` command. It stops only after both RCON and
connections through MCServerNap indicate that the server is idle.

Without RCON, MCServerNap considers the server ready when its private port
accepts a connection. It tracks activity through player connections and sends
`stop` through the server process's standard input. All players must therefore
connect through the MCServerNap port. The launch command must keep Java in the
foreground and preserve standard input; do not use a script that starts Java in
the background and then exits.

`command_timeout_seconds` applies separately to finishing active player
connections and sending a graceful RCON or console stop command.
`shutdown_timeout_seconds` then limits how long MCServerNap waits for the
process to exit.

## Configuration notes

Start from the example file and leave `schema_version = 3` unchanged. For
another server, copy the complete `[servers.<id>]` section and use a different
public listener port, private Minecraft (`backend`) port, and working directory.
Relative paths start from the directory containing the configuration file.

## Direct RCON stop

MCServerNap also includes a small standalone RCON stop command:

```console
target/release/mcservernap stop --rcon-port 25575
```

It uses `127.0.0.1` by default and reads the password from
`MCSERVERNAP_RCON_PASSWORD`, or from `--rcon-pass`. This command talks directly
to RCON and does not use the daemon configuration or its per-server
`password_env` names. It is distinct from `server stop`, remains usable when the
listener configuration is missing or invalid, and never contacts the control
daemon.

## Troubleshooting

Logging defaults to `info`. Set `RUST_LOG=debug` for connection, startup, and
server-state details. Logs produced by MCServerNap include the server ID;
Minecraft's own output is inherited unchanged.

If startup fails, first check the paths relative to the configuration file, the
public and private Minecraft ports, and any configured RCON password
environment variable.

## License

MIT. See [LICENSE](LICENSE).
