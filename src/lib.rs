pub mod config;
pub mod preserialized_packets;
pub mod process;
pub mod rcon;
pub mod supervisor;

use crate::preserialized_packets::PreserializedPackets;
use ::rcon::Connection;
use anyhow::Result;
use regex::Regex;
use std::io::ErrorKind;
// use std::mem::discriminant;
use std::net::SocketAddr;
use std::sync::LazyLock;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Duration, Instant, interval, timeout};

/// Basic enum to provide state machine system for server status
#[derive(Debug)]
pub enum ServerState {
    Stopped,
    Starting {
        child: tokio::process::Child,
    },
    Running {
        child: tokio::process::Child,
        rcon_watchdog_handle: tokio::task::JoinHandle<()>,
    },
}

impl ServerState {
    fn variant_name(&self) -> &str {
        match self {
            ServerState::Stopped => "Stopped",
            ServerState::Starting { .. } => "Starting",
            ServerState::Running { .. } => "Running",
        }
    }

    pub fn switch_to(&mut self, new_state: ServerState) -> Result<()> {
        // Commented out because it's problematic to use because of
        // potential orphan server processes when dropping the child
        // process of a new state
        /*
        if discriminant(self) == discriminant(&new_state) {
            log::debug!(
                "State is already {:?}, no need to switch",
                self.variant_name()
            );
            return Ok(());
        }
        */

        match (&*self, &new_state) {
            (ServerState::Stopped, ServerState::Stopped) => {
                log::debug!(
                    "State is already {:?}, no need to switch",
                    ServerState::Stopped.variant_name()
                );
                return Ok(());
            }
            _ => {}
        }

        let valid_switch = match (&*self, &new_state) {
            (ServerState::Stopped, ServerState::Starting { .. }) => true,
            (ServerState::Starting { .. }, ServerState::Running { .. }) => true,
            (ServerState::Starting { .. }, ServerState::Stopped) => true,
            (ServerState::Running { .. }, ServerState::Stopped) => true,
            _ => false,
        };

        if valid_switch {
            log::debug!(
                "Switching state: {:?} → {:?}",
                self.variant_name(),
                new_state.variant_name()
            );
            match std::mem::replace(self, new_state) {
                ServerState::Running {
                    child: _,
                    rcon_watchdog_handle,
                } => {
                    rcon_watchdog_handle.abort();
                }
                _ => {}
            }
            return Ok(());
        }

        return Err(anyhow::anyhow!(
            "Invalid state transition: {:?} → {:?}",
            self,
            new_state
        ));
    }
}

static PLAYER_COUNT_RE: LazyLock<Regex> =
    LazyLock::new(|| return Regex::new(r"There are (\d+) of a max").unwrap());

/// Read a VarInt (Minecraft format) from the buffer, returning (value, bytes_read). Returns None if malformed
fn read_varint(buf: &[u8]) -> Option<(i32, usize)> {
    let mut num_read = 0;
    let mut result = 0i32;
    for &byte in buf.iter() {
        let val = (byte & 0x7F) as i32;
        result |= val << (7 * num_read);
        num_read += 1;
        if byte & 0x80 == 0 {
            return Some((result, num_read));
        }
        if num_read >= 5 {
            return None;
        }
    }
    None
}

// Write a VarInt (Minecraft format)
pub fn write_varint(mut val: i32, buf: &mut Vec<u8>) {
    loop {
        if (val & !0x7F) == 0 {
            buf.push(val as u8);
            return;
        } else {
            buf.push(((val & 0x7F) | 0x80) as u8);
            val >>= 7;
        }
    }
}

// Verifies a full Minecraft handshake on a single TcpStream.
pub async fn verify_handshake_packet(
    socket: &mut TcpStream,
    peer: SocketAddr,
    packets: &PreserializedPackets,
) -> Result<bool> {
    // 1) Read initial data, ignoring resets or immediate closes
    let mut buf = [0u8; 512];

    let n = match timeout(Duration::from_secs(5), socket.read(&mut buf)).await {
        Ok(Ok(0)) => {
            log::debug!("Connection closed immediately by {}", peer);
            return Ok(false);
        }
        Ok(Ok(n)) => n,
        Ok(Err(e)) if e.kind() == ErrorKind::ConnectionReset => {
            log::debug!("Connection reset by peer {} (ignoring)", peer);
            return Ok(false);
        }
        Ok(Err(e)) => {
            // Unexpected I/O error, propagate
            return Err(e.into());
        }
        Err(_) => {
            log::debug!("Timeout waiting for data from {}", peer);
            return Ok(false);
        }
    };

    log::debug!("Received {} bytes: {:02X?}", n, &buf[..n]);

    // 2) Parse handshake packet (packet ID = 0, next_state = 2)
    // More information on the handshake packet structure: https://minecraft.wiki/w/Java_Edition_protocol/Packets#Handshaking
    // Skip packet length VarInt
    let (_pkt_len, off1) = match read_varint(&buf[..n]) {
        Some(v) => v,
        None => return Ok(false),
    };
    // Packet ID VarInt
    let (pkt_id, off2) = match read_varint(&buf[off1..n]) {
        Some(v) => v,
        None => return Ok(false),
    };
    if pkt_id != 0 {
        // not a handshake packet
        return Ok(false);
    }

    // Skip protocol version VarInt
    let mut offset = off1 + off2;
    let (_protocol_version, len) = match read_varint(&buf[offset..n]) {
        Some(v) => v,
        None => return Ok(false),
    };
    offset += len;

    // Read address length and skip the address string
    let (addr_len, len) = match read_varint(&buf[offset..n]) {
        Some(v) => v,
        None => return Ok(false),
    };
    if addr_len < 0 {
        return Ok(false);
    }
    offset += len + addr_len as usize;

    // Skip the port (2 bytes)
    offset += 2;

    // Read next_state (intent) VarInt
    if offset >= n {
        return Ok(false);
    }
    if let Some((next_state, _)) = read_varint(&buf[offset..n]) {
        if next_state == 1 {
            // Status ping
            handle_status_ping(socket, &packets).await?;
            return Ok(false);
        } else if next_state == 2 {
            // Login handshake
            log::info!("Login handshake detected from {}", peer);
            return Ok(true);
        } else {
            log::debug!("Unknown type of ping from {}, ignoring", peer);
        }
    }

    Ok(false)
}

/// Launches the Minecraft server process with given command.
/// On Windows, opens the batch/script in a new terminal window so logs stay visible
pub fn launch_server(command: &str, args: &[&str]) -> Result<tokio::process::Child> {
    #[cfg(target_os = "windows")]
    {
        let mut cmd = tokio::process::Command::new("cmd");
        cmd.args(&["/C", "start", "", "/WAIT", command]);
        for &arg in args {
            cmd.arg(arg);
        }
        let child = cmd.spawn()?;
        log::info!("Launched server in new window: {} {:?}", command, args);
        Ok(child)
    }
    #[cfg(not(target_os = "windows"))]
    {
        let child = tokio::process::Command::new(command).args(args).spawn()?;
        log::info!("Launched server: {} {:?}", command, args);
        Ok(child)
    }
}

pub async fn kill_server_process(process: tokio::process::Child) {
    #[cfg(target_os = "windows")]
    {
        let pid = process.id().unwrap();
        let _ = std::process::Command::new("taskkill")
            .args(&["/F", "/T", "/PID", &pid.to_string()])
            .output();
    }

    #[cfg(not(target_os = "windows"))]
    {
        if let Err(e) = process.kill().await {
            log::error!("Failed to kill starting server: {}", e);
        }
    }
}

/// Idle watchdog: polls the RCON `list` command every `poll_interval`.
/// If no players have been online for `timeout`, send `/stop` via RCON and exit
pub async fn idle_watchdog_rcon(
    rcon_addr: &str,
    rcon_pass: &str,
    poll_interval: Duration,
    timeout: Duration,
    ready_signal_sender: tokio::sync::oneshot::Sender<()>,
) -> Result<()> {
    log::info!(
        "Starting RCON idle watchdog: polling {} every {:?}",
        rcon_addr,
        poll_interval
    );
    let start = Instant::now();

    // Wait for RCON to become available
    let conn = loop {
        match Connection::<TcpStream>::connect(rcon_addr, rcon_pass).await {
            Ok(c) => break c,
            Err(err) if start.elapsed() <= Duration::from_secs(600) => {
                log::warn!("RCON connection failed ({}), retrying...", err);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(err) => {
                return Err(err.into());
            }
        }
    };

    let mut conn = conn;
    log::info!("Successfully connected to RCON at {}", rcon_addr);
    ready_signal_sender
        .send(())
        .expect("Failed to send RCON ready signal!");

    // Polling loop
    let mut ticker = interval(poll_interval);
    let mut last_online = Instant::now();
    let mut consecutive_errors = 0;

    loop {
        ticker.tick().await;
        let response = loop {
            match conn.cmd("list").await {
                Ok(r) => {
                    consecutive_errors = 0;
                    break r;
                }
                Err(e) if consecutive_errors < 5 => {
                    consecutive_errors += 1;
                    log::warn!(
                        "RCON `list` poll failed: {} \nRetrying... ({}/5)",
                        e,
                        consecutive_errors
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
                Err(e) => {
                    log::error!("RCON connection error: {}. Stopping RCON watchdog.", e);
                    return Err(e.into());
                }
            };
        };
        log::info!("RCON list response: {}", response);

        let count = PLAYER_COUNT_RE
            .captures(&response)
            .and_then(|caps| caps.get(1))
            .and_then(|m| m.as_str().parse::<u32>().ok())
            .unwrap_or(0);

        if count > 0 {
            last_online = Instant::now();
        } else if last_online.elapsed() >= timeout {
            log::info!("No players for {:?}, stopping server...", timeout);
            let _ = conn.cmd("stop").await;
            break;
        }
    }
    Ok(())
}

/// Sends a single `/stop` command to the server via RCON and exits
pub async fn send_stop_command(rcon_addr: &str, rcon_pass: &str) -> Result<()> {
    log::info!(
        "Connecting to RCON at {} to send stop command...",
        rcon_addr
    );
    let mut conn = Connection::<TcpStream>::connect(rcon_addr, rcon_pass).await?;
    let _ = conn.cmd("stop").await?;
    log::info!("Stop command sent.");
    Ok(())
}

pub async fn send_starting_message(
    mut socket: TcpStream,
    packets: &PreserializedPackets,
) -> Result<()> {
    match tokio::time::timeout(
        std::time::Duration::from_secs(5),
        socket.write_all(&packets.starting_message_packet),
    )
    .await
    {
        Ok(Ok(())) => (),
        Ok(Err(e)) => log::warn!("Sending starting message to client failed: {:?}", e),
        Err(_) => log::warn!("Sending starting message to client timed out"),
    }

    // Wait a short moment to let client consume data (required because otherwise client doesn't display json message)
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    socket.shutdown().await?;
    Ok(())
}

async fn handle_status_ping(socket: &mut TcpStream, packets: &PreserializedPackets) -> Result<()> {
    // Read and discard the next packet (packet ID 0, status request)
    let mut buf = [0u8; 512];
    match tokio::time::timeout(std::time::Duration::from_secs(5), socket.read(&mut buf)).await {
        Ok(_) => (),
        Err(_) => log::warn!("Reading TcpStream timed out(handle_status_ping)"),
    }

    // Send to client
    match tokio::time::timeout(
        std::time::Duration::from_secs(5),
        socket.write_all(&packets.motd_packet),
    )
    .await
    {
        Ok(Ok(())) => (),
        Ok(Err(e)) => log::warn!("Sending MOTD to client failed: {:?}", e),
        Err(_) => log::warn!("Sending MOTD to client timed out"),
    }
    socket.shutdown().await?;
    Ok(())
}
