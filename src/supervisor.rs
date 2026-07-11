use std::process::ExitStatus;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::process::Child;
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, sleep_until, timeout};

use crate::process;
use crate::rcon::RconClient;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServerPhase {
    Stopped,
    Starting,
    Running,
    Stopping,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SupervisorCommand {
    Start,
    Shutdown,
}

pub struct SupervisorConfig {
    pub command: String,
    pub arguments: Vec<String>,
    pub rcon_address: String,
    pub rcon_password: String,
    pub poll_interval: Duration,
    pub idle_timeout: Duration,
    pub startup_timeout: Duration,
    pub retry_interval: Duration,
    pub command_timeout: Duration,
    pub shutdown_timeout: Duration,
}

enum CycleOutcome {
    Stopped,
    Shutdown,
}

pub async fn run(
    mut commands: mpsc::Receiver<SupervisorCommand>,
    phase: watch::Sender<ServerPhase>,
    config: SupervisorConfig,
) -> Result<()> {
    phase.send_replace(ServerPhase::Stopped);

    while let Some(command) = commands.recv().await {
        match command {
            SupervisorCommand::Start => {
                let child = match process::launch(&config.command, &config.arguments) {
                    Ok(child) => child,
                    Err(error) => {
                        log::error!("Unable to start the Minecraft server: {error:#}");
                        continue;
                    }
                };
                phase.send_replace(ServerPhase::Starting);

                let outcome = run_server_cycle(child, &mut commands, &phase, &config).await?;
                phase.send_replace(ServerPhase::Stopped);
                if matches!(outcome, CycleOutcome::Shutdown) {
                    return Ok(());
                }
                log::info!("Minecraft server is stopped and ready to nap");
            }
            SupervisorCommand::Shutdown => return Ok(()),
        }
    }

    Ok(())
}

async fn run_server_cycle(
    mut child: Child,
    commands: &mut mpsc::Receiver<SupervisorCommand>,
    phase: &watch::Sender<ServerPhase>,
    config: &SupervisorConfig,
) -> Result<CycleOutcome> {
    let deadline = Instant::now() + config.startup_timeout;
    let mut failed_attempts = 0u64;

    let rcon = loop {
        if Instant::now() >= deadline {
            log::error!(
                "RCON did not become ready within {:?}; terminating the server",
                config.startup_timeout
            );
            phase.send_replace(ServerPhase::Stopping);
            stop_and_reap(&mut child, None, config.shutdown_timeout).await?;
            return Ok(CycleOutcome::Stopped);
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        let connect_timeout = config.command_timeout.min(remaining);
        let connect = timeout(
            connect_timeout,
            RconClient::connect(&config.rcon_address, &config.rcon_password),
        );

        tokio::select! {
            status = child.wait() => {
                log_process_exit(status.context("failed to wait for the server process")?);
                return Ok(CycleOutcome::Stopped);
            }
            command = commands.recv() => {
                match command {
                    Some(SupervisorCommand::Start) => {
                        log::debug!("Ignoring duplicate start request during startup");
                        continue;
                    }
                    Some(SupervisorCommand::Shutdown) | None => {
                        phase.send_replace(ServerPhase::Stopping);
                        stop_and_reap(&mut child, None, config.shutdown_timeout).await?;
                        return Ok(CycleOutcome::Shutdown);
                    }
                }
            }
            result = connect => {
                match result {
                    Ok(Ok(client)) => break client,
                    Ok(Err(error)) => {
                        failed_attempts += 1;
                        if failed_attempts == 1 || failed_attempts.is_multiple_of(30) {
                            log::warn!("RCON is not ready yet: {error:#}");
                        } else {
                            log::debug!("RCON is not ready yet: {error:#}");
                        }
                    }
                    Err(_) => {
                        failed_attempts += 1;
                        log::debug!("RCON connection attempt timed out");
                    }
                }
            }
        }

        let retry_at = (Instant::now() + config.retry_interval).min(deadline);
        tokio::select! {
            status = child.wait() => {
                log_process_exit(status.context("failed to wait for the server process")?);
                return Ok(CycleOutcome::Stopped);
            }
            command = commands.recv() => {
                match command {
                    Some(SupervisorCommand::Start) => {
                        log::debug!("Ignoring duplicate start request during startup");
                    }
                    Some(SupervisorCommand::Shutdown) | None => {
                        phase.send_replace(ServerPhase::Stopping);
                        stop_and_reap(&mut child, None, config.shutdown_timeout).await?;
                        return Ok(CycleOutcome::Shutdown);
                    }
                }
            }
            () = sleep_until(retry_at) => {}
        }
    };

    log::info!("RCON is ready at {}", config.rcon_address);
    phase.send_replace(ServerPhase::Running);
    monitor_running_server(child, rcon, commands, phase, config).await
}

async fn monitor_running_server(
    mut child: Child,
    initial_rcon: RconClient,
    commands: &mut mpsc::Receiver<SupervisorCommand>,
    phase: &watch::Sender<ServerPhase>,
    config: &SupervisorConfig,
) -> Result<CycleOutcome> {
    let mut rcon = Some(initial_rcon);
    let mut next_poll = Instant::now();
    let mut empty_since: Option<Instant> = None;

    loop {
        tokio::select! {
            status = child.wait() => {
                log_process_exit(status.context("failed to wait for the server process")?);
                return Ok(CycleOutcome::Stopped);
            }
            command = commands.recv() => {
                match command {
                    Some(SupervisorCommand::Start) => {
                        log::debug!("Ignoring start request because the server is already running");
                    }
                    Some(SupervisorCommand::Shutdown) | None => {
                        phase.send_replace(ServerPhase::Stopping);
                        stop_and_reap(&mut child, rcon.as_mut(), config.shutdown_timeout).await?;
                        return Ok(CycleOutcome::Shutdown);
                    }
                }
            }
            () = sleep_until(next_poll) => {
                let Some(client) = rcon.as_mut() else {
                    match timeout(
                        config.command_timeout,
                        RconClient::connect(&config.rcon_address, &config.rcon_password),
                    ).await {
                        Ok(Ok(client)) => {
                            log::info!("Reconnected to RCON");
                            rcon = Some(client);
                            next_poll = Instant::now();
                        }
                        Ok(Err(error)) => {
                            log::warn!("RCON reconnection failed: {error:#}");
                            next_poll = Instant::now() + config.retry_interval;
                        }
                        Err(_) => {
                            log::warn!("RCON reconnection timed out");
                            next_poll = Instant::now() + config.retry_interval;
                        }
                    }
                    continue;
                };
                let poll = timeout(config.command_timeout, client.command("list")).await;

                let response = match poll {
                    Ok(Ok(response)) => response,
                    Ok(Err(error)) => {
                        log::warn!("RCON player poll failed; idle shutdown is suspended: {error:#}");
                        rcon = None;
                        empty_since = None;
                        next_poll = Instant::now() + config.retry_interval;
                        continue;
                    }
                    Err(_) => {
                        log::warn!("RCON player poll timed out; idle shutdown is suspended");
                        rcon = None;
                        empty_since = None;
                        next_poll = Instant::now() + config.retry_interval;
                        continue;
                    }
                };

                log::debug!("RCON list response: {response}");
                let player_count = match parse_player_count(&response) {
                    Ok(count) => count,
                    Err(error) => {
                        // Unknown is never treated as zero: false idle shutdowns are worse than
                        // leaving a server running until the next valid observation.
                        log::warn!("Could not parse RCON player count; idle shutdown is suspended: {error:#}");
                        empty_since = None;
                        next_poll = Instant::now() + config.poll_interval;
                        continue;
                    }
                };

                if player_count == 0 {
                    let started = *empty_since.get_or_insert_with(Instant::now);
                    let idle_for = Instant::now().saturating_duration_since(started);
                    if idle_for >= config.idle_timeout {
                        log::info!("Server has been confirmed empty for {idle_for:?}; stopping it");
                        phase.send_replace(ServerPhase::Stopping);
                        stop_and_reap(&mut child, rcon.as_mut(), config.shutdown_timeout).await?;
                        return Ok(CycleOutcome::Stopped);
                    }
                    log::debug!(
                        "Server is empty; idle shutdown in {:?}",
                        config.idle_timeout.saturating_sub(idle_for)
                    );
                } else {
                    empty_since = None;
                    log::debug!("Server has {player_count} online player(s)");
                }
                next_poll = Instant::now() + config.poll_interval;
            }
        }
    }
}

async fn stop_and_reap(
    child: &mut Child,
    rcon: Option<&mut RconClient>,
    shutdown_timeout: Duration,
) -> Result<()> {
    let graceful_stop_sent = if let Some(rcon) = rcon {
        match timeout(shutdown_timeout.min(Duration::from_secs(5)), rcon.stop()).await {
            Ok(Ok(())) => true,
            Ok(Err(error)) => {
                log::warn!("Failed to send the RCON stop command: {error:#}");
                false
            }
            Err(_) => {
                log::warn!("Sending the RCON stop command timed out");
                false
            }
        }
    } else {
        false
    };

    if graceful_stop_sent {
        match timeout(shutdown_timeout, child.wait()).await {
            Ok(Ok(status)) => {
                log_process_exit(status);
                return Ok(());
            }
            Ok(Err(error)) => log::warn!("Failed while waiting for server shutdown: {error}"),
            Err(_) => log::warn!("Server did not exit within {shutdown_timeout:?}; terminating it"),
        }
    }

    process::terminate(child).await?;
    match timeout(Duration::from_secs(5), child.wait()).await {
        Ok(Ok(status)) => log_process_exit(status),
        Ok(Err(error)) => return Err(error).context("failed to reap the server process"),
        Err(_) => {
            return Err(anyhow::anyhow!(
                "server process did not exit after termination"
            ));
        }
    }
    Ok(())
}

pub fn parse_player_count(response: &str) -> Result<u32> {
    let response = response
        .strip_prefix("There are ")
        .context("RCON response does not start with the player-count prefix")?;
    let (player_count, response) = response
        .split_once(" of a max of ")
        .context("RCON player count has an unknown format")?;
    let (max_players, _) = response
        .split_once(" players online:")
        .context("RCON player count has an unknown format")?;
    max_players
        .parse::<u32>()
        .context("RCON maximum player count is invalid")?;
    player_count
        .parse()
        .context("RCON player count is outside the supported range")
}

fn log_process_exit(status: ExitStatus) {
    if status.success() {
        log::info!("Minecraft server exited successfully");
    } else {
        log::warn!("Minecraft server exited with status {status}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_vanilla_list_responses() {
        assert_eq!(
            parse_player_count("There are 0 of a max of 20 players online:").unwrap(),
            0
        );
        assert_eq!(
            parse_player_count("There are 12 of a max of 100 players online: a, b").unwrap(),
            12
        );
    }

    #[test]
    fn never_interprets_unknown_output_as_empty() {
        assert!(parse_player_count("Unknown command").is_err());
        assert!(parse_player_count("There are many players online").is_err());
        assert!(parse_player_count("There are 0 unexpected").is_err());
        assert!(parse_player_count("There are 0 of a max of many players online:").is_err());
        assert!(parse_player_count("There are 0 of a max of 20 players:").is_err());
        assert!(parse_player_count("prefix: There are 0 of a max of 20 players online:").is_err());
    }
}
