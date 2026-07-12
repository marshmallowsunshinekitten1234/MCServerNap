use std::process::ExitStatus;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::process::Child;
use tokio::sync::oneshot::error::TryRecvError;
use tokio::sync::{mpsc, oneshot, watch};
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
pub(crate) struct LifecycleState {
    phase: ServerPhase,
    launch_generation: u64,
}

impl LifecycleState {
    pub(crate) const fn stopped() -> Self {
        Self {
            phase: ServerPhase::Stopped,
            launch_generation: 0,
        }
    }

    pub(crate) const fn phase(self) -> ServerPhase {
        self.phase
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WakeRequest {
    observed: LifecycleState,
}

impl WakeRequest {
    pub(crate) const fn observed(lifecycle: LifecycleState) -> Self {
        Self {
            observed: lifecycle,
        }
    }

    fn can_start(self, current: LifecycleState) -> bool {
        current.phase == ServerPhase::Stopped
            && self.observed.launch_generation == current.launch_generation
            && matches!(
                self.observed.phase,
                ServerPhase::Stopped | ServerPhase::Stopping
            )
    }

    fn requests_restart(self, current: LifecycleState) -> bool {
        current.phase == ServerPhase::Stopping && self.observed == current
    }
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

#[derive(Debug, Eq, PartialEq)]
enum CycleOutcome {
    Stopped,
    Restart,
    Shutdown,
}

struct SupervisorControl {
    wake_requests: mpsc::Receiver<WakeRequest>,
    shutdown: oneshot::Receiver<()>,
    lifecycle: watch::Sender<LifecycleState>,
}

impl SupervisorControl {
    async fn wait_for_wake(&mut self) -> bool {
        loop {
            // Closure is terminal even when the one-slot wake latch is still occupied.
            if self.wake_requests.is_closed() {
                return false;
            }

            tokio::select! {
                biased;

                _ = &mut self.shutdown => return false,
                request = self.wake_requests.recv() => {
                    let Some(request) = request else {
                        return false;
                    };
                    if request.can_start(self.current_lifecycle()) {
                        return true;
                    }
                    log::debug!("Ignoring stale server wake-up request");
                }
            }
        }
    }

    fn shutdown_pending(&mut self) -> bool {
        if self.wake_requests.is_closed() {
            return true;
        }
        !matches!(self.shutdown.try_recv(), Err(TryRecvError::Empty))
    }

    fn current_lifecycle(&self) -> LifecycleState {
        *self.lifecycle.borrow()
    }

    fn finish_launch_attempt(&self, phase: ServerPhase) {
        let current = self.current_lifecycle();
        // Every request that observed the pre-launch phase belongs to the same coalesced burst.
        self.lifecycle.send_replace(LifecycleState {
            phase,
            launch_generation: current.launch_generation.wrapping_add(1),
        });
    }

    fn set_phase(&self, phase: ServerPhase) {
        let current = self.current_lifecycle();
        self.lifecycle
            .send_replace(LifecycleState { phase, ..current });
    }
}

pub(crate) async fn run(
    wake_requests: mpsc::Receiver<WakeRequest>,
    shutdown: oneshot::Receiver<()>,
    lifecycle: watch::Sender<LifecycleState>,
    config: SupervisorConfig,
) -> Result<()> {
    let mut control = SupervisorControl {
        wake_requests,
        shutdown,
        lifecycle,
    };
    control.lifecycle.send_replace(LifecycleState::stopped());
    let mut restart = false;

    loop {
        if !restart && !control.wait_for_wake().await {
            return Ok(());
        }
        restart = false;

        if control.shutdown_pending() {
            return Ok(());
        }

        let child = match process::launch(&config.command, &config.arguments) {
            Ok(child) => {
                control.finish_launch_attempt(ServerPhase::Starting);
                child
            }
            Err(error) => {
                control.finish_launch_attempt(ServerPhase::Stopped);
                log::error!("Unable to start the Minecraft server: {error:#}");
                continue;
            }
        };

        let outcome = run_server_cycle(child, &mut control, &config).await?;
        control.set_phase(ServerPhase::Stopped);
        match outcome {
            CycleOutcome::Stopped => {
                log::info!("Minecraft server is stopped and ready to nap");
            }
            CycleOutcome::Restart => restart = true,
            CycleOutcome::Shutdown => return Ok(()),
        }
    }
}

async fn run_server_cycle(
    mut child: Child,
    control: &mut SupervisorControl,
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
            return stop_server(child, None, control, config.shutdown_timeout).await;
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        let connect_timeout = config.command_timeout.min(remaining);
        let connect = timeout(
            connect_timeout,
            RconClient::connect(&config.rcon_address, &config.rcon_password),
        );

        tokio::select! {
            biased;

            _ = &mut control.shutdown => {
                return stop_for_shutdown(child, None, control, config.shutdown_timeout).await;
            }
            request = control.wake_requests.recv() => {
                if request.is_none() {
                    return stop_for_shutdown(child, None, control, config.shutdown_timeout).await;
                }
                log::debug!("Ignoring duplicate start request during startup");
                continue;
            }
            status = child.wait() => {
                if let Err(error) = log_process_wait(status) {
                    return reap_after_wait_error(
                        child,
                        None,
                        control,
                        config.shutdown_timeout,
                        error,
                    ).await;
                }
                return Ok(CycleOutcome::Stopped);
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
            biased;

            _ = &mut control.shutdown => {
                return stop_for_shutdown(child, None, control, config.shutdown_timeout).await;
            }
            request = control.wake_requests.recv() => {
                if request.is_none() {
                    return stop_for_shutdown(child, None, control, config.shutdown_timeout).await;
                }
                log::debug!("Ignoring duplicate start request during startup");
            }
            status = child.wait() => {
                if let Err(error) = log_process_wait(status) {
                    return reap_after_wait_error(
                        child,
                        None,
                        control,
                        config.shutdown_timeout,
                        error,
                    ).await;
                }
                return Ok(CycleOutcome::Stopped);
            }
            () = sleep_until(retry_at) => {}
        }
    };

    if control.shutdown_pending() {
        return stop_for_shutdown(child, Some(rcon), control, config.shutdown_timeout).await;
    }
    log::info!("RCON is ready at {}", config.rcon_address);
    control.set_phase(ServerPhase::Running);
    monitor_running_server(child, rcon, control, config).await
}

async fn monitor_running_server(
    mut child: Child,
    initial_rcon: RconClient,
    control: &mut SupervisorControl,
    config: &SupervisorConfig,
) -> Result<CycleOutcome> {
    let mut rcon = Some(initial_rcon);
    let mut next_poll = Instant::now();
    let mut empty_since: Option<Instant> = None;

    loop {
        tokio::select! {
            biased;

            _ = &mut control.shutdown => {
                return stop_for_shutdown(child, rcon, control, config.shutdown_timeout).await;
            }
            request = control.wake_requests.recv() => {
                if request.is_none() {
                    return stop_for_shutdown(child, rcon, control, config.shutdown_timeout).await;
                }
                log::debug!("Ignoring start request because the server is already running");
            }
            status = child.wait() => {
                if let Err(error) = log_process_wait(status) {
                    return reap_after_wait_error(
                        child,
                        rcon,
                        control,
                        config.shutdown_timeout,
                        error,
                    ).await;
                }
                return Ok(CycleOutcome::Stopped);
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

                let Some(player_count) = observed_player_count(&response) else {
                    empty_since = None;
                    next_poll = Instant::now() + config.poll_interval;
                    continue;
                };

                if player_count == 0 {
                    let started = *empty_since.get_or_insert_with(Instant::now);
                    let idle_for = Instant::now().saturating_duration_since(started);
                    if idle_for >= config.idle_timeout {
                        log::info!("Server has been confirmed empty for {idle_for:?}; stopping it");
                        return stop_server(
                            child,
                            rcon,
                            control,
                            config.shutdown_timeout,
                        ).await;
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

async fn reap_after_wait_error(
    mut child: Child,
    mut rcon: Option<RconClient>,
    control: &mut SupervisorControl,
    shutdown_timeout: Duration,
    wait_error: std::io::Error,
) -> Result<CycleOutcome> {
    control.set_phase(ServerPhase::Stopping);
    stop_and_reap(&mut child, rcon.as_mut(), shutdown_timeout)
        .await
        .with_context(|| format!("cleanup failed after server process wait error: {wait_error}"))?;
    control.set_phase(ServerPhase::Stopped);
    Err(wait_error).context("failed to wait for the server process")
}

async fn stop_for_shutdown(
    mut child: Child,
    mut rcon: Option<RconClient>,
    control: &mut SupervisorControl,
    shutdown_timeout: Duration,
) -> Result<CycleOutcome> {
    control.set_phase(ServerPhase::Stopping);
    stop_and_reap(&mut child, rcon.as_mut(), shutdown_timeout).await?;
    Ok(CycleOutcome::Shutdown)
}

async fn stop_server(
    mut child: Child,
    mut rcon: Option<RconClient>,
    control: &mut SupervisorControl,
    shutdown_timeout: Duration,
) -> Result<CycleOutcome> {
    control.set_phase(ServerPhase::Stopping);
    let cleanup = stop_and_reap(&mut child, rcon.as_mut(), shutdown_timeout);
    tokio::pin!(cleanup);

    let mut restart = false;
    let shutdown_received = loop {
        tokio::select! {
            biased;

            _ = &mut control.shutdown => break true,
            request = control.wake_requests.recv(), if !restart => {
                let Some(request) = request else {
                    break true;
                };
                if request.requests_restart(control.current_lifecycle()) {
                    restart = true;
                } else {
                    log::debug!("Ignoring stale server wake-up request while stopping");
                }
            }
            result = &mut cleanup => {
                result?;
                break false;
            }
        }
    };

    if shutdown_received {
        cleanup.await?;
        return Ok(CycleOutcome::Shutdown);
    }

    if control.shutdown_pending() {
        return Ok(CycleOutcome::Shutdown);
    }

    loop {
        match control.wake_requests.try_recv() {
            Ok(request) if request.requests_restart(control.current_lifecycle()) => restart = true,
            Ok(_) => log::debug!("Ignoring stale server wake-up request while stopping"),
            Err(mpsc::error::TryRecvError::Empty) => break,
            Err(mpsc::error::TryRecvError::Disconnected) => {
                return Ok(CycleOutcome::Shutdown);
            }
        }
    }

    Ok(if restart {
        CycleOutcome::Restart
    } else {
        CycleOutcome::Stopped
    })
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

fn observed_player_count(response: &str) -> Option<u32> {
    log::debug!("RCON list response: {response}");
    match parse_player_count(response) {
        Ok(count) => Some(count),
        Err(error) => {
            // Unknown is never treated as zero: false idle shutdowns are worse than leaving a
            // server running until the next valid observation.
            log::warn!("Could not parse RCON player count; idle shutdown is suspended: {error:#}");
            None
        }
    }
}

fn log_process_wait(status: std::io::Result<ExitStatus>) -> std::io::Result<()> {
    status.map(log_process_exit)
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
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::task::JoinHandle;

    use super::*;

    const TEST_AUTH: i32 = 3;
    const TEST_AUTH_RESPONSE: i32 = 2;
    const TEST_EXEC_COMMAND: i32 = 2;

    struct TestRconPacket {
        id: i32,
        kind: i32,
        body: Vec<u8>,
    }

    async fn read_test_rcon_packet(stream: &mut TcpStream) -> TestRconPacket {
        let length = stream
            .read_i32_le()
            .await
            .expect("test RCON packet length should arrive");
        let mut payload =
            vec![0; usize::try_from(length).expect("test RCON packet length should be positive")];
        stream
            .read_exact(&mut payload)
            .await
            .expect("test RCON packet payload should arrive");
        assert!(payload.len() >= 10);
        assert_eq!(payload[payload.len() - 2..], [0, 0]);

        TestRconPacket {
            id: i32::from_le_bytes(payload[0..4].try_into().unwrap()),
            kind: i32::from_le_bytes(payload[4..8].try_into().unwrap()),
            body: payload[8..payload.len() - 2].to_vec(),
        }
    }

    async fn write_test_rcon_packet(stream: &mut TcpStream, id: i32, kind: i32, body: &[u8]) {
        stream
            .write_i32_le(i32::try_from(body.len() + 10).unwrap())
            .await
            .expect("test RCON packet length should be written");
        stream
            .write_i32_le(id)
            .await
            .expect("test RCON packet ID should be written");
        stream
            .write_i32_le(kind)
            .await
            .expect("test RCON packet kind should be written");
        stream
            .write_all(body)
            .await
            .expect("test RCON packet body should be written");
        stream
            .write_all(&[0, 0])
            .await
            .expect("test RCON packet terminators should be written");
    }

    async fn spawn_test_rcon_server() -> (String, oneshot::Receiver<()>, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test RCON listener should bind");
        let address = listener
            .local_addr()
            .expect("test RCON listener should have an address")
            .to_string();
        let (ready_sender, ready_receiver) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener
                .accept()
                .await
                .expect("supervisor should connect to test RCON");
            let auth = read_test_rcon_packet(&mut stream).await;
            assert_eq!(auth.kind, TEST_AUTH);
            assert_eq!(auth.body, b"secret");
            write_test_rcon_packet(&mut stream, auth.id, TEST_AUTH_RESPONSE, b"").await;

            ready_sender
                .send(())
                .expect("test should still await RCON readiness");
            let stop = read_test_rcon_packet(&mut stream).await;
            assert_eq!(stop.kind, TEST_EXEC_COMMAND);
            assert_eq!(stop.body, b"stop");
        });
        (address, ready_receiver, server)
    }

    fn test_config(rcon_address: String) -> SupervisorConfig {
        let executable = std::env::current_exe().expect("test executable path should be known");
        SupervisorConfig {
            command: executable.to_string_lossy().into_owned(),
            arguments: vec![
                "--ignored".to_owned(),
                "--exact".to_owned(),
                "supervisor::tests::server_process_fixture".to_owned(),
                "--quiet".to_owned(),
            ],
            rcon_address,
            rcon_password: "secret".to_owned(),
            poll_interval: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(30),
            startup_timeout: Duration::from_secs(30),
            retry_interval: Duration::from_millis(10),
            command_timeout: Duration::from_secs(1),
            shutdown_timeout: Duration::from_millis(250),
        }
    }

    async fn wait_for_phase(
        lifecycle: &mut watch::Receiver<LifecycleState>,
        expected: ServerPhase,
    ) {
        timeout(
            Duration::from_secs(5),
            lifecycle.wait_for(|state| state.phase == expected),
        )
        .await
        .expect("supervisor phase transition should be bounded")
        .expect("supervisor should keep the lifecycle source open");
    }

    #[test]
    #[ignore = "launched as the supervisor's test server process"]
    fn server_process_fixture() {
        std::thread::sleep(Duration::from_secs(30));
    }

    #[test]
    fn starting_and_running_wakes_cannot_start_a_later_cycle() {
        let stopped = LifecycleState {
            phase: ServerPhase::Stopped,
            launch_generation: 1,
        };

        for phase in [ServerPhase::Starting, ServerPhase::Running] {
            let request = WakeRequest::observed(LifecycleState {
                phase,
                launch_generation: 1,
            });
            assert!(!request.can_start(stopped));
        }
    }

    #[tokio::test]
    async fn closed_shutdown_source_precedes_queued_wake_while_stopped() {
        let (wake_sender, wake_receiver) = mpsc::channel(1);
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let (lifecycle_sender, lifecycle) = watch::channel(LifecycleState::stopped());
        wake_sender
            .try_send(WakeRequest::observed(*lifecycle.borrow()))
            .expect("first wake request should fit");
        drop(shutdown_sender);

        run(
            wake_receiver,
            shutdown_receiver,
            lifecycle_sender,
            test_config("127.0.0.1:9".to_owned()),
        )
        .await
        .expect("supervisor shutdown should succeed");

        assert_eq!(lifecycle.borrow().phase, ServerPhase::Stopped);
        assert_eq!(lifecycle.borrow().launch_generation, 0);
    }

    #[tokio::test]
    async fn closed_wake_source_stops_and_reaps_an_active_child() {
        let (wake_sender, wake_receiver) = mpsc::channel(1);
        let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
        let (lifecycle_sender, mut lifecycle) = watch::channel(LifecycleState::stopped());
        let supervisor = tokio::spawn(run(
            wake_receiver,
            shutdown_receiver,
            lifecycle_sender,
            test_config("127.0.0.1:9".to_owned()),
        ));

        wake_sender
            .send(WakeRequest::observed(*lifecycle.borrow()))
            .await
            .expect("supervisor should accept the initial wake request");
        wait_for_phase(&mut lifecycle, ServerPhase::Starting).await;
        drop(wake_sender);

        timeout(Duration::from_secs(10), supervisor)
            .await
            .expect("supervisor cleanup should be bounded")
            .expect("supervisor task should not panic")
            .expect("supervisor cleanup should succeed");
        assert_eq!(lifecycle.borrow().phase, ServerPhase::Stopped);
        assert_eq!(lifecycle.borrow().launch_generation, 1);
    }

    #[tokio::test]
    async fn stopped_and_starting_wake_bursts_launch_only_one_child() {
        let (wake_sender, wake_receiver) = mpsc::channel(1);
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let (lifecycle_sender, mut lifecycle) = watch::channel(LifecycleState::stopped());
        let initial = *lifecycle.borrow();
        wake_sender
            .try_send(WakeRequest::observed(initial))
            .expect("first wake request should fit");
        for _ in 0..32 {
            assert!(
                matches!(
                    wake_sender.try_send(WakeRequest::observed(initial)),
                    Err(mpsc::error::TrySendError::Full(_))
                ),
                "a stopped-phase wake burst should occupy one pending slot"
            );
        }
        let supervisor = tokio::spawn(run(
            wake_receiver,
            shutdown_receiver,
            lifecycle_sender,
            test_config("127.0.0.1:9".to_owned()),
        ));

        wait_for_phase(&mut lifecycle, ServerPhase::Starting).await;
        let starting = *lifecycle.borrow();
        for _ in 0..16 {
            wake_sender
                .send(WakeRequest::observed(starting))
                .await
                .expect("supervisor should drain startup wake requests");
        }
        shutdown_sender
            .send(())
            .expect("supervisor should still await shutdown");

        timeout(Duration::from_secs(10), supervisor)
            .await
            .expect("supervisor cleanup should be bounded")
            .expect("supervisor task should not panic")
            .expect("supervisor cleanup should succeed");
        assert_eq!(lifecycle.borrow().phase, ServerPhase::Stopped);
        assert_eq!(lifecycle.borrow().launch_generation, 1);
    }

    #[tokio::test]
    async fn shutdown_precedes_a_running_phase_wake() {
        let (rcon_address, rcon_ready, rcon_server) = spawn_test_rcon_server().await;
        let rcon = RconClient::connect(&rcon_address, "secret")
            .await
            .expect("test RCON client should authenticate");
        rcon_ready
            .await
            .expect("test RCON server should authenticate the client");
        let config = test_config(rcon_address);
        let child = process::launch(&config.command, &config.arguments)
            .expect("test server process should launch");
        let (wake_sender, wake_receiver) = mpsc::channel(1);
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let running = LifecycleState {
            phase: ServerPhase::Running,
            launch_generation: 1,
        };
        let (lifecycle_sender, lifecycle) = watch::channel(running);
        wake_sender
            .try_send(WakeRequest::observed(running))
            .expect("running-phase wake request should fit");
        shutdown_sender
            .send(())
            .expect("running monitor should still await shutdown");
        let mut control = SupervisorControl {
            wake_requests: wake_receiver,
            shutdown: shutdown_receiver,
            lifecycle: lifecycle_sender,
        };

        let outcome = timeout(
            Duration::from_secs(10),
            monitor_running_server(child, rcon, &mut control, &config),
        )
        .await
        .expect("supervisor cleanup should be bounded")
        .expect("supervisor cleanup should succeed");
        assert_eq!(outcome, CycleOutcome::Shutdown);
        timeout(Duration::from_secs(2), rcon_server)
            .await
            .expect("test RCON server should finish")
            .expect("test RCON server should not panic");
        assert_eq!(lifecycle.borrow().phase, ServerPhase::Stopping);
        assert_eq!(lifecycle.borrow().launch_generation, 1);
    }

    #[tokio::test]
    async fn stopping_wake_flood_cannot_starve_cleanup() {
        let (rcon_address, rcon_ready, rcon_server) = spawn_test_rcon_server().await;
        let rcon = RconClient::connect(&rcon_address, "secret")
            .await
            .expect("test RCON client should authenticate");
        rcon_ready
            .await
            .expect("test RCON server should authenticate the client");
        let config = test_config(rcon_address);
        let child = process::launch(&config.command, &config.arguments)
            .expect("test server process should launch");
        let (wake_sender, wake_receiver) = mpsc::channel(1);
        let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
        let active = LifecycleState {
            phase: ServerPhase::Running,
            launch_generation: 1,
        };
        let (lifecycle_sender, mut lifecycle) = watch::channel(active);
        let stop = tokio::spawn(async move {
            let mut control = SupervisorControl {
                wake_requests: wake_receiver,
                shutdown: shutdown_receiver,
                lifecycle: lifecycle_sender,
            };
            stop_server(child, Some(rcon), &mut control, config.shutdown_timeout).await
        });

        wait_for_phase(&mut lifecycle, ServerPhase::Stopping).await;
        let stopping = *lifecycle.borrow();
        let flooders = (0..8)
            .map(|_| {
                let wake_sender = wake_sender.clone();
                tokio::spawn(async move {
                    let mut sent = 0usize;
                    while wake_sender
                        .send(WakeRequest::observed(stopping))
                        .await
                        .is_ok()
                    {
                        sent += 1;
                    }
                    sent
                })
            })
            .collect::<Vec<_>>();
        drop(wake_sender);

        let outcome = timeout(Duration::from_secs(10), stop)
            .await
            .expect("server cleanup should be bounded")
            .expect("stop task should not panic")
            .expect("server cleanup should succeed");
        assert_eq!(outcome, CycleOutcome::Restart);
        let mut sent = 0;
        for flooder in flooders {
            sent += flooder.await.expect("wake flood task should not panic");
        }
        assert!(
            sent >= 2,
            "wake traffic should continue after restart latches"
        );
        timeout(Duration::from_secs(2), rcon_server)
            .await
            .expect("test RCON server should finish")
            .expect("test RCON server should not panic");
    }

    #[tokio::test]
    async fn shutdown_overrides_a_stopping_phase_restart() {
        let (rcon_address, rcon_ready, rcon_server) = spawn_test_rcon_server().await;
        let rcon = RconClient::connect(&rcon_address, "secret")
            .await
            .expect("test RCON client should authenticate");
        rcon_ready
            .await
            .expect("test RCON server should authenticate the client");
        let config = test_config(rcon_address);
        let child = process::launch(&config.command, &config.arguments)
            .expect("test server process should launch");
        let (wake_sender, wake_receiver) = mpsc::channel(1);
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let active = LifecycleState {
            phase: ServerPhase::Running,
            launch_generation: 1,
        };
        let (lifecycle_sender, mut lifecycle) = watch::channel(active);
        let stop = tokio::spawn(async move {
            let mut control = SupervisorControl {
                wake_requests: wake_receiver,
                shutdown: shutdown_receiver,
                lifecycle: lifecycle_sender,
            };
            stop_server(child, Some(rcon), &mut control, config.shutdown_timeout).await
        });

        wait_for_phase(&mut lifecycle, ServerPhase::Stopping).await;
        wake_sender
            .send(WakeRequest::observed(*lifecycle.borrow()))
            .await
            .expect("supervisor should accept one stopping-phase wake request");
        shutdown_sender
            .send(())
            .expect("stop task should still await shutdown");

        let outcome = timeout(Duration::from_secs(10), stop)
            .await
            .expect("server cleanup should be bounded")
            .expect("stop task should not panic")
            .expect("server cleanup should succeed");
        assert_eq!(outcome, CycleOutcome::Shutdown);
        timeout(Duration::from_secs(2), rcon_server)
            .await
            .expect("test RCON server should finish")
            .expect("test RCON server should not panic");
    }

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
