use std::future::Future;
use std::process::ExitStatus;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::process::Child;
use tokio::sync::oneshot::error::TryRecvError;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{Instant, sleep_until, timeout};

use crate::process;
use crate::rcon::RconClient;

const STABILITY_WINDOW: Duration = Duration::from_secs(60);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServerPhase {
    Stopped,
    Starting,
    Running,
    Stopping,
    Cooldown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FailureCategory {
    LaunchFailed,
    StartupTimedOut,
    ExitedBeforeReady,
    ExitedUnexpectedly,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleState {
    phase: ServerPhase,
    launch_generation: u64,
    failure: Option<FailureCategory>,
    failure_streak: u32,
    phase_started_at: Instant,
    retry_at: Option<Instant>,
}

impl LifecycleState {
    pub(crate) fn stopped() -> Self {
        Self {
            phase: ServerPhase::Stopped,
            launch_generation: 0,
            failure: None,
            failure_streak: 0,
            phase_started_at: Instant::now(),
            retry_at: None,
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
                ServerPhase::Stopped | ServerPhase::Stopping | ServerPhase::Cooldown
            )
    }

    fn requests_restart(self, current: LifecycleState) -> bool {
        current.phase == ServerPhase::Stopping
            && current.failure.is_none()
            && self.observed == current
    }

    fn requests_retry(self, current: LifecycleState) -> bool {
        current.failure.is_some()
            && self.observed.failure == current.failure
            && self.observed.failure_streak == current.failure_streak
            && self.observed.launch_generation == current.launch_generation
            && matches!(
                self.observed.phase,
                ServerPhase::Stopping | ServerPhase::Cooldown
            )
            && matches!(current.phase, ServerPhase::Stopping | ServerPhase::Cooldown)
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
    Failed {
        retry_at: Instant,
        retry_pending: bool,
    },
    Shutdown,
}

enum ReadinessOutcome {
    Ready(RconClient),
    StartupTimedOut,
    Exited(ExitStatus),
    ProcessWaitFailed(std::io::Error),
    Shutdown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CooldownOutcome {
    Stopped,
    Launch,
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
            phase_started_at: Instant::now(),
            retry_at: None,
            ..current
        });
    }

    fn record_launch_failure(&self) -> Instant {
        let current = self.current_lifecycle();
        let failure_streak = current.failure_streak.saturating_add(1);
        let now = Instant::now();
        let retry_at = now + failure_backoff(failure_streak);
        self.lifecycle.send_replace(LifecycleState {
            phase: ServerPhase::Cooldown,
            launch_generation: current.launch_generation.wrapping_add(1),
            failure: Some(FailureCategory::LaunchFailed),
            failure_streak,
            phase_started_at: now,
            retry_at: Some(retry_at),
        });
        retry_at
    }

    fn record_reaped_failure(&self, failure: FailureCategory) -> Instant {
        let current = self.current_lifecycle();
        let failure_streak = current.failure_streak.saturating_add(1);
        let now = Instant::now();
        let retry_at = now + failure_backoff(failure_streak);
        self.lifecycle.send_replace(LifecycleState {
            phase: ServerPhase::Cooldown,
            failure: Some(failure),
            failure_streak,
            phase_started_at: now,
            retry_at: Some(retry_at),
            ..current
        });
        retry_at
    }

    fn begin_failure_cleanup(&self, failure: FailureCategory) -> Duration {
        let current = self.current_lifecycle();
        let failure_streak = current.failure_streak.saturating_add(1);
        self.lifecycle.send_replace(LifecycleState {
            phase: ServerPhase::Stopping,
            failure: Some(failure),
            failure_streak,
            phase_started_at: Instant::now(),
            retry_at: None,
            ..current
        });
        failure_backoff(failure_streak)
    }

    fn finish_failure_cleanup(&self, retry_delay: Duration) -> Instant {
        let current = self.current_lifecycle();
        let now = Instant::now();
        let retry_at = now + retry_delay;
        self.lifecycle.send_replace(LifecycleState {
            phase: ServerPhase::Cooldown,
            phase_started_at: now,
            retry_at: Some(retry_at),
            ..current
        });
        retry_at
    }

    fn reset_failure_streak(&self) {
        let current = self.current_lifecycle();
        self.lifecycle.send_replace(LifecycleState {
            failure: None,
            failure_streak: 0,
            retry_at: None,
            ..current
        });
    }

    fn set_phase(&self, phase: ServerPhase) {
        let current = self.current_lifecycle();
        self.lifecycle.send_replace(LifecycleState {
            phase,
            phase_started_at: Instant::now(),
            retry_at: None,
            ..current
        });
    }

    async fn wait_for_cooldown(
        &mut self,
        retry_at: Instant,
        mut retry_pending: bool,
    ) -> CooldownOutcome {
        loop {
            if self.shutdown_pending() {
                self.set_phase(ServerPhase::Stopped);
                return CooldownOutcome::Shutdown;
            }

            if Instant::now() >= retry_at {
                if retry_pending {
                    return CooldownOutcome::Launch;
                }
                self.set_phase(ServerPhase::Stopped);
                return CooldownOutcome::Stopped;
            }

            tokio::select! {
                biased;

                _ = &mut self.shutdown => {
                    self.set_phase(ServerPhase::Stopped);
                    return CooldownOutcome::Shutdown;
                }
                request = self.wake_requests.recv(), if !retry_pending => {
                    let Some(request) = request else {
                        self.set_phase(ServerPhase::Stopped);
                        return CooldownOutcome::Shutdown;
                    };
                    if request.requests_retry(self.current_lifecycle()) {
                        retry_pending = true;
                        log::info!("Latched one server retry for the end of the failure cooldown");
                    } else {
                        log::debug!("Ignoring stale server wake-up request during failure cooldown");
                    }
                }
                () = sleep_until(retry_at) => {}
            }
        }
    }
}

fn failure_backoff(failure_streak: u32) -> Duration {
    assert!(failure_streak > 0, "failure streak must be nonzero");
    Duration::from_secs(match failure_streak {
        1 => 5,
        2 => 10,
        3 => 20,
        4 => 40,
        _ => 60,
    })
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
    let mut launch_pending = false;

    loop {
        if !launch_pending && !control.wait_for_wake().await {
            control.set_phase(ServerPhase::Stopped);
            return Ok(());
        }
        launch_pending = false;

        if control.shutdown_pending() {
            control.set_phase(ServerPhase::Stopped);
            return Ok(());
        }

        let child = match process::launch(&config.command, &config.arguments) {
            Ok(child) => {
                control.finish_launch_attempt(ServerPhase::Starting);
                child
            }
            Err(error) => {
                log::error!("Unable to start the Minecraft server: {error:#}");
                let retry_at = control.record_launch_failure();
                match control.wait_for_cooldown(retry_at, false).await {
                    CooldownOutcome::Launch => launch_pending = true,
                    CooldownOutcome::Stopped => {}
                    CooldownOutcome::Shutdown => return Ok(()),
                }
                continue;
            }
        };

        let outcome = run_server_cycle(child, &mut control, &config).await?;
        match outcome {
            CycleOutcome::Stopped => {
                control.set_phase(ServerPhase::Stopped);
                log::info!("Minecraft server is stopped and ready to nap");
            }
            CycleOutcome::Restart => {
                control.set_phase(ServerPhase::Stopped);
                launch_pending = true;
            }
            CycleOutcome::Failed {
                retry_at,
                retry_pending,
            } => match control.wait_for_cooldown(retry_at, retry_pending).await {
                CooldownOutcome::Launch => launch_pending = true,
                CooldownOutcome::Stopped => {}
                CooldownOutcome::Shutdown => return Ok(()),
            },
            CycleOutcome::Shutdown => {
                control.set_phase(ServerPhase::Stopped);
                return Ok(());
            }
        }
    }
}

async fn run_server_cycle(
    mut child: Child,
    control: &mut SupervisorControl,
    config: &SupervisorConfig,
) -> Result<CycleOutcome> {
    let readiness = wait_for_readiness(&mut child, control, config).await;
    let rcon = match readiness {
        ReadinessOutcome::Ready(rcon) => rcon,
        ReadinessOutcome::StartupTimedOut => {
            log::error!(
                "RCON did not become ready within {:?}; terminating the server",
                config.startup_timeout
            );
            return stop_failed_server(
                child,
                None,
                control,
                config.shutdown_timeout,
                FailureCategory::StartupTimedOut,
            )
            .await;
        }
        ReadinessOutcome::Exited(status) => {
            return Ok(reaped_failure(
                control,
                FailureCategory::ExitedBeforeReady,
                status,
            ));
        }
        ReadinessOutcome::ProcessWaitFailed(error) => {
            return reap_after_wait_error(child, None, control, config.shutdown_timeout, error)
                .await;
        }
        ReadinessOutcome::Shutdown => {
            return stop_for_shutdown(child, None, control, config.shutdown_timeout).await;
        }
    };

    if control.shutdown_pending() {
        return stop_for_shutdown(child, Some(rcon), control, config.shutdown_timeout).await;
    }
    log::info!("RCON is ready at {}", config.rcon_address);
    control.set_phase(ServerPhase::Running);
    monitor_running_server(child, rcon, control, config).await
}

async fn wait_for_readiness(
    child: &mut Child,
    control: &mut SupervisorControl,
    config: &SupervisorConfig,
) -> ReadinessOutcome {
    let deadline = Instant::now() + config.startup_timeout;
    let mut failed_attempts = 0u64;

    loop {
        if Instant::now() >= deadline {
            return match child.try_wait() {
                Ok(Some(status)) => ReadinessOutcome::Exited(status),
                Ok(None) => ReadinessOutcome::StartupTimedOut,
                Err(error) => ReadinessOutcome::ProcessWaitFailed(error),
            };
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        let connect_timeout = config.command_timeout.min(remaining);
        let connect = timeout(
            connect_timeout,
            RconClient::connect(&config.rcon_address, &config.rcon_password),
        );

        tokio::select! {
            biased;

            _ = &mut control.shutdown => return ReadinessOutcome::Shutdown,
            request = control.wake_requests.recv() => {
                if request.is_none() {
                    return ReadinessOutcome::Shutdown;
                }
                log::debug!("Ignoring duplicate start request during startup");
                continue;
            }
            status = child.wait() => {
                return match status {
                    Ok(status) => ReadinessOutcome::Exited(status),
                    Err(error) => ReadinessOutcome::ProcessWaitFailed(error),
                };
            }
            result = connect => {
                match result {
                    Ok(Ok(client)) => return ReadinessOutcome::Ready(client),
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

            _ = &mut control.shutdown => return ReadinessOutcome::Shutdown,
            request = control.wake_requests.recv() => {
                if request.is_none() {
                    return ReadinessOutcome::Shutdown;
                }
                log::debug!("Ignoring duplicate start request during startup");
            }
            status = child.wait() => {
                return match status {
                    Ok(status) => ReadinessOutcome::Exited(status),
                    Err(error) => ReadinessOutcome::ProcessWaitFailed(error),
                };
            }
            () = sleep_until(retry_at) => {}
        }
    }
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
    let stable_at = control.current_lifecycle().phase_started_at + STABILITY_WINDOW;
    let mut stability_pending = control.current_lifecycle().failure_streak > 0;

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
                return process_wait_outcome(
                    status,
                    child,
                    rcon,
                    control,
                    config.shutdown_timeout,
                    FailureCategory::ExitedUnexpectedly,
                ).await;
            }
            () = sleep_until(stable_at), if stability_pending => {
                match child.try_wait() {
                    Ok(None) => {
                        control.reset_failure_streak();
                        stability_pending = false;
                        log::info!("Server has run continuously for {STABILITY_WINDOW:?}; reset the failure streak");
                    }
                    Ok(Some(status)) => {
                        return Ok(reaped_failure(
                            control,
                            FailureCategory::ExitedUnexpectedly,
                            status,
                        ));
                    }
                    Err(error) => {
                        return reap_after_wait_error(
                            child,
                            rcon,
                            control,
                            config.shutdown_timeout,
                            error,
                        ).await;
                    }
                }
            }
            () = sleep_until(next_poll) => {
                let Some(idle_for) = poll_confirmed_idle(
                    &mut rcon,
                    &mut empty_since,
                    &mut next_poll,
                    config,
                ).await else {
                    continue;
                };
                match try_reaped_failure(
                    &mut child,
                    control,
                    FailureCategory::ExitedUnexpectedly,
                ) {
                    Ok(Some(outcome)) => return Ok(outcome),
                    Ok(None) => {}
                    Err(error) => {
                        return reap_after_wait_error(
                            child,
                            rcon,
                            control,
                            config.shutdown_timeout,
                            error,
                        ).await;
                    }
                }
                log::info!("Server has been confirmed empty for {idle_for:?}; stopping it");
                return stop_server(child, rcon, control, config.shutdown_timeout).await;
            }
        }
    }
}

async fn poll_confirmed_idle(
    rcon: &mut Option<RconClient>,
    empty_since: &mut Option<Instant>,
    next_poll: &mut Instant,
    config: &SupervisorConfig,
) -> Option<Duration> {
    let Some(client) = rcon.as_mut() else {
        match timeout(
            config.command_timeout,
            RconClient::connect(&config.rcon_address, &config.rcon_password),
        )
        .await
        {
            Ok(Ok(client)) => {
                log::info!("Reconnected to RCON");
                *rcon = Some(client);
                *next_poll = Instant::now();
            }
            Ok(Err(error)) => {
                log::warn!("RCON reconnection failed: {error:#}");
                *next_poll = Instant::now() + config.retry_interval;
            }
            Err(_) => {
                log::warn!("RCON reconnection timed out");
                *next_poll = Instant::now() + config.retry_interval;
            }
        }
        return None;
    };

    let response = match timeout(config.command_timeout, client.command("list")).await {
        Ok(Ok(response)) => response,
        Ok(Err(error)) => {
            log::warn!("RCON player poll failed; idle shutdown is suspended: {error:#}");
            *rcon = None;
            *empty_since = None;
            *next_poll = Instant::now() + config.retry_interval;
            return None;
        }
        Err(_) => {
            log::warn!("RCON player poll timed out; idle shutdown is suspended");
            *rcon = None;
            *empty_since = None;
            *next_poll = Instant::now() + config.retry_interval;
            return None;
        }
    };

    let Some(player_count) = observed_player_count(&response) else {
        *empty_since = None;
        *next_poll = Instant::now() + config.poll_interval;
        return None;
    };

    if player_count == 0 {
        let started = *empty_since.get_or_insert_with(Instant::now);
        let idle_for = Instant::now().saturating_duration_since(started);
        if idle_for >= config.idle_timeout {
            return Some(idle_for);
        }
        log::debug!(
            "Server is empty; idle shutdown in {:?}",
            config.idle_timeout.saturating_sub(idle_for)
        );
    } else {
        *empty_since = None;
        log::debug!("Server has {player_count} online player(s)");
    }
    *next_poll = Instant::now() + config.poll_interval;
    None
}

fn reaped_failure(
    control: &SupervisorControl,
    failure: FailureCategory,
    status: ExitStatus,
) -> CycleOutcome {
    log_process_exit(status);
    let retry_at = control.record_reaped_failure(failure);
    CycleOutcome::Failed {
        retry_at,
        retry_pending: false,
    }
}

fn try_reaped_failure(
    child: &mut Child,
    control: &SupervisorControl,
    failure: FailureCategory,
) -> std::io::Result<Option<CycleOutcome>> {
    let Some(status) = child.try_wait()? else {
        return Ok(None);
    };
    Ok(Some(reaped_failure(control, failure, status)))
}

async fn process_wait_outcome(
    status: std::io::Result<ExitStatus>,
    child: Child,
    rcon: Option<RconClient>,
    control: &mut SupervisorControl,
    shutdown_timeout: Duration,
    failure: FailureCategory,
) -> Result<CycleOutcome> {
    match status {
        Ok(status) => Ok(reaped_failure(control, failure, status)),
        Err(error) => reap_after_wait_error(child, rcon, control, shutdown_timeout, error).await,
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

async fn stop_failed_server(
    mut child: Child,
    mut rcon: Option<RconClient>,
    control: &mut SupervisorControl,
    shutdown_timeout: Duration,
    failure: FailureCategory,
) -> Result<CycleOutcome> {
    let retry_delay = control.begin_failure_cleanup(failure);
    let cleanup = stop_and_reap(&mut child, rcon.as_mut(), shutdown_timeout);
    finish_failed_cleanup(cleanup, control, retry_delay).await
}

async fn finish_failed_cleanup<F>(
    cleanup: F,
    control: &mut SupervisorControl,
    retry_delay: Duration,
) -> Result<CycleOutcome>
where
    F: Future<Output = Result<()>>,
{
    tokio::pin!(cleanup);
    let mut retry_pending = false;

    let shutdown_received = loop {
        tokio::select! {
            biased;

            _ = &mut control.shutdown => break true,
            request = control.wake_requests.recv(), if !retry_pending => {
                let Some(request) = request else {
                    break true;
                };
                if request.requests_retry(control.current_lifecycle()) {
                    retry_pending = true;
                    log::info!("Latched one server retry while settling the failed process");
                } else {
                    log::debug!("Ignoring stale server wake-up request during failed-process cleanup");
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

    let retry_at = control.finish_failure_cleanup(retry_delay);
    Ok(CycleOutcome::Failed {
        retry_at,
        retry_pending,
    })
}

async fn stop_server(
    mut child: Child,
    mut rcon: Option<RconClient>,
    control: &mut SupervisorControl,
    shutdown_timeout: Duration,
) -> Result<CycleOutcome> {
    control.reset_failure_streak();
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
    let mut wait_error = None;
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
            Ok(Err(error)) => {
                log::warn!("Failed while waiting for server shutdown: {error}");
                wait_error = Some(error);
            }
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
    if let Some(error) = wait_error {
        return Err(error).context("failed while waiting for graceful server shutdown");
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
    const TEST_RESPONSE_VALUE: i32 = 0;

    struct TestRconPacket {
        id: i32,
        kind: i32,
        body: Vec<u8>,
    }

    async fn try_read_test_rcon_packet(stream: &mut TcpStream) -> std::io::Result<TestRconPacket> {
        let length = stream.read_i32_le().await?;
        let length = usize::try_from(length).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "test RCON packet length is negative",
            )
        })?;
        let mut payload = vec![0; length];
        stream.read_exact(&mut payload).await?;
        if payload.len() < 10 || payload[payload.len() - 2..] != [0, 0] {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "test RCON packet has an invalid payload",
            ));
        }

        Ok(TestRconPacket {
            id: i32::from_le_bytes(payload[0..4].try_into().unwrap()),
            kind: i32::from_le_bytes(payload[4..8].try_into().unwrap()),
            body: payload[8..payload.len() - 2].to_vec(),
        })
    }

    async fn read_test_rcon_packet(stream: &mut TcpStream) -> TestRconPacket {
        try_read_test_rcon_packet(stream)
            .await
            .expect("test RCON packet should arrive")
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

    async fn spawn_player_count_rcon_server(
        player_count: u32,
    ) -> (String, oneshot::Receiver<()>, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test RCON listener should bind");
        let address = listener
            .local_addr()
            .expect("test RCON listener should have an address")
            .to_string();
        let (first_poll_sender, first_poll_receiver) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener
                .accept()
                .await
                .expect("supervisor should connect to test RCON");
            let auth = read_test_rcon_packet(&mut stream).await;
            assert_eq!(auth.kind, TEST_AUTH);
            assert_eq!(auth.body, b"secret");
            write_test_rcon_packet(&mut stream, auth.id, TEST_AUTH_RESPONSE, b"").await;

            let mut first_poll_sender = Some(first_poll_sender);
            loop {
                let packet = match try_read_test_rcon_packet(&mut stream).await {
                    Ok(packet) => packet,
                    Err(error)
                        if matches!(
                            error.kind(),
                            std::io::ErrorKind::UnexpectedEof
                                | std::io::ErrorKind::ConnectionReset
                                | std::io::ErrorKind::BrokenPipe
                        ) =>
                    {
                        break;
                    }
                    Err(error) => panic!("test RCON packet read failed: {error}"),
                };
                assert_eq!(packet.kind, TEST_EXEC_COMMAND);
                if packet.body == b"stop" {
                    break;
                }
                assert_eq!(packet.body, b"list");

                let end_marker = read_test_rcon_packet(&mut stream).await;
                assert_eq!(end_marker.kind, TEST_EXEC_COMMAND);
                assert!(end_marker.body.is_empty());
                let response = format!("There are {player_count} of a max of 20 players online:");
                write_test_rcon_packet(
                    &mut stream,
                    packet.id,
                    TEST_RESPONSE_VALUE,
                    response.as_bytes(),
                )
                .await;
                write_test_rcon_packet(&mut stream, end_marker.id, TEST_RESPONSE_VALUE, b"").await;
                if let Some(sender) = first_poll_sender.take() {
                    let _ = sender.send(());
                }
            }
        });
        (address, first_poll_receiver, server)
    }

    async fn spawn_blocked_player_count_rcon_server() -> (
        String,
        oneshot::Receiver<()>,
        oneshot::Sender<()>,
        JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test RCON listener should bind");
        let address = listener
            .local_addr()
            .expect("test RCON listener should have an address")
            .to_string();
        let (poll_started_sender, poll_started_receiver) = oneshot::channel();
        let (release_sender, release_receiver) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener
                .accept()
                .await
                .expect("supervisor should connect to test RCON");
            let auth = read_test_rcon_packet(&mut stream).await;
            assert_eq!(auth.kind, TEST_AUTH);
            assert_eq!(auth.body, b"secret");
            write_test_rcon_packet(&mut stream, auth.id, TEST_AUTH_RESPONSE, b"").await;

            let command = read_test_rcon_packet(&mut stream).await;
            assert_eq!(command.kind, TEST_EXEC_COMMAND);
            assert_eq!(command.body, b"list");
            let end_marker = read_test_rcon_packet(&mut stream).await;
            assert_eq!(end_marker.kind, TEST_EXEC_COMMAND);
            assert!(end_marker.body.is_empty());
            poll_started_sender
                .send(())
                .expect("test should still await the blocked RCON poll");
            release_receiver
                .await
                .expect("test should release the blocked RCON poll");
        });
        (address, poll_started_receiver, release_sender, server)
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

    fn launch_failure_config() -> SupervisorConfig {
        SupervisorConfig {
            command: "mcservernap-test-command-that-does-not-exist".to_owned(),
            arguments: Vec::new(),
            rcon_address: "127.0.0.1:9".to_owned(),
            rcon_password: "secret".to_owned(),
            poll_interval: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(30),
            startup_timeout: Duration::from_secs(30),
            retry_interval: Duration::from_millis(10),
            command_timeout: Duration::from_secs(1),
            shutdown_timeout: Duration::from_millis(250),
        }
    }

    fn rapid_exit_config(rcon_address: String) -> SupervisorConfig {
        let mut config = test_config(rcon_address);
        config.arguments[2] = "supervisor::tests::rapid_exit_fixture".to_owned();
        config
    }

    fn stable_exit_config(rcon_address: String) -> SupervisorConfig {
        let mut config = test_config(rcon_address);
        config.arguments[2] = "supervisor::tests::stable_exit_fixture".to_owned();
        config
    }

    fn signaled_exit_config(
        rcon_address: String,
        exit_signal_address: std::net::SocketAddr,
    ) -> SupervisorConfig {
        let mut config = test_config(rcon_address);
        config.arguments[2] = "supervisor::tests::signaled_exit_fixture".to_owned();
        config
            .arguments
            .insert(3, format!("exit-signal={exit_signal_address}"));
        config
    }

    fn failed_lifecycle(phase: ServerPhase, failure_streak: u32) -> LifecycleState {
        LifecycleState {
            phase,
            launch_generation: 1,
            failure: Some(FailureCategory::ExitedUnexpectedly),
            failure_streak,
            ..LifecycleState::stopped()
        }
    }

    fn spawn_launch_failure_supervisor() -> (
        mpsc::Sender<WakeRequest>,
        oneshot::Sender<()>,
        watch::Receiver<LifecycleState>,
        JoinHandle<Result<()>>,
    ) {
        let (wake_sender, wake_receiver) = mpsc::channel(1);
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let (lifecycle_sender, lifecycle) = watch::channel(LifecycleState::stopped());
        let supervisor = tokio::spawn(run(
            wake_receiver,
            shutdown_receiver,
            lifecycle_sender,
            launch_failure_config(),
        ));
        (wake_sender, shutdown_sender, lifecycle, supervisor)
    }

    async fn wait_for_lifecycle(
        lifecycle: &mut watch::Receiver<LifecycleState>,
        predicate: impl Fn(&LifecycleState) -> bool,
    ) {
        lifecycle
            .wait_for(predicate)
            .await
            .expect("supervisor should keep the lifecycle source open");
    }

    fn wait_for_test_process_exit(process_id: u32) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !test_process_has_exited(process_id) {
            assert!(
                std::time::Instant::now() < deadline,
                "test server process did not exit"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[cfg(target_os = "windows")]
    fn test_process_has_exited(process_id: u32) -> bool {
        let filter = format!("PID eq {process_id}");
        let output = std::process::Command::new("tasklist")
            .args(["/FI", &filter, "/FO", "CSV", "/NH"])
            .output()
            .expect("tasklist should inspect the test server process");
        assert!(output.status.success(), "tasklist should succeed");
        let quoted_process_id = format!("\"{process_id}\"");
        !String::from_utf8_lossy(&output.stdout).lines().any(|line| {
            line.split(',')
                .nth(1)
                .is_some_and(|field| field.trim() == quoted_process_id)
        })
    }

    #[cfg(unix)]
    fn test_process_has_exited(process_id: u32) -> bool {
        let output = std::process::Command::new("ps")
            .args(["-o", "stat=", "-p", &process_id.to_string()])
            .output()
            .expect("ps should inspect the test server process");
        !output.status.success()
            || String::from_utf8_lossy(&output.stdout)
                .trim_start()
                .starts_with('Z')
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
    #[ignore = "launched as a short-lived supervisor test server process"]
    fn rapid_exit_fixture() {
        std::thread::sleep(Duration::from_millis(500));
    }

    #[test]
    #[ignore = "launched as a stable-window supervisor test server process"]
    fn stable_exit_fixture() {
        std::thread::sleep(Duration::from_secs(2));
    }

    #[test]
    #[ignore = "launched as a signaled supervisor test server process"]
    fn signaled_exit_fixture() {
        let address = std::env::args()
            .find_map(|argument| argument.strip_prefix("exit-signal=").map(str::to_owned))
            .expect("test process should receive an exit signal address");
        let mut signal = std::net::TcpStream::connect(address)
            .expect("test process should connect to its exit signal");
        let mut byte = [0];
        std::io::Read::read_exact(&mut signal, &mut byte)
            .expect("test process should receive its exit signal");
        std::process::exit(0);
    }

    #[test]
    fn failure_backoff_progresses_and_caps_at_sixty_seconds() {
        let expected = [5, 10, 20, 40, 60, 60, 60];
        for (failure_streak, expected_seconds) in (1..).zip(expected) {
            assert_eq!(
                failure_backoff(failure_streak),
                Duration::from_secs(expected_seconds)
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn launch_failure_does_not_retry_without_new_demand() {
        let (wake_sender, shutdown_sender, mut lifecycle, supervisor) =
            spawn_launch_failure_supervisor();
        wake_sender
            .send(WakeRequest::observed(*lifecycle.borrow()))
            .await
            .expect("supervisor should accept the initial wake request");
        wait_for_lifecycle(&mut lifecycle, |state| state.phase == ServerPhase::Cooldown).await;

        let failure = *lifecycle.borrow();
        assert_eq!(failure.failure, Some(FailureCategory::LaunchFailed));
        assert_eq!(failure.failure_streak, 1);
        assert_eq!(failure.launch_generation, 1);
        assert_eq!(
            failure
                .retry_at
                .expect("failure should have a retry deadline")
                - failure.phase_started_at,
            Duration::from_secs(5)
        );

        tokio::time::advance(Duration::from_secs(5)).await;
        wait_for_lifecycle(&mut lifecycle, |state| state.phase == ServerPhase::Stopped).await;
        assert_eq!(lifecycle.borrow().launch_generation, 1);
        assert_eq!(lifecycle.borrow().failure_streak, 1);

        shutdown_sender
            .send(())
            .expect("supervisor should still await shutdown");
        supervisor
            .await
            .expect("supervisor task should not panic")
            .expect("supervisor shutdown should succeed");
    }

    #[tokio::test(start_paused = true)]
    async fn one_retry_latches_and_additional_demand_coalesces() {
        let (wake_sender, shutdown_sender, mut lifecycle, supervisor) =
            spawn_launch_failure_supervisor();
        wake_sender
            .send(WakeRequest::observed(*lifecycle.borrow()))
            .await
            .expect("supervisor should accept the initial wake request");
        wait_for_lifecycle(&mut lifecycle, |state| state.phase == ServerPhase::Cooldown).await;
        let first_failure = *lifecycle.borrow();

        wake_sender
            .send(WakeRequest::observed(first_failure))
            .await
            .expect("one post-failure wake should latch");
        let permit = wake_sender
            .reserve()
            .await
            .expect("supervisor should consume the latched retry request");
        permit.send(WakeRequest::observed(first_failure));
        for _ in 0..16 {
            assert!(matches!(
                wake_sender.try_send(WakeRequest::observed(first_failure)),
                Err(mpsc::error::TrySendError::Full(_))
            ));
        }

        tokio::time::advance(Duration::from_secs(5)).await;
        wait_for_lifecycle(&mut lifecycle, |state| state.failure_streak == 2).await;
        assert_eq!(lifecycle.borrow().phase, ServerPhase::Cooldown);
        assert_eq!(lifecycle.borrow().launch_generation, 2);
        assert_eq!(
            lifecycle.borrow().failure,
            Some(FailureCategory::LaunchFailed)
        );

        tokio::time::advance(Duration::from_secs(10)).await;
        wait_for_lifecycle(&mut lifecycle, |state| state.phase == ServerPhase::Stopped).await;
        assert_eq!(lifecycle.borrow().launch_generation, 2);

        shutdown_sender
            .send(())
            .expect("supervisor should still await shutdown");
        supervisor
            .await
            .expect("supervisor task should not panic")
            .expect("supervisor shutdown should succeed");
    }

    #[tokio::test(start_paused = true)]
    async fn wake_after_no_demand_expiry_launches_immediately() {
        let (wake_sender, shutdown_sender, mut lifecycle, supervisor) =
            spawn_launch_failure_supervisor();
        wake_sender
            .send(WakeRequest::observed(*lifecycle.borrow()))
            .await
            .expect("supervisor should accept the initial wake request");
        wait_for_lifecycle(&mut lifecycle, |state| state.phase == ServerPhase::Cooldown).await;

        tokio::time::advance(Duration::from_secs(5)).await;
        wait_for_lifecycle(&mut lifecycle, |state| state.phase == ServerPhase::Stopped).await;
        let expired = *lifecycle.borrow();
        wake_sender
            .send(WakeRequest::observed(expired))
            .await
            .expect("supervisor should accept later demand");
        wait_for_lifecycle(&mut lifecycle, |state| state.failure_streak == 2).await;

        let failure = *lifecycle.borrow();
        assert_eq!(failure.phase, ServerPhase::Cooldown);
        assert_eq!(failure.launch_generation, 2);
        assert_eq!(
            failure
                .retry_at
                .expect("second failure should have a deadline")
                - failure.phase_started_at,
            Duration::from_secs(10)
        );

        shutdown_sender
            .send(())
            .expect("supervisor should still await shutdown");
        supervisor
            .await
            .expect("supervisor task should not panic")
            .expect("supervisor shutdown should succeed");
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_interrupts_failure_cooldown() {
        let (wake_sender, shutdown_sender, mut lifecycle, supervisor) =
            spawn_launch_failure_supervisor();
        wake_sender
            .send(WakeRequest::observed(*lifecycle.borrow()))
            .await
            .expect("supervisor should accept the initial wake request");
        wait_for_lifecycle(&mut lifecycle, |state| state.phase == ServerPhase::Cooldown).await;

        shutdown_sender
            .send(())
            .expect("supervisor should still await shutdown");
        supervisor
            .await
            .expect("supervisor task should not panic")
            .expect("supervisor shutdown should succeed");
        assert_eq!(lifecycle.borrow().phase, ServerPhase::Stopped);
        assert_eq!(lifecycle.borrow().launch_generation, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn startup_timeout_cooldown_starts_after_cleanup_and_latched_wake_waits() {
        let (wake_sender, wake_receiver) = mpsc::channel(1);
        let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
        let starting = LifecycleState {
            phase: ServerPhase::Starting,
            launch_generation: 1,
            ..LifecycleState::stopped()
        };
        let (lifecycle_sender, mut lifecycle) = watch::channel(starting);
        let mut control = SupervisorControl {
            wake_requests: wake_receiver,
            shutdown: shutdown_receiver,
            lifecycle: lifecycle_sender,
        };
        let retry_delay = control.begin_failure_cleanup(FailureCategory::StartupTimedOut);
        let failed_cleanup = *lifecycle.borrow();
        assert_eq!(failed_cleanup.phase, ServerPhase::Stopping);
        assert_eq!(
            failed_cleanup.failure,
            Some(FailureCategory::StartupTimedOut)
        );
        assert_eq!(failed_cleanup.failure_streak, 1);
        assert!(failed_cleanup.retry_at.is_none());

        let (cleanup_sender, cleanup_receiver) = oneshot::channel();
        let cooldown = tokio::spawn(async move {
            let outcome = finish_failed_cleanup(
                async move {
                    cleanup_receiver
                        .await
                        .expect("test should release failed-process cleanup");
                    Ok(())
                },
                &mut control,
                retry_delay,
            )
            .await?;
            let CycleOutcome::Failed {
                retry_at,
                retry_pending,
            } = outcome
            else {
                panic!("successful failed-process cleanup should enter cooldown");
            };
            Ok::<_, anyhow::Error>(control.wait_for_cooldown(retry_at, retry_pending).await)
        });

        wake_sender
            .send(WakeRequest::observed(failed_cleanup))
            .await
            .expect("wake during failure cleanup should be accepted");
        let permit = wake_sender
            .reserve()
            .await
            .expect("failure cleanup should consume and latch the first wake");
        permit.send(WakeRequest::observed(failed_cleanup));

        tokio::time::advance(Duration::from_secs(30)).await;
        assert_eq!(lifecycle.borrow().phase, ServerPhase::Stopping);
        assert!(lifecycle.borrow().retry_at.is_none());
        let cleanup_finished_at = Instant::now();
        cleanup_sender
            .send(())
            .expect("failed-process cleanup should still be pending");
        wait_for_lifecycle(&mut lifecycle, |state| state.phase == ServerPhase::Cooldown).await;
        assert_eq!(
            lifecycle.borrow().retry_at,
            Some(cleanup_finished_at + Duration::from_secs(5))
        );

        tokio::time::advance(Duration::from_secs(4)).await;
        assert!(!cooldown.is_finished());
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(
            cooldown
                .await
                .expect("cooldown task should not panic")
                .expect("cooldown should succeed"),
            CooldownOutcome::Launch
        );
    }

    #[tokio::test(start_paused = true)]
    async fn demand_racing_cooldown_expiry_launches_at_most_once() {
        let (wake_sender, shutdown_sender, mut lifecycle, supervisor) =
            spawn_launch_failure_supervisor();
        wake_sender
            .send(WakeRequest::observed(*lifecycle.borrow()))
            .await
            .expect("supervisor should accept the initial wake request");
        wait_for_lifecycle(&mut lifecycle, |state| state.phase == ServerPhase::Cooldown).await;
        let failure = *lifecycle.borrow();
        let retry_at = failure.retry_at.expect("failure should have a deadline");
        let racing_sender = wake_sender.clone();
        let demand = tokio::spawn(async move {
            sleep_until(retry_at).await;
            racing_sender
                .send(WakeRequest::observed(failure))
                .await
                .expect("racing demand should reach the supervisor");
        });

        tokio::time::advance(Duration::from_secs(5)).await;
        demand.await.expect("racing demand task should not panic");
        wait_for_lifecycle(&mut lifecycle, |state| state.failure_streak == 2).await;
        assert_eq!(lifecycle.borrow().launch_generation, 2);

        tokio::task::yield_now().await;
        assert_eq!(lifecycle.borrow().launch_generation, 2);
        shutdown_sender
            .send(())
            .expect("supervisor should still await shutdown");
        supervisor
            .await
            .expect("supervisor task should not panic")
            .expect("supervisor shutdown should succeed");
    }

    #[tokio::test]
    async fn startup_timeout_enters_retryable_failure_cleanup() {
        let (wake_sender, wake_receiver) = mpsc::channel(1);
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let (lifecycle_sender, mut lifecycle) = watch::channel(LifecycleState::stopped());
        let mut config = test_config("127.0.0.1:9".to_owned());
        config.startup_timeout = Duration::from_millis(50);
        config.command_timeout = Duration::from_millis(10);
        config.retry_interval = Duration::from_millis(10);
        let supervisor = tokio::spawn(run(
            wake_receiver,
            shutdown_receiver,
            lifecycle_sender,
            config,
        ));

        wake_sender
            .send(WakeRequest::observed(*lifecycle.borrow()))
            .await
            .expect("supervisor should accept the initial wake request");
        wait_for_phase(&mut lifecycle, ServerPhase::Starting).await;
        wait_for_phase(&mut lifecycle, ServerPhase::Cooldown).await;

        let failure = *lifecycle.borrow();
        assert_eq!(failure.failure, Some(FailureCategory::StartupTimedOut));
        assert_eq!(failure.failure_streak, 1);
        assert_eq!(
            failure
                .retry_at
                .expect("startup timeout should have a deadline")
                - failure.phase_started_at,
            Duration::from_secs(5)
        );

        shutdown_sender
            .send(())
            .expect("supervisor should still await shutdown");
        supervisor
            .await
            .expect("supervisor task should not panic")
            .expect("supervisor shutdown should succeed");
    }

    #[tokio::test]
    async fn successful_exit_before_readiness_is_retryable() {
        let config = rapid_exit_config("127.0.0.1:9".to_owned());
        let child = process::launch(&config.command, &config.arguments)
            .expect("short-lived test server should launch");
        let (_wake_sender, wake_receiver) = mpsc::channel(1);
        let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
        let (lifecycle_sender, lifecycle) = watch::channel(LifecycleState::stopped());
        let mut control = SupervisorControl {
            wake_requests: wake_receiver,
            shutdown: shutdown_receiver,
            lifecycle: lifecycle_sender,
        };
        control.finish_launch_attempt(ServerPhase::Starting);

        let outcome = run_server_cycle(child, &mut control, &config)
            .await
            .expect("pre-readiness exit should be retryable");
        assert!(matches!(outcome, CycleOutcome::Failed { .. }));
        let failure = *lifecycle.borrow();
        assert_eq!(failure.phase, ServerPhase::Cooldown);
        assert_eq!(failure.failure, Some(FailureCategory::ExitedBeforeReady));
        assert_eq!(failure.failure_streak, 1);
        assert_eq!(
            failure
                .retry_at
                .expect("pre-readiness exit should have a deadline")
                - failure.phase_started_at,
            Duration::from_secs(5)
        );
    }

    #[tokio::test]
    async fn rapid_post_readiness_exits_escalate_the_retained_streak() {
        let (_wake_sender, wake_receiver) = mpsc::channel(1);
        let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
        let initial = failed_lifecycle(ServerPhase::Stopped, 1);
        let (lifecycle_sender, lifecycle) = watch::channel(initial);
        let mut control = SupervisorControl {
            wake_requests: wake_receiver,
            shutdown: shutdown_receiver,
            lifecycle: lifecycle_sender,
        };

        for (expected_streak, expected_delay) in [(2, 10), (3, 20)] {
            let (rcon_address, _first_poll, rcon_server) = spawn_player_count_rcon_server(1).await;
            let config = rapid_exit_config(rcon_address);
            let child = process::launch(&config.command, &config.arguments)
                .expect("short-lived test server should launch");
            control.finish_launch_attempt(ServerPhase::Starting);

            let outcome = run_server_cycle(child, &mut control, &config)
                .await
                .expect("unexpected exit should be retryable");
            assert!(matches!(outcome, CycleOutcome::Failed { .. }));
            let failure = *lifecycle.borrow();
            assert_eq!(failure.phase, ServerPhase::Cooldown);
            assert_eq!(failure.failure, Some(FailureCategory::ExitedUnexpectedly));
            assert_eq!(failure.failure_streak, expected_streak);
            assert_eq!(
                failure
                    .retry_at
                    .expect("unexpected exit should have a deadline")
                    - failure.phase_started_at,
                Duration::from_secs(expected_delay)
            );
            rcon_server
                .await
                .expect("test RCON server should not panic");
            control.set_phase(ServerPhase::Stopped);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn delayed_stability_check_retains_streak_for_an_already_exited_child() {
        let exit_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test exit-signal listener should bind");
        let exit_signal_address = exit_listener
            .local_addr()
            .expect("test exit-signal listener should have an address");
        let (rcon_address, poll_started, release_poll, rcon_server) =
            spawn_blocked_player_count_rcon_server().await;
        let mut config = signaled_exit_config(rcon_address.clone(), exit_signal_address);
        config.command_timeout = Duration::from_secs(120);
        let rcon = RconClient::connect(&rcon_address, "secret")
            .await
            .expect("test RCON client should authenticate");
        let child = process::launch(&config.command, &config.arguments)
            .expect("signaled test server should launch");
        let process_id = child
            .id()
            .expect("signaled test server should have a process ID");
        let (mut exit_signal, _) = exit_listener
            .accept()
            .await
            .expect("test server should connect to its exit signal");
        let (_wake_sender, wake_receiver) = mpsc::channel(1);
        let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
        let running = failed_lifecycle(ServerPhase::Running, 2);
        let (lifecycle_sender, lifecycle) = watch::channel(running);
        let mut control = SupervisorControl {
            wake_requests: wake_receiver,
            shutdown: shutdown_receiver,
            lifecycle: lifecycle_sender,
        };
        let monitor = tokio::spawn(async move {
            monitor_running_server(child, rcon, &mut control, &config).await
        });

        poll_started
            .await
            .expect("running monitor should block in its first player poll");
        exit_signal
            .write_all(&[1])
            .await
            .expect("test server should receive its exit signal");
        let mut exit_signal = exit_signal
            .into_std()
            .expect("test exit signal should convert to a standard socket");
        exit_signal
            .set_nonblocking(false)
            .expect("test exit signal should support blocking reads");
        // Blocking here keeps paused Tokio time before the stability deadline until the OS has
        // closed the socket as part of process exit.
        if let Err(error) = std::io::Read::read_to_end(&mut exit_signal, &mut Vec::new()) {
            assert_eq!(
                error.kind(),
                std::io::ErrorKind::ConnectionReset,
                "test server process should close its exit signal"
            );
        }
        wait_for_test_process_exit(process_id);

        tokio::time::advance(STABILITY_WINDOW).await;
        assert_eq!(lifecycle.borrow().failure_streak, 2);
        release_poll
            .send(())
            .expect("blocked RCON poll should still await release");

        let outcome = monitor
            .await
            .expect("running monitor task should not panic")
            .expect("delayed unexpected exit should remain retryable");
        assert!(matches!(outcome, CycleOutcome::Failed { .. }));
        let failure = *lifecycle.borrow();
        assert_eq!(failure.failure, Some(FailureCategory::ExitedUnexpectedly));
        assert_eq!(failure.failure_streak, 3);
        assert_eq!(
            failure
                .retry_at
                .expect("unexpected exit should have a deadline")
                - failure.phase_started_at,
            Duration::from_secs(20)
        );
        rcon_server
            .await
            .expect("test RCON server should not panic");
    }

    #[tokio::test(start_paused = true)]
    async fn stable_running_resets_the_streak_before_an_unexpected_exit() {
        let (rcon_address, first_poll, rcon_server) = spawn_player_count_rcon_server(1).await;
        let config = stable_exit_config(rcon_address.clone());
        let rcon = RconClient::connect(&rcon_address, "secret")
            .await
            .expect("test RCON client should authenticate");
        let child = process::launch(&config.command, &config.arguments)
            .expect("stable-window test server should launch");
        let (_wake_sender, wake_receiver) = mpsc::channel(1);
        let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
        let running = failed_lifecycle(ServerPhase::Running, 3);
        let (lifecycle_sender, mut lifecycle) = watch::channel(running);
        let mut control = SupervisorControl {
            wake_requests: wake_receiver,
            shutdown: shutdown_receiver,
            lifecycle: lifecycle_sender,
        };
        let monitor = tokio::spawn(async move {
            monitor_running_server(child, rcon, &mut control, &config).await
        });

        first_poll
            .await
            .expect("running monitor should complete its first player poll");
        assert_eq!(lifecycle.borrow().failure_streak, 3);
        tokio::time::advance(STABILITY_WINDOW).await;
        wait_for_lifecycle(&mut lifecycle, |state| state.failure_streak == 0).await;
        assert_eq!(lifecycle.borrow().phase, ServerPhase::Running);
        assert_eq!(lifecycle.borrow().failure, None);

        let outcome = monitor
            .await
            .expect("running monitor task should not panic")
            .expect("unexpected process exit should be retryable");
        assert!(matches!(outcome, CycleOutcome::Failed { .. }));
        let failure = *lifecycle.borrow();
        assert_eq!(failure.failure, Some(FailureCategory::ExitedUnexpectedly));
        assert_eq!(failure.failure_streak, 1);
        assert_eq!(
            failure
                .retry_at
                .expect("unexpected exit should have a deadline")
                - failure.phase_started_at,
            Duration::from_secs(5)
        );
        rcon_server
            .await
            .expect("test RCON server should not panic");
    }

    #[tokio::test]
    async fn controlled_idle_stop_resets_the_failure_streak() {
        let (rcon_address, rcon_ready, rcon_server) = spawn_test_rcon_server().await;
        let config = test_config(rcon_address.clone());
        let rcon = RconClient::connect(&rcon_address, "secret")
            .await
            .expect("test RCON client should authenticate");
        rcon_ready
            .await
            .expect("test RCON server should authenticate the client");
        let child = process::launch(&config.command, &config.arguments)
            .expect("test server process should launch");
        let (_wake_sender, wake_receiver) = mpsc::channel(1);
        let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
        let running = failed_lifecycle(ServerPhase::Running, 3);
        let (lifecycle_sender, lifecycle) = watch::channel(running);
        let mut control = SupervisorControl {
            wake_requests: wake_receiver,
            shutdown: shutdown_receiver,
            lifecycle: lifecycle_sender,
        };

        let outcome = stop_server(child, Some(rcon), &mut control, config.shutdown_timeout)
            .await
            .expect("controlled idle stop should succeed");
        assert_eq!(outcome, CycleOutcome::Stopped);
        assert_eq!(lifecycle.borrow().failure_streak, 0);
        assert_eq!(lifecycle.borrow().failure, None);
        rcon_server
            .await
            .expect("test RCON server should not panic");
    }

    #[tokio::test]
    async fn process_wait_error_is_fatal_after_successful_cleanup() {
        let config = test_config("127.0.0.1:9".to_owned());
        let child = process::launch(&config.command, &config.arguments)
            .expect("test server process should launch");
        let (_wake_sender, wake_receiver) = mpsc::channel(1);
        let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
        let running = LifecycleState {
            phase: ServerPhase::Running,
            launch_generation: 1,
            ..LifecycleState::stopped()
        };
        let (lifecycle_sender, lifecycle) = watch::channel(running);
        let mut control = SupervisorControl {
            wake_requests: wake_receiver,
            shutdown: shutdown_receiver,
            lifecycle: lifecycle_sender,
        };

        let error = reap_after_wait_error(
            child,
            None,
            &mut control,
            config.shutdown_timeout,
            std::io::Error::other("synthetic process wait failure"),
        )
        .await
        .expect_err("process wait errors must remain fatal");
        assert!(
            error
                .to_string()
                .contains("failed to wait for the server process")
        );
        assert_eq!(lifecycle.borrow().phase, ServerPhase::Stopped);
        assert_eq!(lifecycle.borrow().failure_streak, 0);
        assert!(lifecycle.borrow().retry_at.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn failed_process_cleanup_error_is_fatal_without_cooldown() {
        let (_wake_sender, wake_receiver) = mpsc::channel(1);
        let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
        let starting = LifecycleState {
            phase: ServerPhase::Starting,
            launch_generation: 1,
            ..LifecycleState::stopped()
        };
        let (lifecycle_sender, lifecycle) = watch::channel(starting);
        let mut control = SupervisorControl {
            wake_requests: wake_receiver,
            shutdown: shutdown_receiver,
            lifecycle: lifecycle_sender,
        };
        let retry_delay = control.begin_failure_cleanup(FailureCategory::StartupTimedOut);

        let error = finish_failed_cleanup(
            async { Err(anyhow::anyhow!("synthetic cleanup failure")) },
            &mut control,
            retry_delay,
        )
        .await
        .expect_err("cleanup failure must remain fatal");
        assert_eq!(error.to_string(), "synthetic cleanup failure");
        assert_eq!(lifecycle.borrow().phase, ServerPhase::Stopping);
        assert_eq!(
            lifecycle.borrow().failure,
            Some(FailureCategory::StartupTimedOut)
        );
        assert!(lifecycle.borrow().retry_at.is_none());
    }

    #[test]
    fn starting_and_running_wakes_cannot_start_a_later_cycle() {
        let stopped = LifecycleState {
            phase: ServerPhase::Stopped,
            launch_generation: 1,
            ..LifecycleState::stopped()
        };

        for phase in [ServerPhase::Starting, ServerPhase::Running] {
            let request = WakeRequest::observed(LifecycleState {
                phase,
                launch_generation: 1,
                ..stopped
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
            ..LifecycleState::stopped()
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
        let active = failed_lifecycle(ServerPhase::Running, 3);
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
        assert_eq!(lifecycle.borrow().failure_streak, 0);
        assert_eq!(lifecycle.borrow().failure, None);
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
        assert_eq!(lifecycle.borrow().failure_streak, 0);
        assert_eq!(lifecycle.borrow().failure, None);
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
            ..LifecycleState::stopped()
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
