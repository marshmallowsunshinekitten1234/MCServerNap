use std::future::Future;
use std::io::ErrorKind;
use std::process::ExitStatus;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::net::TcpStream;
use tokio::sync::oneshot::error::TryRecvError;
use tokio::sync::{OwnedRwLockWriteGuard, mpsc, oneshot, watch};
use tokio::time::{Instant, sleep_until, timeout, timeout_at};

use crate::backend_use::{BackendCycle, BackendUseCoordinator, CurrentCycleActivity};
use crate::context::ServerContext;
use crate::endpoint::Endpoint;
use crate::process::{self, LaunchCommand, OwnedProcess};
use crate::rcon::{self, RconClient, RconProbeClassification, RconSecret};

const STABILITY_WINDOW: Duration = Duration::from_secs(60);

macro_rules! server_debug {
    ($config:expr, $($argument:tt)*) => {
        log::debug!("[server={}] {}", $config.context, format_args!($($argument)*))
    };
}

macro_rules! server_info {
    ($config:expr, $($argument:tt)*) => {
        log::info!("[server={}] {}", $config.context, format_args!($($argument)*))
    };
}

macro_rules! server_warn {
    ($config:expr, $($argument:tt)*) => {
        log::warn!("[server={}] {}", $config.context, format_args!($($argument)*))
    };
}

macro_rules! server_error {
    ($config:expr, $($argument:tt)*) => {
        log::error!("[server={}] {}", $config.context, format_args!($($argument)*))
    };
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServerPhase {
    Reconciling,
    Stopped,
    Starting,
    Running,
    Stopping,
    Cooldown,
    External,
    Conflict,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReconciliationEvidence {
    None,
    Inconclusive,
    Positive,
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
    reconciliation_evidence: ReconciliationEvidence,
}

#[derive(Clone, Copy)]
pub(crate) struct LifecycleStatusSnapshot {
    pub(crate) phase: ServerPhase,
    pub(crate) failure: Option<FailureCategory>,
    pub(crate) failure_streak: u32,
    pub(crate) retry_at: Option<Instant>,
}

impl LifecycleState {
    pub(crate) fn reconciling() -> Self {
        Self {
            phase: ServerPhase::Reconciling,
            launch_generation: 0,
            failure: None,
            failure_streak: 0,
            phase_started_at: Instant::now(),
            retry_at: None,
            reconciliation_evidence: ReconciliationEvidence::None,
        }
    }

    #[cfg(test)]
    pub(crate) fn stopped() -> Self {
        Self {
            phase: ServerPhase::Stopped,
            launch_generation: 0,
            failure: None,
            failure_streak: 0,
            phase_started_at: Instant::now(),
            retry_at: None,
            reconciliation_evidence: ReconciliationEvidence::None,
        }
    }

    pub(crate) const fn phase(self) -> ServerPhase {
        self.phase
    }

    pub(crate) const fn status_snapshot(self) -> LifecycleStatusSnapshot {
        LifecycleStatusSnapshot {
            phase: self.phase,
            failure: self.failure,
            failure_streak: self.failure_streak,
            retry_at: self.retry_at,
        }
    }

    pub(crate) fn owned_running_cycle(self) -> Option<BackendCycle> {
        (self.phase == ServerPhase::Running)
            .then(|| BackendCycle::from_launch_generation(self.launch_generation))
    }

    #[cfg(test)]
    pub(crate) fn test_phase(phase: ServerPhase) -> Self {
        let reconciliation_evidence = match phase {
            ServerPhase::External => ReconciliationEvidence::Positive,
            ServerPhase::Conflict => ReconciliationEvidence::Inconclusive,
            _ => ReconciliationEvidence::None,
        };
        let state = Self {
            phase,
            reconciliation_evidence,
            ..Self::stopped()
        };
        assert!(state.invariant_holds());
        state
    }

    #[cfg(test)]
    pub(crate) fn test_status(
        phase: ServerPhase,
        failure: Option<FailureCategory>,
        failure_streak: u32,
        retry_at: Option<Instant>,
    ) -> Self {
        Self {
            failure,
            failure_streak,
            retry_at,
            ..Self::test_phase(phase)
        }
    }

    fn invariant_holds(self) -> bool {
        match self.phase {
            ServerPhase::Reconciling => true,
            ServerPhase::External => {
                self.reconciliation_evidence == ReconciliationEvidence::Positive
            }
            ServerPhase::Conflict => self.reconciliation_evidence != ReconciliationEvidence::None,
            ServerPhase::Stopped
            | ServerPhase::Starting
            | ServerPhase::Running
            | ServerPhase::Stopping
            | ServerPhase::Cooldown => self.reconciliation_evidence == ReconciliationEvidence::None,
        }
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
                ServerPhase::Reconciling
                    | ServerPhase::Stopped
                    | ServerPhase::Stopping
                    | ServerPhase::Cooldown
            )
    }

    fn requests_conflict_reconciliation(self, current: LifecycleState) -> bool {
        current.phase == ServerPhase::Conflict && self.observed == current
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
    pub context: ServerContext,
    pub launch: LaunchCommand,
    pub rcon: Option<RconConfig>,
    pub poll_interval: Duration,
    pub idle_timeout: Duration,
    pub startup_timeout: Duration,
    pub retry_interval: Duration,
    pub command_timeout: Duration,
    pub shutdown_timeout: Duration,
}

pub struct RconConfig {
    pub endpoint: Endpoint,
    pub password: RconSecret,
}

pub(crate) struct BackendEndpoint {
    endpoint: Endpoint,
    connect_timeout: Duration,
    #[cfg(test)]
    scripted_probes: Option<tokio::sync::Mutex<mpsc::UnboundedReceiver<ReconciliationResult>>>,
}

impl BackendEndpoint {
    pub(crate) fn network(endpoint: Endpoint, connect_timeout: Duration) -> Self {
        Self {
            endpoint,
            connect_timeout,
            #[cfg(test)]
            scripted_probes: None,
        }
    }
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
    Ready,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BackendProbeClassification {
    Connected,
    Refused,
    Inconclusive,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReconciliationClassification {
    Clean,
    FullyReady,
    Positive,
    Inconclusive,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ConflictClassification {
    rcon: Option<RconProbeClassification>,
    backend: BackendProbeClassification,
    sticky_positive: bool,
}

struct BackendProbeResult {
    classification: BackendProbeClassification,
    detail: Option<anyhow::Error>,
}

enum ReconciliationResult {
    ConfiguredRcon {
        rcon: rcon::RconProbeResult,
        backend: BackendProbeResult,
    },
    BackendOnly(BackendProbeResult),
}

impl ReconciliationResult {
    fn classification(&self) -> ReconciliationClassification {
        use BackendProbeClassification::{Connected, Inconclusive as BackendInconclusive, Refused};
        use RconProbeClassification::{
            AcceptedFailure, Authenticated, Inconclusive as RconInconclusive,
            Refused as RconRefused,
        };

        match self {
            Self::ConfiguredRcon { rcon, backend } => {
                match (rcon.classification, backend.classification) {
                    (Authenticated, Connected) => ReconciliationClassification::FullyReady,
                    (Authenticated | AcceptedFailure, _) | (_, Connected) => {
                        ReconciliationClassification::Positive
                    }
                    (RconRefused, Refused) => ReconciliationClassification::Clean,
                    (RconRefused | RconInconclusive, Refused | BackendInconclusive) => {
                        ReconciliationClassification::Inconclusive
                    }
                }
            }
            Self::BackendOnly(backend) => match backend.classification {
                Connected => ReconciliationClassification::FullyReady,
                Refused => ReconciliationClassification::Clean,
                BackendInconclusive => ReconciliationClassification::Inconclusive,
            },
        }
    }

    fn conflict_classification(&self, sticky_positive: bool) -> ConflictClassification {
        let (rcon, backend) = match self {
            Self::ConfiguredRcon { rcon, backend } => {
                (Some(rcon.classification), backend.classification)
            }
            Self::BackendOnly(backend) => (None, backend.classification),
        };
        ConflictClassification {
            rcon,
            backend,
            sticky_positive,
        }
    }

    fn log_details(&self, config: &SupervisorConfig) {
        let (rcon_detail, backend) = match self {
            Self::ConfiguredRcon { rcon, backend } => (rcon.detail.as_ref(), backend),
            Self::BackendOnly(backend) => (None, backend),
        };
        if let Some(error) = rcon_detail {
            server_debug!(config, "RCON reconciliation detail: {error:#}");
        }
        if let Some(error) = &backend.detail {
            server_debug!(config, "Minecraft backend reconciliation detail: {error:#}");
        }
    }
}

struct SupervisorControl {
    wake_requests: mpsc::Receiver<WakeRequest>,
    shutdown: oneshot::Receiver<()>,
    lifecycle: watch::Sender<LifecycleState>,
}

impl SupervisorControl {
    async fn wait_for_reconciliation_demand(&mut self) -> bool {
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
                    let current = self.current_lifecycle();
                    if request.can_start(current)
                        || request.requests_conflict_reconciliation(current)
                    {
                        return true;
                    }
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

    fn publish(&self, next: LifecycleState) {
        assert!(next.invariant_holds(), "invalid lifecycle state: {next:?}");
        self.lifecycle.send_replace(next);
    }

    fn record_launch_success(&self) {
        let current = self.current_lifecycle();
        assert_eq!(current.phase, ServerPhase::Reconciling);
        assert_ne!(
            current.reconciliation_evidence,
            ReconciliationEvidence::Positive
        );
        // Every request that observed the pre-launch phase belongs to the same coalesced burst.
        self.publish(LifecycleState {
            phase: ServerPhase::Starting,
            launch_generation: current.launch_generation.wrapping_add(1),
            failure: current.failure,
            failure_streak: current.failure_streak,
            phase_started_at: Instant::now(),
            retry_at: None,
            reconciliation_evidence: ReconciliationEvidence::None,
        });
    }

    fn record_launch_failure(&self) -> Instant {
        let current = self.current_lifecycle();
        assert_eq!(current.phase, ServerPhase::Reconciling);
        assert_ne!(
            current.reconciliation_evidence,
            ReconciliationEvidence::Positive
        );
        let failure_streak = current.failure_streak.saturating_add(1);
        let now = Instant::now();
        let retry_delay = failure_backoff(failure_streak);
        let retry_at = now + retry_delay;
        self.publish(LifecycleState {
            phase: ServerPhase::Cooldown,
            launch_generation: current.launch_generation.wrapping_add(1),
            failure: Some(FailureCategory::LaunchFailed),
            failure_streak,
            phase_started_at: now,
            retry_at: Some(retry_at),
            reconciliation_evidence: ReconciliationEvidence::None,
        });
        retry_at
    }

    fn record_reaped_failure(&self, failure: FailureCategory) -> Instant {
        let current = self.current_lifecycle();
        assert!(matches!(
            current.phase,
            ServerPhase::Starting | ServerPhase::Running
        ));
        assert_eq!(
            current.reconciliation_evidence,
            ReconciliationEvidence::None
        );
        let failure_streak = current.failure_streak.saturating_add(1);
        let now = Instant::now();
        let retry_delay = failure_backoff(failure_streak);
        let retry_at = now + retry_delay;
        self.publish(LifecycleState {
            phase: ServerPhase::Cooldown,
            launch_generation: current.launch_generation,
            failure: Some(failure),
            failure_streak,
            phase_started_at: now,
            retry_at: Some(retry_at),
            reconciliation_evidence: ReconciliationEvidence::None,
        });
        retry_at
    }

    fn begin_failure_cleanup(&self, failure: FailureCategory) -> Duration {
        let current = self.current_lifecycle();
        assert_eq!(current.phase, ServerPhase::Starting);
        assert_eq!(
            current.reconciliation_evidence,
            ReconciliationEvidence::None
        );
        let failure_streak = current.failure_streak.saturating_add(1);
        self.publish(LifecycleState {
            phase: ServerPhase::Stopping,
            launch_generation: current.launch_generation,
            failure: Some(failure),
            failure_streak,
            phase_started_at: Instant::now(),
            retry_at: None,
            reconciliation_evidence: ReconciliationEvidence::None,
        });
        failure_backoff(failure_streak)
    }

    fn finish_failure_cleanup(&self, retry_delay: Duration) -> Instant {
        let current = self.current_lifecycle();
        assert_eq!(current.phase, ServerPhase::Stopping);
        assert_eq!(
            current.reconciliation_evidence,
            ReconciliationEvidence::None
        );
        let now = Instant::now();
        let retry_at = now + retry_delay;
        self.publish(LifecycleState {
            phase: ServerPhase::Cooldown,
            launch_generation: current.launch_generation,
            failure: current.failure,
            failure_streak: current.failure_streak,
            phase_started_at: now,
            retry_at: Some(retry_at),
            reconciliation_evidence: ReconciliationEvidence::None,
        });
        retry_at
    }

    fn reset_failure_streak(&self) {
        let current = self.current_lifecycle();
        assert_eq!(current.phase, ServerPhase::Running);
        assert_eq!(
            current.reconciliation_evidence,
            ReconciliationEvidence::None
        );
        self.publish(LifecycleState {
            phase: current.phase,
            launch_generation: current.launch_generation,
            failure: None,
            failure_streak: 0,
            phase_started_at: current.phase_started_at,
            retry_at: None,
            reconciliation_evidence: ReconciliationEvidence::None,
        });
    }

    fn publish_running(&self, backend_use: &BackendUseCoordinator) -> BackendCycle {
        let current = self.current_lifecycle();
        assert_eq!(current.phase, ServerPhase::Starting);
        assert_eq!(
            current.reconciliation_evidence,
            ReconciliationEvidence::None
        );
        let running_since = Instant::now();
        let cycle = BackendCycle::from_launch_generation(current.launch_generation);
        backend_use.begin_cycle(cycle, running_since);
        self.publish(LifecycleState {
            phase: ServerPhase::Running,
            launch_generation: current.launch_generation,
            failure: current.failure,
            failure_streak: current.failure_streak,
            phase_started_at: running_since,
            retry_at: None,
            reconciliation_evidence: ReconciliationEvidence::None,
        });
        cycle
    }

    fn begin_idle_stop(&self, cycle: BackendCycle) {
        let current = self.current_lifecycle();
        assert_eq!(current.owned_running_cycle(), Some(cycle));
        self.publish(LifecycleState {
            phase: ServerPhase::Stopping,
            launch_generation: current.launch_generation,
            failure: None,
            failure_streak: 0,
            phase_started_at: Instant::now(),
            retry_at: None,
            reconciliation_evidence: ReconciliationEvidence::None,
        });
    }

    fn set_phase(&self, phase: ServerPhase) {
        assert!(matches!(
            phase,
            ServerPhase::Stopped
                | ServerPhase::Starting
                | ServerPhase::Running
                | ServerPhase::Stopping
                | ServerPhase::Cooldown
        ));
        let current = self.current_lifecycle();
        assert_eq!(
            current.reconciliation_evidence,
            ReconciliationEvidence::None
        );
        self.publish(LifecycleState {
            phase,
            launch_generation: current.launch_generation,
            failure: current.failure,
            failure_streak: current.failure_streak,
            phase_started_at: Instant::now(),
            retry_at: None,
            reconciliation_evidence: ReconciliationEvidence::None,
        });
    }

    fn begin_reconciliation(&self) -> ReconciliationEvidence {
        let current = self.current_lifecycle();
        assert!(matches!(
            current.phase,
            ServerPhase::Stopped | ServerPhase::Cooldown | ServerPhase::Conflict
        ));
        self.publish(LifecycleState {
            phase: ServerPhase::Reconciling,
            launch_generation: current.launch_generation,
            failure: current.failure,
            failure_streak: current.failure_streak,
            phase_started_at: Instant::now(),
            retry_at: current.retry_at,
            reconciliation_evidence: current.reconciliation_evidence,
        });
        current.reconciliation_evidence
    }

    fn publish_stopped_after_clean_reconciliation(&self) {
        let current = self.current_lifecycle();
        assert_eq!(current.phase, ServerPhase::Reconciling);
        self.publish(LifecycleState {
            phase: ServerPhase::Stopped,
            launch_generation: current.launch_generation,
            failure: current.failure,
            failure_streak: current.failure_streak,
            phase_started_at: Instant::now(),
            retry_at: None,
            reconciliation_evidence: ReconciliationEvidence::None,
        });
    }

    fn publish_external(&self, consume_demand: bool) {
        let current = self.current_lifecycle();
        assert!(matches!(
            current.phase,
            ServerPhase::Reconciling | ServerPhase::External
        ));
        self.publish(LifecycleState {
            phase: ServerPhase::External,
            launch_generation: if consume_demand {
                current.launch_generation.wrapping_add(1)
            } else {
                current.launch_generation
            },
            failure: current.failure,
            failure_streak: current.failure_streak,
            phase_started_at: Instant::now(),
            retry_at: None,
            reconciliation_evidence: ReconciliationEvidence::Positive,
        });
    }

    fn publish_conflict(&self, evidence: ReconciliationEvidence, consume_demand: bool) {
        assert_ne!(evidence, ReconciliationEvidence::None);
        let current = self.current_lifecycle();
        assert!(matches!(
            current.phase,
            ServerPhase::Reconciling | ServerPhase::External
        ));
        self.publish(LifecycleState {
            phase: ServerPhase::Conflict,
            launch_generation: if consume_demand {
                current.launch_generation.wrapping_add(1)
            } else {
                current.launch_generation
            },
            failure: current.failure,
            failure_streak: current.failure_streak,
            phase_started_at: Instant::now(),
            retry_at: None,
            reconciliation_evidence: evidence,
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
                request = self.wake_requests.recv() => {
                    let Some(request) = request else {
                        self.set_phase(ServerPhase::Stopped);
                        return CooldownOutcome::Shutdown;
                    };
                    if request.requests_retry(self.current_lifecycle()) && !retry_pending {
                        retry_pending = true;
                    }
                }
                () = sleep_until(retry_at) => {}
            }
        }
    }
}

enum ReconciliationWaitOutcome {
    Completed {
        result: ReconciliationResult,
        startup_demand: bool,
    },
    Shutdown,
}

#[derive(Debug)]
enum StartupReconciliationOutcome {
    Idle,
    Launch,
    Shutdown,
}

#[derive(Debug)]
enum FinalGateOutcome {
    Launch,
    Blocked,
    Shutdown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExternalMonitorOutcome {
    Conflict,
    Shutdown,
}

async fn probe_backend(backend: &BackendEndpoint) -> BackendProbeResult {
    match timeout(
        backend.connect_timeout,
        TcpStream::connect((backend.endpoint.host(), backend.endpoint.port())),
    )
    .await
    {
        Ok(Ok(_)) => BackendProbeResult {
            classification: BackendProbeClassification::Connected,
            detail: None,
        },
        Ok(Err(error)) if error.kind() == ErrorKind::ConnectionRefused => BackendProbeResult {
            classification: BackendProbeClassification::Refused,
            detail: Some(error.into()),
        },
        Ok(Err(error)) => BackendProbeResult {
            classification: BackendProbeClassification::Inconclusive,
            detail: Some(error.into()),
        },
        Err(_) => BackendProbeResult {
            classification: BackendProbeClassification::Inconclusive,
            detail: Some(anyhow::anyhow!(
                "Minecraft backend TCP connection timed out"
            )),
        },
    }
}

async fn reconciliation_probe(
    config: &SupervisorConfig,
    backend: &BackendEndpoint,
) -> ReconciliationResult {
    #[cfg(test)]
    if let Some(scripted_probes) = &backend.scripted_probes {
        return scripted_probes
            .lock()
            .await
            .recv()
            .await
            .expect("test reconciliation script ended unexpectedly");
    }

    if let Some(rcon_config) = &config.rcon {
        let (rcon, backend) = tokio::join!(
            rcon::probe(
                &rcon_config.endpoint,
                rcon_config.password.expose(),
                config.command_timeout,
            ),
            probe_backend(backend),
        );
        ReconciliationResult::ConfiguredRcon { rcon, backend }
    } else {
        ReconciliationResult::BackendOnly(probe_backend(backend).await)
    }
}

async fn wait_for_reconciliation(
    control: &mut SupervisorControl,
    config: &SupervisorConfig,
    backend: &BackendEndpoint,
    latch_startup_demand: bool,
) -> ReconciliationWaitOutcome {
    let probe = reconciliation_probe(config, backend);
    tokio::pin!(probe);
    let mut startup_demand = false;

    loop {
        if control.wake_requests.is_closed() {
            return ReconciliationWaitOutcome::Shutdown;
        }

        tokio::select! {
            biased;

            _ = &mut control.shutdown => {
                return ReconciliationWaitOutcome::Shutdown;
            }
            result = &mut probe => {
                return ReconciliationWaitOutcome::Completed {
                    result,
                    startup_demand,
                };
            }
            request = control.wake_requests.recv() => {
                let Some(request) = request else {
                    return ReconciliationWaitOutcome::Shutdown;
                };
                if latch_startup_demand
                    && !startup_demand
                    && request.observed == control.current_lifecycle()
                {
                    startup_demand = true;
                    server_info!(config, "Latched one server wake-up request during startup reconciliation");
                } else {
                    server_debug!(config, "Ignoring coalesced server wake-up request during reconciliation");
                }
            }
        }
    }
}

fn publish_conflict_observation(
    control: &SupervisorControl,
    config: &SupervisorConfig,
    result: &ReconciliationResult,
    evidence: ReconciliationEvidence,
    consume_demand: bool,
    last_conflict: &mut Option<ConflictClassification>,
) {
    let sticky_positive = evidence == ReconciliationEvidence::Positive;
    let classification = result.conflict_classification(sticky_positive);
    control.publish_conflict(evidence, consume_demand);
    if conflict_warning_changed(last_conflict, classification) {
        let next_step = if sticky_positive {
            "restore full external readiness and try another login, or, after independently verifying process exit, restart the listener to clear remembered evidence"
        } else {
            "another login may retry reconciliation"
        };
        if let Some(rcon) = classification.rcon {
            server_warn!(
                config,
                "Backend conflict: RCON={rcon:?}, backend-port={:?}, sticky-positive={sticky_positive}; {next_step}",
                classification.backend,
            );
        } else {
            server_warn!(
                config,
                "Backend conflict: RCON=not-configured, backend-port={:?}, sticky-positive={sticky_positive}; {next_step}",
                classification.backend,
            );
        }
    }
}

fn publish_external_backend(
    control: &SupervisorControl,
    config: &SupervisorConfig,
    consume_demand: bool,
    last_conflict: &mut Option<ConflictClassification>,
) {
    control.publish_external(consume_demand);
    server_info!(
        config,
        "Detected a ready external backend; proxying without process ownership or automatic shutdown"
    );
    *last_conflict = None;
}

fn conflict_warning_changed(
    last_conflict: &mut Option<ConflictClassification>,
    classification: ConflictClassification,
) -> bool {
    if last_conflict.as_ref() == Some(&classification) {
        return false;
    }
    *last_conflict = Some(classification);
    true
}

async fn reconcile_startup(
    control: &mut SupervisorControl,
    config: &SupervisorConfig,
    backend: &BackendEndpoint,
    last_conflict: &mut Option<ConflictClassification>,
) -> StartupReconciliationOutcome {
    let ReconciliationWaitOutcome::Completed {
        result,
        startup_demand,
    } = wait_for_reconciliation(control, config, backend, true).await
    else {
        return StartupReconciliationOutcome::Shutdown;
    };
    result.log_details(config);
    if control.shutdown_pending() {
        return StartupReconciliationOutcome::Shutdown;
    }

    match result.classification() {
        ReconciliationClassification::Clean => {
            control.publish_stopped_after_clean_reconciliation();
            *last_conflict = None;
            if startup_demand {
                StartupReconciliationOutcome::Launch
            } else {
                StartupReconciliationOutcome::Idle
            }
        }
        ReconciliationClassification::FullyReady => {
            publish_external_backend(control, config, startup_demand, last_conflict);
            StartupReconciliationOutcome::Idle
        }
        ReconciliationClassification::Positive => {
            publish_conflict_observation(
                control,
                config,
                &result,
                ReconciliationEvidence::Positive,
                startup_demand,
                last_conflict,
            );
            StartupReconciliationOutcome::Idle
        }
        ReconciliationClassification::Inconclusive => {
            publish_conflict_observation(
                control,
                config,
                &result,
                ReconciliationEvidence::Inconclusive,
                startup_demand,
                last_conflict,
            );
            StartupReconciliationOutcome::Idle
        }
    }
}

async fn reconcile_final_gate(
    control: &mut SupervisorControl,
    config: &SupervisorConfig,
    backend: &BackendEndpoint,
    last_conflict: &mut Option<ConflictClassification>,
) -> FinalGateOutcome {
    let prior_evidence = control.begin_reconciliation();
    let ReconciliationWaitOutcome::Completed { result, .. } =
        wait_for_reconciliation(control, config, backend, false).await
    else {
        return FinalGateOutcome::Shutdown;
    };
    result.log_details(config);
    if control.shutdown_pending() {
        return FinalGateOutcome::Shutdown;
    }

    let classification = result.classification();
    if prior_evidence == ReconciliationEvidence::Positive {
        if classification == ReconciliationClassification::FullyReady {
            publish_external_backend(control, config, true, last_conflict);
        } else {
            publish_conflict_observation(
                control,
                config,
                &result,
                ReconciliationEvidence::Positive,
                true,
                last_conflict,
            );
        }
        return FinalGateOutcome::Blocked;
    }

    match classification {
        ReconciliationClassification::Clean => {
            *last_conflict = None;
            FinalGateOutcome::Launch
        }
        ReconciliationClassification::FullyReady => {
            publish_external_backend(control, config, true, last_conflict);
            FinalGateOutcome::Blocked
        }
        ReconciliationClassification::Positive => {
            publish_conflict_observation(
                control,
                config,
                &result,
                ReconciliationEvidence::Positive,
                true,
                last_conflict,
            );
            FinalGateOutcome::Blocked
        }
        ReconciliationClassification::Inconclusive => {
            publish_conflict_observation(
                control,
                config,
                &result,
                ReconciliationEvidence::Inconclusive,
                true,
                last_conflict,
            );
            FinalGateOutcome::Blocked
        }
    }
}

async fn monitor_external_server(
    control: &mut SupervisorControl,
    config: &SupervisorConfig,
    backend: &BackendEndpoint,
    last_conflict: &mut Option<ConflictClassification>,
) -> ExternalMonitorOutcome {
    let mut next_poll = Instant::now() + config.poll_interval;

    loop {
        if control.wake_requests.is_closed() {
            return ExternalMonitorOutcome::Shutdown;
        }

        tokio::select! {
            biased;

            _ = &mut control.shutdown => return ExternalMonitorOutcome::Shutdown,
            request = control.wake_requests.recv() => {
                if request.is_none() {
                    return ExternalMonitorOutcome::Shutdown;
                }
                server_debug!(config, "Ignoring stale server wake-up request while proxying an external backend");
                continue;
            }
            () = sleep_until(next_poll) => {}
        }

        let ReconciliationWaitOutcome::Completed { result, .. } =
            wait_for_reconciliation(control, config, backend, false).await
        else {
            return ExternalMonitorOutcome::Shutdown;
        };
        result.log_details(config);
        if control.shutdown_pending() {
            return ExternalMonitorOutcome::Shutdown;
        }
        if result.classification() == ReconciliationClassification::FullyReady {
            next_poll = Instant::now() + config.poll_interval;
            continue;
        }

        publish_conflict_observation(
            control,
            config,
            &result,
            ReconciliationEvidence::Positive,
            false,
            last_conflict,
        );
        return ExternalMonitorOutcome::Conflict;
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

fn log_current_failure_cooldown(config: &SupervisorConfig, control: &SupervisorControl) {
    let lifecycle = control.current_lifecycle();
    let failure = lifecycle
        .failure
        .expect("failure cooldown must retain its failure category");
    let retry_at = lifecycle
        .retry_at
        .expect("failure cooldown must retain its retry deadline");
    let retry_delay = retry_at.saturating_duration_since(lifecycle.phase_started_at);
    server_warn!(
        config,
        "Server entered a {retry_delay:?} cooldown after {failure:?} (failure streak {})",
        lifecycle.failure_streak
    );
}

pub(crate) async fn run(
    wake_requests: mpsc::Receiver<WakeRequest>,
    shutdown: oneshot::Receiver<()>,
    lifecycle: watch::Sender<LifecycleState>,
    config: SupervisorConfig,
    backend: BackendEndpoint,
    backend_use: Arc<BackendUseCoordinator>,
) -> Result<()> {
    let mut control = SupervisorControl {
        wake_requests,
        shutdown,
        lifecycle,
    };
    assert_eq!(control.current_lifecycle().phase, ServerPhase::Reconciling);
    assert!(control.current_lifecycle().invariant_holds());
    let mut last_conflict = None;
    let mut launch_pending =
        match reconcile_startup(&mut control, &config, &backend, &mut last_conflict).await {
            StartupReconciliationOutcome::Launch => true,
            StartupReconciliationOutcome::Idle => false,
            StartupReconciliationOutcome::Shutdown => return Ok(()),
        };

    loop {
        if control.current_lifecycle().phase == ServerPhase::External {
            match monitor_external_server(&mut control, &config, &backend, &mut last_conflict).await
            {
                ExternalMonitorOutcome::Conflict => {}
                ExternalMonitorOutcome::Shutdown => return Ok(()),
            }
        }

        if !launch_pending && !control.wait_for_reconciliation_demand().await {
            if control.current_lifecycle().phase == ServerPhase::Stopped {
                control.set_phase(ServerPhase::Stopped);
            }
            return Ok(());
        }
        launch_pending = false;

        if control.shutdown_pending() {
            return Ok(());
        }

        match reconcile_final_gate(&mut control, &config, &backend, &mut last_conflict).await {
            FinalGateOutcome::Launch => {}
            FinalGateOutcome::Blocked => continue,
            FinalGateOutcome::Shutdown => return Ok(()),
        }

        let child = match process::launch(&config.launch) {
            Ok(child) => {
                server_info!(
                    config,
                    "Launched server command {:?} {:?}",
                    config.launch.program,
                    config.launch.arguments
                );
                control.record_launch_success();
                child
            }
            Err(error) => {
                server_error!(config, "Unable to start the Minecraft server: {error:#}");
                let retry_at = control.record_launch_failure();
                log_current_failure_cooldown(&config, &control);
                match control.wait_for_cooldown(retry_at, false).await {
                    CooldownOutcome::Launch => launch_pending = true,
                    CooldownOutcome::Stopped => {}
                    CooldownOutcome::Shutdown => return Ok(()),
                }
                continue;
            }
        };

        let outcome =
            run_server_cycle(child, &mut control, &config, &backend, &backend_use).await?;
        match outcome {
            CycleOutcome::Stopped => {
                control.set_phase(ServerPhase::Stopped);
                server_info!(config, "Minecraft server is stopped and ready to nap");
            }
            CycleOutcome::Restart => {
                control.set_phase(ServerPhase::Stopped);
                launch_pending = true;
            }
            CycleOutcome::Failed {
                retry_at,
                retry_pending,
            } => {
                log_current_failure_cooldown(&config, &control);
                match control.wait_for_cooldown(retry_at, retry_pending).await {
                    CooldownOutcome::Launch => launch_pending = true,
                    CooldownOutcome::Stopped => {}
                    CooldownOutcome::Shutdown => return Ok(()),
                }
            }
            CycleOutcome::Shutdown => {
                control.set_phase(ServerPhase::Stopped);
                return Ok(());
            }
        }
    }
}

async fn run_server_cycle(
    mut child: OwnedProcess,
    control: &mut SupervisorControl,
    config: &SupervisorConfig,
    backend: &BackendEndpoint,
    backend_use: &BackendUseCoordinator,
) -> Result<CycleOutcome> {
    let readiness = wait_for_readiness(&mut child, control, config, backend).await;
    match readiness {
        ReadinessOutcome::Ready => {}
        ReadinessOutcome::StartupTimedOut => {
            let readiness_source = if config.rcon.is_some() {
                "RCON"
            } else {
                "the Minecraft backend TCP port"
            };
            server_error!(
                config,
                "{readiness_source} did not become ready within {:?}; terminating the server",
                config.startup_timeout
            );
            return stop_failed_server(child, control, config, FailureCategory::StartupTimedOut)
                .await;
        }
        ReadinessOutcome::Exited(status) => {
            return Ok(reaped_failure(
                control,
                config,
                FailureCategory::ExitedBeforeReady,
                status,
            ));
        }
        ReadinessOutcome::ProcessWaitFailed(error) => {
            return reap_after_wait_error(child, control, config, error).await;
        }
        ReadinessOutcome::Shutdown => {
            return stop_for_shutdown(child, control, config).await;
        }
    }

    if control.shutdown_pending() {
        return stop_for_shutdown(child, control, config).await;
    }
    if let Some(rcon) = &config.rcon {
        server_info!(config, "RCON is ready at {}", rcon.endpoint);
    } else {
        server_info!(config, "Minecraft backend TCP port is ready");
    }
    let cycle = control.publish_running(backend_use);
    monitor_running_server(
        child,
        PlayerInspection::from_config(config),
        control,
        config,
        backend_use,
        cycle,
    )
    .await
}

async fn wait_for_readiness(
    child: &mut OwnedProcess,
    control: &mut SupervisorControl,
    config: &SupervisorConfig,
    backend: &BackendEndpoint,
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
        let connect_timeout = if config.rcon.is_some() {
            config.command_timeout.min(remaining)
        } else {
            backend.connect_timeout.min(remaining)
        };
        let connect = async {
            if let Some(rcon) = &config.rcon {
                timeout(
                    connect_timeout,
                    RconClient::connect(&rcon.endpoint, rcon.password.expose()),
                )
                .await
                .context("RCON connection attempt timed out")??;
            } else {
                timeout(
                    connect_timeout,
                    TcpStream::connect((backend.endpoint.host(), backend.endpoint.port())),
                )
                .await
                .context("Minecraft backend TCP connection attempt timed out")??;
            }
            Result::<()>::Ok(())
        };

        tokio::select! {
            biased;

            _ = &mut control.shutdown => return ReadinessOutcome::Shutdown,
            request = control.wake_requests.recv() => {
                if request.is_none() {
                    return ReadinessOutcome::Shutdown;
                }
                server_debug!(config, "Ignoring duplicate start request during startup");
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
                    Ok(()) => return ReadinessOutcome::Ready,
                    Err(error) => {
                        failed_attempts += 1;
                        if failed_attempts == 1 || failed_attempts.is_multiple_of(30) {
                            server_warn!(config, "Backend is not ready yet: {error:#}");
                        } else {
                            server_debug!(config, "Backend is not ready yet: {error:#}");
                        }
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
                server_debug!(config, "Ignoring duplicate start request during startup");
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

enum IdleExclusiveOutcome {
    Acquired(OwnedRwLockWriteGuard<()>),
    ActivityChanged,
    Shutdown,
    ChildWait(std::io::Result<ExitStatus>),
}

enum FinalRconCheck {
    Zero,
    Veto(Option<RconClient>),
    Shutdown,
    ChildWait(std::io::Result<ExitStatus>),
}

enum IdlePlayerCheck {
    Confirmed,
    Veto,
    Shutdown,
    ChildWait(std::io::Result<ExitStatus>),
}

struct RunningMonitorState {
    cycle: BackendCycle,
    player_inspection: PlayerInspection,
    stable_at: Instant,
    stability_pending: bool,
}

enum PlayerInspection {
    ProxyOnly,
    ConfiguredRcon {
        client: Option<RconClient>,
        next_poll: Instant,
        zero_anchor: Option<Instant>,
    },
}

impl PlayerInspection {
    fn from_config(config: &SupervisorConfig) -> Self {
        if config.rcon.is_some() {
            Self::ConfiguredRcon {
                client: None,
                next_poll: Instant::now(),
                zero_anchor: None,
            }
        } else {
            Self::ProxyOnly
        }
    }
}

impl RunningMonitorState {
    fn new(
        cycle: BackendCycle,
        player_inspection: PlayerInspection,
        lifecycle: LifecycleState,
    ) -> Self {
        assert_eq!(lifecycle.owned_running_cycle(), Some(cycle));
        Self {
            cycle,
            player_inspection,
            stable_at: lifecycle.phase_started_at + STABILITY_WINDOW,
            stability_pending: lifecycle.failure_streak > 0,
        }
    }

    fn effective_idle_anchor(&self, activity: Option<CurrentCycleActivity>) -> Option<Instant> {
        let activity = activity.filter(|snapshot| snapshot.cycle() == self.cycle)?;
        let proxy_anchor = activity.proxy_idle_anchor()?;
        match &self.player_inspection {
            PlayerInspection::ProxyOnly => Some(proxy_anchor),
            PlayerInspection::ConfiguredRcon { zero_anchor, .. } => {
                Some(proxy_anchor.max((*zero_anchor)?))
            }
        }
    }
}

enum RunningMonitorOutcome {
    Continue,
    Committed,
    Shutdown,
    ChildWait(std::io::Result<ExitStatus>),
}

fn complete_stability_window(
    child: &mut OwnedProcess,
    control: &SupervisorControl,
    config: &SupervisorConfig,
    stability_pending: &mut bool,
) -> std::io::Result<Option<ExitStatus>> {
    let Some(status) = child.try_wait()? else {
        control.reset_failure_streak();
        *stability_pending = false;
        server_info!(
            config,
            "Server has run continuously for {STABILITY_WINDOW:?}; reset the failure streak"
        );
        return Ok(None);
    };
    Ok(Some(status))
}

async fn acquire_idle_exclusive(
    child: &mut OwnedProcess,
    control: &mut SupervisorControl,
    backend_use: &BackendUseCoordinator,
    config: &SupervisorConfig,
    state: &mut RunningMonitorState,
    expected_anchor: Instant,
) -> IdleExclusiveOutcome {
    let exclusive = backend_use.acquire_exclusive();
    tokio::pin!(exclusive);

    loop {
        if control.wake_requests.is_closed() {
            return IdleExclusiveOutcome::Shutdown;
        }
        let activity_changed = backend_use.activity_notified();
        tokio::pin!(activity_changed);
        if state.effective_idle_anchor(backend_use.activity_snapshot()) != Some(expected_anchor) {
            return IdleExclusiveOutcome::ActivityChanged;
        }

        tokio::select! {
            biased;

            _ = &mut control.shutdown => return IdleExclusiveOutcome::Shutdown,
            request = control.wake_requests.recv() => {
                if request.is_none() {
                    return IdleExclusiveOutcome::Shutdown;
                }
                server_debug!(config, "Ignoring start request because the server is already running");
            }
            status = child.wait() => return IdleExclusiveOutcome::ChildWait(status),
            () = sleep_until(state.stable_at), if state.stability_pending => {
                match complete_stability_window(child, control, config, &mut state.stability_pending) {
                    Ok(Some(status)) => return IdleExclusiveOutcome::ChildWait(Ok(status)),
                    Ok(None) => {}
                    Err(error) => return IdleExclusiveOutcome::ChildWait(Err(error)),
                }
            }
            () = &mut activity_changed => {
                let anchor = state.effective_idle_anchor(backend_use.activity_snapshot());
                if anchor != Some(expected_anchor)
                    || anchor.is_none_or(|anchor| Instant::now() < anchor + config.idle_timeout)
                {
                    return IdleExclusiveOutcome::ActivityChanged;
                }
            }
            guard = &mut exclusive => return IdleExclusiveOutcome::Acquired(guard),
        }
    }
}

async fn final_rcon_player_check(
    mut client: RconClient,
    child: &mut OwnedProcess,
    control: &mut SupervisorControl,
    config: &SupervisorConfig,
    state: &mut RunningMonitorState,
) -> FinalRconCheck {
    enum WaitOutcome {
        Command(std::result::Result<Result<String>, tokio::time::error::Elapsed>),
        Shutdown,
        ChildWait(std::io::Result<ExitStatus>),
    }

    let outcome = {
        let command = timeout(config.command_timeout, client.command("list"));
        tokio::pin!(command);
        loop {
            if control.wake_requests.is_closed() {
                break WaitOutcome::Shutdown;
            }
            tokio::select! {
                biased;

                _ = &mut control.shutdown => break WaitOutcome::Shutdown,
                request = control.wake_requests.recv() => {
                    if request.is_none() {
                        break WaitOutcome::Shutdown;
                    }
                    server_debug!(config, "Ignoring start request because the server is already running");
                }
                status = child.wait() => break WaitOutcome::ChildWait(status),
                () = sleep_until(state.stable_at), if state.stability_pending => {
                    match complete_stability_window(child, control, config, &mut state.stability_pending) {
                        Ok(Some(status)) => break WaitOutcome::ChildWait(Ok(status)),
                        Ok(None) => {}
                        Err(error) => break WaitOutcome::ChildWait(Err(error)),
                    }
                }
                result = &mut command => break WaitOutcome::Command(result),
            }
        }
    };

    match outcome {
        WaitOutcome::Command(Ok(Ok(response))) => match observed_player_count(config, &response) {
            Some(0) => FinalRconCheck::Zero,
            Some(player_count) => {
                server_debug!(
                    config,
                    "Final idle check found {player_count} online player(s)"
                );
                FinalRconCheck::Veto(Some(client))
            }
            None => FinalRconCheck::Veto(Some(client)),
        },
        WaitOutcome::Command(Ok(Err(error))) => {
            server_warn!(
                config,
                "Final RCON player check failed; idle shutdown was canceled: {error:#}"
            );
            FinalRconCheck::Veto(None)
        }
        WaitOutcome::Command(Err(_)) => {
            server_warn!(
                config,
                "Final RCON player check timed out; idle shutdown was canceled"
            );
            FinalRconCheck::Veto(None)
        }
        WaitOutcome::Shutdown => FinalRconCheck::Shutdown,
        WaitOutcome::ChildWait(status) => FinalRconCheck::ChildWait(status),
    }
}

async fn confirm_idle_player_state(
    child: &mut OwnedProcess,
    control: &mut SupervisorControl,
    config: &SupervisorConfig,
    state: &mut RunningMonitorState,
) -> IdlePlayerCheck {
    let final_rcon = match &mut state.player_inspection {
        PlayerInspection::ProxyOnly => return IdlePlayerCheck::Confirmed,
        PlayerInspection::ConfiguredRcon {
            client,
            next_poll,
            zero_anchor,
        } => {
            let Some(client) = client.take() else {
                *zero_anchor = None;
                *next_poll = Instant::now() + config.retry_interval;
                return IdlePlayerCheck::Veto;
            };
            client
        }
    };

    match final_rcon_player_check(final_rcon, child, control, config, state).await {
        FinalRconCheck::Zero => IdlePlayerCheck::Confirmed,
        FinalRconCheck::Veto(rcon) => {
            let retry_connection = rcon.is_none();
            let PlayerInspection::ConfiguredRcon {
                client,
                next_poll,
                zero_anchor,
            } = &mut state.player_inspection
            else {
                unreachable!("final RCON check requires configured player inspection");
            };
            *client = rcon;
            *zero_anchor = None;
            *next_poll = Instant::now()
                + if retry_connection {
                    config.retry_interval
                } else {
                    config.poll_interval
                };
            IdlePlayerCheck::Veto
        }
        FinalRconCheck::Shutdown => IdlePlayerCheck::Shutdown,
        FinalRconCheck::ChildWait(status) => IdlePlayerCheck::ChildWait(status),
    }
}

async fn attempt_expired_idle_stop(
    child: &mut OwnedProcess,
    control: &mut SupervisorControl,
    config: &SupervisorConfig,
    backend_use: &BackendUseCoordinator,
    state: &mut RunningMonitorState,
    anchor: Instant,
) -> RunningMonitorOutcome {
    let exclusive =
        match acquire_idle_exclusive(child, control, backend_use, config, state, anchor).await {
            IdleExclusiveOutcome::Acquired(guard) => guard,
            IdleExclusiveOutcome::ActivityChanged => return RunningMonitorOutcome::Continue,
            IdleExclusiveOutcome::Shutdown => return RunningMonitorOutcome::Shutdown,
            IdleExclusiveOutcome::ChildWait(status) => {
                return RunningMonitorOutcome::ChildWait(status);
            }
        };

    let activity = backend_use.activity_snapshot();
    let unchanged_anchor = state.effective_idle_anchor(activity);
    if activity.is_none_or(|snapshot| {
        snapshot.cycle() != state.cycle || snapshot.active_login_transfer_sessions() != 0
    }) || unchanged_anchor != Some(anchor)
        || Instant::now() < anchor + config.idle_timeout
        || control.current_lifecycle().owned_running_cycle() != Some(state.cycle)
    {
        return RunningMonitorOutcome::Continue;
    }
    if control.shutdown_pending() {
        return RunningMonitorOutcome::Shutdown;
    }
    match child.try_wait() {
        Ok(Some(status)) => {
            return RunningMonitorOutcome::ChildWait(Ok(status));
        }
        Ok(None) => {}
        Err(error) => {
            return RunningMonitorOutcome::ChildWait(Err(error));
        }
    }

    match confirm_idle_player_state(child, control, config, state).await {
        IdlePlayerCheck::Confirmed => {}
        IdlePlayerCheck::Veto => return RunningMonitorOutcome::Continue,
        IdlePlayerCheck::Shutdown => return RunningMonitorOutcome::Shutdown,
        IdlePlayerCheck::ChildWait(status) => {
            return RunningMonitorOutcome::ChildWait(status);
        }
    }

    if control.current_lifecycle().owned_running_cycle() != Some(state.cycle) {
        return RunningMonitorOutcome::Continue;
    }
    if control.shutdown_pending() {
        return RunningMonitorOutcome::Shutdown;
    }
    match child.try_wait() {
        Ok(Some(status)) => {
            return RunningMonitorOutcome::ChildWait(Ok(status));
        }
        Ok(None) => {}
        Err(error) => {
            return RunningMonitorOutcome::ChildWait(Err(error));
        }
    }

    let idle_for = Instant::now().saturating_duration_since(anchor);
    control.begin_idle_stop(state.cycle);
    if config.rcon.is_some() {
        server_info!(
            config,
            "Server proxy sessions and RCON player count have been idle for {idle_for:?}; stopping it"
        );
    } else {
        server_info!(
            config,
            "Server proxy sessions have been idle for {idle_for:?}; stopping it"
        );
    }
    drop(exclusive);
    RunningMonitorOutcome::Committed
}

async fn advance_running_monitor(
    child: &mut OwnedProcess,
    control: &mut SupervisorControl,
    config: &SupervisorConfig,
    backend_use: &BackendUseCoordinator,
    state: &mut RunningMonitorState,
) -> RunningMonitorOutcome {
    let activity_changed = backend_use.activity_notified();
    tokio::pin!(activity_changed);
    let effective_anchor = state.effective_idle_anchor(backend_use.activity_snapshot());
    if let Some(anchor) = effective_anchor
        && Instant::now() >= anchor + config.idle_timeout
    {
        return attempt_expired_idle_stop(child, control, config, backend_use, state, anchor).await;
    }

    let next_poll = match &state.player_inspection {
        PlayerInspection::ProxyOnly => None,
        PlayerInspection::ConfiguredRcon { next_poll, .. } => Some(*next_poll),
    };
    let stable_at = state.stable_at;
    let stability_pending = state.stability_pending;
    tokio::select! {
        biased;

        _ = &mut control.shutdown => RunningMonitorOutcome::Shutdown,
        request = control.wake_requests.recv() => {
            if request.is_none() {
                RunningMonitorOutcome::Shutdown
            } else {
                server_debug!(config, "Ignoring start request because the server is already running");
                RunningMonitorOutcome::Continue
            }
        }
        status = child.wait() => RunningMonitorOutcome::ChildWait(status),
        () = sleep_until(stable_at), if stability_pending => {
            match complete_stability_window(child, control, config, &mut state.stability_pending) {
                Ok(Some(status)) => RunningMonitorOutcome::ChildWait(Ok(status)),
                Ok(None) => RunningMonitorOutcome::Continue,
                Err(error) => RunningMonitorOutcome::ChildWait(Err(error)),
            }
        }
        () = &mut activity_changed => RunningMonitorOutcome::Continue,
        () = async {
            if let Some(next_poll) = next_poll {
                sleep_until(next_poll).await;
            } else {
                std::future::pending::<()>().await;
            }
        } => {
            poll_player_presence(&mut state.player_inspection, config).await;
            RunningMonitorOutcome::Continue
        }
        () = async {
            if let Some(anchor) = effective_anchor {
                sleep_until(anchor + config.idle_timeout).await;
            } else {
                std::future::pending::<()>().await;
            }
        } => RunningMonitorOutcome::Continue,
    }
}

async fn monitor_running_server(
    mut child: OwnedProcess,
    player_inspection: PlayerInspection,
    control: &mut SupervisorControl,
    config: &SupervisorConfig,
    backend_use: &BackendUseCoordinator,
    cycle: BackendCycle,
) -> Result<CycleOutcome> {
    let mut state = RunningMonitorState::new(cycle, player_inspection, control.current_lifecycle());

    loop {
        match advance_running_monitor(&mut child, control, config, backend_use, &mut state).await {
            RunningMonitorOutcome::Continue => {}
            RunningMonitorOutcome::Committed => {
                drop(state);
                return stop_committed_idle_server(child, control, config).await;
            }
            RunningMonitorOutcome::Shutdown => {
                drop(state);
                return stop_for_shutdown(child, control, config).await;
            }
            RunningMonitorOutcome::ChildWait(status) => {
                drop(state);
                return process_wait_outcome(
                    status,
                    child,
                    control,
                    config,
                    FailureCategory::ExitedUnexpectedly,
                )
                .await;
            }
        }
    }
}

async fn poll_player_presence(player_inspection: &mut PlayerInspection, config: &SupervisorConfig) {
    let PlayerInspection::ConfiguredRcon {
        client,
        next_poll,
        zero_anchor,
    } = player_inspection
    else {
        return;
    };
    let rcon_config = config
        .rcon
        .as_ref()
        .expect("configured RCON player state requires RCON configuration");
    let Some(rcon_client) = client.as_mut() else {
        *zero_anchor = None;
        match timeout(
            config.command_timeout,
            RconClient::connect(&rcon_config.endpoint, rcon_config.password.expose()),
        )
        .await
        {
            Ok(Ok(connected)) => {
                server_info!(config, "Reconnected to RCON");
                *client = Some(connected);
                *next_poll = Instant::now();
            }
            Ok(Err(error)) => {
                server_warn!(config, "RCON reconnection failed: {error:#}");
                *next_poll = Instant::now() + config.retry_interval;
            }
            Err(_) => {
                server_warn!(config, "RCON reconnection timed out");
                *next_poll = Instant::now() + config.retry_interval;
            }
        }
        return;
    };

    let response = match timeout(config.command_timeout, rcon_client.command("list")).await {
        Ok(Ok(response)) => response,
        Ok(Err(error)) => {
            server_warn!(
                config,
                "RCON player poll failed; idle shutdown is suspended: {error:#}"
            );
            *client = None;
            *zero_anchor = None;
            *next_poll = Instant::now() + config.retry_interval;
            return;
        }
        Err(_) => {
            server_warn!(
                config,
                "RCON player poll timed out; idle shutdown is suspended"
            );
            *client = None;
            *zero_anchor = None;
            *next_poll = Instant::now() + config.retry_interval;
            return;
        }
    };

    match observed_player_count(config, &response) {
        Some(0) => {
            let started = *zero_anchor.get_or_insert_with(Instant::now);
            let idle_for = Instant::now().saturating_duration_since(started);
            server_debug!(
                config,
                "RCON has continuously reported zero players for {idle_for:?}"
            );
        }
        Some(player_count) => {
            *zero_anchor = None;
            server_debug!(config, "Server has {player_count} online player(s)");
        }
        None => *zero_anchor = None,
    }
    *next_poll = Instant::now() + config.poll_interval;
}

fn reaped_failure(
    control: &SupervisorControl,
    config: &SupervisorConfig,
    failure: FailureCategory,
    status: ExitStatus,
) -> CycleOutcome {
    log_process_exit(config, status);
    let retry_at = control.record_reaped_failure(failure);
    CycleOutcome::Failed {
        retry_at,
        retry_pending: false,
    }
}

async fn process_wait_outcome(
    status: std::io::Result<ExitStatus>,
    child: OwnedProcess,
    control: &mut SupervisorControl,
    config: &SupervisorConfig,
    failure: FailureCategory,
) -> Result<CycleOutcome> {
    match status {
        Ok(status) => Ok(reaped_failure(control, config, failure, status)),
        Err(error) => reap_after_wait_error(child, control, config, error).await,
    }
}

async fn reap_after_wait_error(
    mut child: OwnedProcess,
    control: &mut SupervisorControl,
    config: &SupervisorConfig,
    wait_error: std::io::Error,
) -> Result<CycleOutcome> {
    control.set_phase(ServerPhase::Stopping);
    stop_and_reap(&mut child, config)
        .await
        .with_context(|| format!("cleanup failed after server process wait error: {wait_error}"))?;
    control.set_phase(ServerPhase::Stopped);
    Err(wait_error).context("failed to wait for the server process")
}

async fn stop_for_shutdown(
    mut child: OwnedProcess,
    control: &mut SupervisorControl,
    config: &SupervisorConfig,
) -> Result<CycleOutcome> {
    control.set_phase(ServerPhase::Stopping);
    stop_and_reap(&mut child, config).await?;
    Ok(CycleOutcome::Shutdown)
}

async fn stop_failed_server(
    mut child: OwnedProcess,
    control: &mut SupervisorControl,
    config: &SupervisorConfig,
    failure: FailureCategory,
) -> Result<CycleOutcome> {
    let retry_delay = control.begin_failure_cleanup(failure);
    let cleanup = stop_and_reap(&mut child, config);
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

async fn stop_committed_idle_server(
    mut child: OwnedProcess,
    control: &mut SupervisorControl,
    config: &SupervisorConfig,
) -> Result<CycleOutcome> {
    let stopping = control.current_lifecycle();
    assert_eq!(stopping.phase, ServerPhase::Stopping);
    assert_eq!(stopping.failure, None);
    assert_eq!(stopping.failure_streak, 0);
    let cleanup = stop_and_reap(&mut child, config);
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
                    server_debug!(config, "Ignoring stale server wake-up request while stopping");
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
            Ok(_) => server_debug!(
                config,
                "Ignoring stale server wake-up request while stopping"
            ),
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

async fn stop_and_reap(child: &mut OwnedProcess, config: &SupervisorConfig) -> Result<()> {
    let mut wait_error = None;
    let command_deadline = Instant::now() + config.command_timeout;
    let graceful_stop_sent = if let Some(rcon_config) = &config.rcon {
        let rcon_stop = async {
            let mut client =
                RconClient::connect(&rcon_config.endpoint, rcon_config.password.expose()).await?;
            client.stop().await
        };
        match timeout_at(command_deadline, rcon_stop).await {
            Ok(Ok(())) => true,
            Ok(Err(error)) => {
                server_warn!(
                    config,
                    "Failed to send the RCON stop command; trying the owned console: {error:#}"
                );
                send_console_stop_before(child, config, command_deadline).await
            }
            Err(_) => {
                server_warn!(config, "Sending the RCON stop command timed out");
                false
            }
        }
    } else {
        send_console_stop_before(child, config, command_deadline).await
    };

    if graceful_stop_sent {
        match timeout(config.shutdown_timeout, child.wait()).await {
            Ok(Ok(status)) => {
                log_process_exit(config, status);
                return Ok(());
            }
            Ok(Err(error)) => {
                server_warn!(config, "Failed while waiting for server shutdown: {error}");
                wait_error = Some(error);
            }
            Err(_) => server_warn!(
                config,
                "Server did not exit within {:?}; terminating it",
                config.shutdown_timeout
            ),
        }
    }

    let termination_result = process::terminate(child).await;
    match timeout(Duration::from_secs(5), child.wait()).await {
        Ok(Ok(status)) => log_process_exit(config, status),
        Ok(Err(error)) => return Err(error).context("failed to reap the server process"),
        Err(_) => {
            return Err(anyhow::anyhow!(
                "server process did not exit after termination"
            ));
        }
    }
    termination_result?;
    if let Some(error) = wait_error {
        return Err(error).context("failed while waiting for graceful server shutdown");
    }
    Ok(())
}

async fn send_console_stop_before(
    child: &mut OwnedProcess,
    config: &SupervisorConfig,
    deadline: Instant,
) -> bool {
    match timeout_at(deadline, child.send_console_stop()).await {
        Ok(Ok(())) => true,
        Ok(Err(error)) => {
            server_warn!(config, "Failed to send the console stop command: {error:#}");
            false
        }
        Err(_) => {
            server_warn!(config, "Sending the console stop command timed out");
            false
        }
    }
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

fn observed_player_count(config: &SupervisorConfig, response: &str) -> Option<u32> {
    server_debug!(config, "RCON list response: {response}");
    match parse_player_count(response) {
        Ok(count) => Some(count),
        Err(error) => {
            // Unknown is never treated as zero: false idle shutdowns are worse than leaving a
            // server running until the next valid observation.
            server_warn!(
                config,
                "Could not parse RCON player count; idle shutdown is suspended: {error:#}"
            );
            None
        }
    }
}

fn log_process_exit(config: &SupervisorConfig, status: ExitStatus) {
    if status.success() {
        server_info!(config, "Minecraft server exited successfully");
    } else {
        server_warn!(config, "Minecraft server exited with status {status}");
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

    #[derive(Debug)]
    struct TestRconPacket {
        id: i32,
        kind: i32,
        body: Vec<u8>,
    }

    enum TestListReply {
        Body(String),
        Close,
        Block { started: oneshot::Sender<()> },
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum TestRconEvent {
        List(usize),
        Stop,
        Closed,
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

    async fn spawn_scripted_list_rcon_server(
        replies: Vec<TestListReply>,
    ) -> (
        Endpoint,
        mpsc::UnboundedReceiver<TestRconEvent>,
        JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test RCON listener should bind");
        let address = endpoint(listener.local_addr().unwrap());
        let (events, event_receiver) = mpsc::unbounded_channel();
        let server = tokio::spawn(async move {
            let mut replies = std::collections::VecDeque::from(replies);
            let mut list_index = 0;
            'connections: loop {
                let (mut stream, _) = listener
                    .accept()
                    .await
                    .expect("test RCON client should connect");
                let auth = read_test_rcon_packet(&mut stream).await;
                assert_eq!(auth.kind, TEST_AUTH);
                assert_eq!(auth.body, b"secret");
                write_test_rcon_packet(&mut stream, auth.id, TEST_AUTH_RESPONSE, b"").await;

                loop {
                    let command = match try_read_test_rcon_packet(&mut stream).await {
                        Ok(command) => command,
                        Err(error)
                            if matches!(
                                error.kind(),
                                std::io::ErrorKind::UnexpectedEof
                                    | std::io::ErrorKind::ConnectionReset
                                    | std::io::ErrorKind::BrokenPipe
                            ) =>
                        {
                            let _ = events.send(TestRconEvent::Closed);
                            continue 'connections;
                        }
                        Err(error) => panic!("test RCON packet read failed: {error}"),
                    };
                    assert_eq!(command.kind, TEST_EXEC_COMMAND);
                    if command.body == b"stop" {
                        events.send(TestRconEvent::Stop).unwrap();
                        break 'connections;
                    }
                    assert_eq!(command.body, b"list");
                    let end_marker = read_test_rcon_packet(&mut stream).await;
                    assert_eq!(end_marker.kind, TEST_EXEC_COMMAND);
                    assert!(end_marker.body.is_empty());
                    list_index += 1;
                    let reply = replies
                        .pop_front()
                        .expect("test RCON server received an unexpected list command");
                    match reply {
                        TestListReply::Body(body) => {
                            write_test_rcon_packet(
                                &mut stream,
                                command.id,
                                TEST_RESPONSE_VALUE,
                                body.as_bytes(),
                            )
                            .await;
                            write_test_rcon_packet(
                                &mut stream,
                                end_marker.id,
                                TEST_RESPONSE_VALUE,
                                b"",
                            )
                            .await;
                            events.send(TestRconEvent::List(list_index)).unwrap();
                        }
                        TestListReply::Close => {
                            events.send(TestRconEvent::List(list_index)).unwrap();
                            stream.set_zero_linger().unwrap();
                            drop(stream);
                            let _ = events.send(TestRconEvent::Closed);
                            continue 'connections;
                        }
                        TestListReply::Block { started } => {
                            events.send(TestRconEvent::List(list_index)).unwrap();
                            started.send(()).unwrap();
                            let mut byte = [0];
                            match stream.read(&mut byte).await {
                                Ok(0) => {}
                                Err(error)
                                    if matches!(
                                        error.kind(),
                                        std::io::ErrorKind::ConnectionReset
                                            | std::io::ErrorKind::BrokenPipe
                                    ) => {}
                                Ok(received) => panic!(
                                    "canceled final RCON client was reused and sent {received} byte(s)"
                                ),
                                Err(error) => panic!("blocked RCON client read failed: {error}"),
                            }
                            let _ = events.send(TestRconEvent::Closed);
                            continue 'connections;
                        }
                    }
                }
            }
        });
        (address, event_receiver, server)
    }

    async fn spawn_test_rcon_server() -> (Endpoint, oneshot::Receiver<()>, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test RCON listener should bind");
        let address = endpoint(
            listener
                .local_addr()
                .expect("test RCON listener should have an address"),
        );
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
    ) -> (Endpoint, oneshot::Receiver<()>, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test RCON listener should bind");
        let address = endpoint(
            listener
                .local_addr()
                .expect("test RCON listener should have an address"),
        );
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
        Endpoint,
        oneshot::Receiver<()>,
        oneshot::Sender<()>,
        JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test RCON listener should bind");
        let address = endpoint(
            listener
                .local_addr()
                .expect("test RCON listener should have an address"),
        );
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

    fn endpoint(address: std::net::SocketAddr) -> Endpoint {
        Endpoint::new(address.ip().to_string(), address.port())
    }

    fn test_config(rcon_endpoint: &Endpoint) -> SupervisorConfig {
        let executable = std::env::current_exe().expect("test executable path should be known");
        SupervisorConfig {
            context: ServerContext::new("test".to_owned()),
            launch: LaunchCommand::new(
                executable.into_os_string(),
                vec![
                    "--ignored".into(),
                    "--exact".into(),
                    "supervisor::tests::server_process_fixture".into(),
                    "--quiet".into(),
                ],
                std::env::current_dir().expect("test working directory should be known"),
            ),
            rcon: Some(RconConfig {
                endpoint: rcon_endpoint.clone(),
                password: RconSecret::new("secret".to_owned()).unwrap(),
            }),
            poll_interval: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(30),
            startup_timeout: Duration::from_secs(30),
            retry_interval: Duration::from_millis(10),
            command_timeout: Duration::from_secs(1),
            shutdown_timeout: Duration::from_millis(250),
        }
    }

    fn no_rcon_config() -> SupervisorConfig {
        let mut config = test_config(&Endpoint::new("127.0.0.1", 9));
        config.rcon = None;
        config.launch.arguments[2] = "supervisor::tests::console_stop_fixture".into();
        config
    }

    fn player_count_reply(player_count: u32) -> TestListReply {
        TestListReply::Body(format!(
            "There are {player_count} of a max of 20 players online:"
        ))
    }

    fn launch_failure_config() -> SupervisorConfig {
        SupervisorConfig {
            context: ServerContext::new("test".to_owned()),
            launch: LaunchCommand::new(
                "mcservernap-test-command-that-does-not-exist".into(),
                Vec::new(),
                std::env::current_dir().expect("test working directory should be known"),
            ),
            rcon: Some(RconConfig {
                endpoint: Endpoint::new("127.0.0.1", 9),
                password: RconSecret::new("secret".to_owned()).unwrap(),
            }),
            poll_interval: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(30),
            startup_timeout: Duration::from_secs(30),
            retry_interval: Duration::from_millis(10),
            command_timeout: Duration::from_secs(1),
            shutdown_timeout: Duration::from_millis(250),
        }
    }

    fn test_reconciliation(
        rcon: RconProbeClassification,
        backend: BackendProbeClassification,
    ) -> ReconciliationResult {
        ReconciliationResult::ConfiguredRcon {
            rcon: rcon::RconProbeResult {
                classification: rcon,
                detail: None,
            },
            backend: BackendProbeResult {
                classification: backend,
                detail: None,
            },
        }
    }

    fn scripted_backend() -> (mpsc::UnboundedSender<ReconciliationResult>, BackendEndpoint) {
        let (probe_sender, probe_receiver) = mpsc::unbounded_channel();
        (
            probe_sender,
            BackendEndpoint {
                endpoint: Endpoint::new("127.0.0.1", 9),
                connect_timeout: Duration::from_secs(1),
                scripted_probes: Some(tokio::sync::Mutex::new(probe_receiver)),
            },
        )
    }

    fn refused_endpoints() -> (Endpoint, BackendEndpoint) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")
            .expect("test should reserve an unused local port");
        let address = listener
            .local_addr()
            .expect("test listener should have an address");
        drop(listener);
        let (probe_sender, probe_receiver) = mpsc::unbounded_channel();
        for _ in 0..32 {
            probe_sender
                .send(test_reconciliation(
                    RconProbeClassification::Refused,
                    BackendProbeClassification::Refused,
                ))
                .expect("test probe script should accept a clean result");
        }
        drop(probe_sender);
        let endpoint = endpoint(address);
        (
            endpoint.clone(),
            BackendEndpoint {
                endpoint,
                connect_timeout: Duration::from_secs(1),
                scripted_probes: Some(tokio::sync::Mutex::new(probe_receiver)),
            },
        )
    }

    fn rapid_exit_config(rcon_endpoint: &Endpoint) -> SupervisorConfig {
        let mut config = test_config(rcon_endpoint);
        config.launch.arguments[2] = "supervisor::tests::rapid_exit_fixture".into();
        config
    }

    fn stable_exit_config(rcon_endpoint: &Endpoint) -> SupervisorConfig {
        let mut config = test_config(rcon_endpoint);
        config.launch.arguments[2] = "supervisor::tests::stable_exit_fixture".into();
        config
    }

    fn signaled_exit_config(
        rcon_endpoint: &Endpoint,
        exit_signal_address: std::net::SocketAddr,
    ) -> SupervisorConfig {
        let mut config = test_config(rcon_endpoint);
        config.launch.arguments[2] = "supervisor::tests::signaled_exit_fixture".into();
        config
            .launch
            .arguments
            .insert(3, format!("exit-signal={exit_signal_address}").into());
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

    fn running_lifecycle(failure_streak: u32) -> LifecycleState {
        LifecycleState {
            phase: ServerPhase::Running,
            launch_generation: 1,
            failure: (failure_streak > 0).then_some(FailureCategory::ExitedUnexpectedly),
            failure_streak,
            ..LifecycleState::stopped()
        }
    }

    fn test_backend_use() -> Arc<BackendUseCoordinator> {
        Arc::new(BackendUseCoordinator::new())
    }

    fn test_backend_endpoint() -> BackendEndpoint {
        BackendEndpoint::network(Endpoint::new("127.0.0.1", 9), Duration::from_secs(1))
    }

    fn test_control(
        initial: LifecycleState,
    ) -> (
        SupervisorControl,
        mpsc::Sender<WakeRequest>,
        oneshot::Sender<()>,
        watch::Receiver<LifecycleState>,
    ) {
        let (wake_sender, wake_requests) = mpsc::channel(1);
        let (shutdown_sender, shutdown) = oneshot::channel();
        let (lifecycle, lifecycle_receiver) = watch::channel(initial);
        (
            SupervisorControl {
                wake_requests,
                shutdown,
                lifecycle,
            },
            wake_sender,
            shutdown_sender,
            lifecycle_receiver,
        )
    }

    struct TestSupervisor {
        wake_sender: mpsc::Sender<WakeRequest>,
        shutdown_sender: oneshot::Sender<()>,
        lifecycle: watch::Receiver<LifecycleState>,
        task: JoinHandle<Result<()>>,
    }

    impl TestSupervisor {
        fn spawn(config: SupervisorConfig, backend: BackendEndpoint) -> Self {
            let (wake_sender, wake_receiver) = mpsc::channel(1);
            let (shutdown_sender, shutdown_receiver) = oneshot::channel();
            let (lifecycle_sender, lifecycle) = watch::channel(LifecycleState::reconciling());
            let task = tokio::spawn(run(
                wake_receiver,
                shutdown_receiver,
                lifecycle_sender,
                config,
                backend,
                test_backend_use(),
            ));
            Self {
                wake_sender,
                shutdown_sender,
                lifecycle,
                task,
            }
        }

        async fn shutdown(self) -> watch::Receiver<LifecycleState> {
            self.shutdown_sender
                .send(())
                .expect("supervisor should still await shutdown");
            self.task
                .await
                .expect("supervisor task should not panic")
                .expect("supervisor shutdown should succeed");
            self.lifecycle
        }
    }

    fn running_backend_use(lifecycle: LifecycleState) -> Arc<BackendUseCoordinator> {
        let backend_use = test_backend_use();
        let cycle = lifecycle
            .owned_running_cycle()
            .expect("test lifecycle should represent an owned running backend");
        backend_use.begin_cycle(cycle, lifecycle.phase_started_at);
        backend_use
    }

    async fn connected_player_inspection(rcon_endpoint: &Endpoint) -> PlayerInspection {
        PlayerInspection::ConfiguredRcon {
            client: Some(RconClient::connect(rcon_endpoint, "secret").await.unwrap()),
            next_poll: Instant::now(),
            zero_anchor: None,
        }
    }

    async fn assert_final_rcon_veto(reply: TestListReply) {
        let (rcon_address, mut events, rcon_server) =
            spawn_scripted_list_rcon_server(vec![player_count_reply(0), reply]).await;
        let mut config = test_config(&rcon_address);
        config.idle_timeout = Duration::from_secs(1);
        config.poll_interval = Duration::from_secs(100);
        let child = process::launch(&config.launch).expect("test server process should launch");
        let (wake_sender, wake_receiver) = mpsc::channel(1);
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let running = running_lifecycle(0);
        let (lifecycle_sender, lifecycle) = watch::channel(running);
        let mut control = SupervisorControl {
            wake_requests: wake_receiver,
            shutdown: shutdown_receiver,
            lifecycle: lifecycle_sender,
        };
        let backend_use = running_backend_use(running);
        let cycle = running.owned_running_cycle().unwrap();
        let player_inspection = connected_player_inspection(&rcon_address).await;
        let monitor_backend_use = Arc::clone(&backend_use);
        let monitor = tokio::spawn(async move {
            monitor_running_server(
                child,
                player_inspection,
                &mut control,
                &config,
                &monitor_backend_use,
                cycle,
            )
            .await
        });

        assert_eq!(events.recv().await, Some(TestRconEvent::List(1)));
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(events.recv().await, Some(TestRconEvent::List(2)));
        let observation = backend_use.acquire_exclusive().await;
        assert_eq!(lifecycle.borrow().phase, ServerPhase::Running);
        drop(observation);

        tokio::time::resume();
        shutdown_sender
            .send(())
            .expect("running monitor should still await shutdown");
        let outcome = monitor
            .await
            .expect("running monitor task should not panic")
            .expect("shutdown after a final RCON veto should succeed");
        assert_eq!(outcome, CycleOutcome::Shutdown);
        drop(wake_sender);
        assert_eq!(events.recv().await, Some(TestRconEvent::Closed));
        assert_eq!(events.recv().await, Some(TestRconEvent::Stop));
        rcon_server
            .await
            .expect("test RCON server should not panic");
    }

    fn spawn_launch_failure_supervisor() -> TestSupervisor {
        let (rcon_address, backend) = refused_endpoints();
        let mut config = launch_failure_config();
        config.rcon.as_mut().unwrap().endpoint = rcon_address;
        TestSupervisor::spawn(config, backend)
    }

    async fn wait_for_lifecycle(
        lifecycle: &mut watch::Receiver<LifecycleState>,
        predicate: impl Fn(&LifecycleState) -> bool,
    ) {
        let outcome = timeout(Duration::from_secs(5), lifecycle.wait_for(predicate))
            .await
            .map(|result| result.map(|_| ()));
        match outcome {
            Ok(Ok(())) => {}
            Ok(Err(_)) => panic!("supervisor closed the lifecycle source"),
            Err(_) => {
                let current = *lifecycle.borrow();
                panic!("lifecycle wait timed out in {current:?}");
            }
        }
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
    #[ignore = "launched as a console-controlled supervisor test server process"]
    fn console_stop_fixture() {
        let mut command = [0; 5];
        std::io::Read::read_exact(&mut std::io::stdin(), &mut command)
            .expect("test process should receive a console stop command");
        assert_eq!(&command, b"stop\n");
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
    fn failure_backoff_caps_at_sixty_seconds() {
        let expected = [5, 10, 20, 40, 60, 60, 60];
        for (failure_streak, expected_seconds) in (1..).zip(expected) {
            assert_eq!(
                failure_backoff(failure_streak),
                Duration::from_secs(expected_seconds)
            );
        }
    }

    #[test]
    fn reconciliation_requires_double_refusal_to_be_clean() {
        use BackendProbeClassification::{Connected, Inconclusive as BackendInconclusive, Refused};
        use RconProbeClassification::{
            AcceptedFailure, Authenticated, Inconclusive as RconInconclusive,
            Refused as RconRefused,
        };

        let cases = [
            (
                Authenticated,
                Connected,
                ReconciliationClassification::FullyReady,
            ),
            (RconRefused, Refused, ReconciliationClassification::Clean),
            (
                AcceptedFailure,
                Refused,
                ReconciliationClassification::Positive,
            ),
            (
                Authenticated,
                Refused,
                ReconciliationClassification::Positive,
            ),
            (
                RconRefused,
                Connected,
                ReconciliationClassification::Positive,
            ),
            (
                RconInconclusive,
                Refused,
                ReconciliationClassification::Inconclusive,
            ),
            (
                RconRefused,
                BackendInconclusive,
                ReconciliationClassification::Inconclusive,
            ),
        ];

        for (rcon, backend, expected) in cases {
            assert_eq!(
                test_reconciliation(rcon, backend).classification(),
                expected
            );
        }
    }

    #[test]
    fn backend_only_reconciliation_is_exact() {
        let cases = [
            (
                BackendProbeClassification::Connected,
                ReconciliationClassification::FullyReady,
            ),
            (
                BackendProbeClassification::Refused,
                ReconciliationClassification::Clean,
            ),
            (
                BackendProbeClassification::Inconclusive,
                ReconciliationClassification::Inconclusive,
            ),
        ];

        for (backend, expected) in cases {
            assert_eq!(
                ReconciliationResult::BackendOnly(BackendProbeResult {
                    classification: backend,
                    detail: None,
                })
                .classification(),
                expected
            );
        }
    }

    #[test]
    fn transitions_preserve_evidence_invariants() {
        let (control, _wake_sender, _shutdown_sender, lifecycle) =
            test_control(LifecycleState::test_phase(ServerPhase::Conflict));

        assert_eq!(
            control.begin_reconciliation(),
            ReconciliationEvidence::Inconclusive
        );
        control.publish_stopped_after_clean_reconciliation();
        assert_eq!(
            lifecycle.borrow().reconciliation_evidence,
            ReconciliationEvidence::None
        );
        control.begin_reconciliation();
        control.record_launch_success();
        assert_eq!(
            lifecycle.borrow().reconciliation_evidence,
            ReconciliationEvidence::None
        );
        control.set_phase(ServerPhase::Stopped);
        control.begin_reconciliation();
        control.publish_external(false);
        assert_eq!(lifecycle.borrow().phase, ServerPhase::External);
        assert_eq!(
            lifecycle.borrow().reconciliation_evidence,
            ReconciliationEvidence::Positive
        );
        control.publish_conflict(ReconciliationEvidence::Positive, false);
        assert!(lifecycle.borrow().invariant_holds());
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                control.set_phase(ServerPhase::Stopped);
            }))
            .is_err()
        );
    }

    #[test]
    fn reconciliation_preserves_cooldown_metadata() {
        let retry_at = Instant::now() + Duration::from_secs(5);
        let mut cooldown = failed_lifecycle(ServerPhase::Cooldown, 3);
        cooldown.retry_at = Some(retry_at);
        let (control, _wake_sender, _shutdown_sender, lifecycle) = test_control(cooldown);

        control.begin_reconciliation();
        let reconciling = *lifecycle.borrow();
        assert_eq!(reconciling.retry_at, Some(retry_at));
        assert_eq!(reconciling.failure_streak, 3);
        assert_eq!(
            reconciling.failure,
            Some(FailureCategory::ExitedUnexpectedly)
        );
    }

    #[tokio::test]
    async fn startup_demand_requires_a_distinct_final_probe() {
        let (probe_sender, backend) = scripted_backend();
        let (wake_sender, wake_receiver) = mpsc::channel(1);
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let initial = LifecycleState::reconciling();
        let (lifecycle_sender, mut lifecycle) = watch::channel(initial);
        wake_sender
            .try_send(WakeRequest::observed(initial))
            .expect("startup wake should fit");
        probe_sender
            .send(test_reconciliation(
                RconProbeClassification::Refused,
                BackendProbeClassification::Refused,
            ))
            .expect("startup clean result should queue");
        let supervisor = tokio::spawn(run(
            wake_receiver,
            shutdown_receiver,
            lifecycle_sender,
            launch_failure_config(),
            backend,
            test_backend_use(),
        ));

        wait_for_lifecycle(&mut lifecycle, |state| {
            state.phase == ServerPhase::Reconciling
                && state.phase_started_at != initial.phase_started_at
        })
        .await;
        assert_eq!(lifecycle.borrow().launch_generation, 0);
        assert_eq!(lifecycle.borrow().failure_streak, 0);

        probe_sender
            .send(test_reconciliation(
                RconProbeClassification::Refused,
                BackendProbeClassification::Refused,
            ))
            .expect("final clean result should queue");
        wait_for_phase(&mut lifecycle, ServerPhase::Cooldown).await;
        assert_eq!(lifecycle.borrow().launch_generation, 1);
        shutdown_sender
            .send(())
            .expect("supervisor should still await shutdown");
        supervisor
            .await
            .expect("supervisor task should not panic")
            .expect("supervisor shutdown should succeed");
    }

    #[tokio::test]
    async fn blocked_startup_requires_new_demand() {
        let (probe_sender, backend) = scripted_backend();
        let (wake_sender, wake_receiver) = mpsc::channel(1);
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let initial = LifecycleState::reconciling();
        let (lifecycle_sender, mut lifecycle) = watch::channel(initial);
        wake_sender
            .try_send(WakeRequest::observed(initial))
            .expect("startup wake should fit");
        let supervisor = tokio::spawn(run(
            wake_receiver,
            shutdown_receiver,
            lifecycle_sender,
            launch_failure_config(),
            backend,
            test_backend_use(),
        ));
        let startup_slot = wake_sender
            .reserve()
            .await
            .expect("supervisor should accept startup demand");
        drop(startup_slot);
        probe_sender
            .send(test_reconciliation(
                RconProbeClassification::Inconclusive,
                BackendProbeClassification::Refused,
            ))
            .expect("startup inconclusive result should queue");
        probe_sender
            .send(test_reconciliation(
                RconProbeClassification::Refused,
                BackendProbeClassification::Refused,
            ))
            .expect("later clean result should queue");

        wait_for_phase(&mut lifecycle, ServerPhase::Conflict).await;
        assert_eq!(lifecycle.borrow().launch_generation, 1);
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        assert_eq!(lifecycle.borrow().phase, ServerPhase::Conflict);
        assert_eq!(lifecycle.borrow().failure, None);

        wake_sender
            .send(WakeRequest::observed(*lifecycle.borrow()))
            .await
            .expect("later conflict wake should queue");
        wait_for_phase(&mut lifecycle, ServerPhase::Cooldown).await;
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
    async fn stale_startup_wake_precedes_valid_conflict_wake() {
        let (probe_sender, backend) = scripted_backend();
        probe_sender
            .send(test_reconciliation(
                RconProbeClassification::Inconclusive,
                BackendProbeClassification::Refused,
            ))
            .expect("startup inconclusive result should queue");
        probe_sender
            .send(test_reconciliation(
                RconProbeClassification::Refused,
                BackendProbeClassification::Refused,
            ))
            .expect("conflict clean result should queue");
        let mut supervisor = TestSupervisor::spawn(launch_failure_config(), backend);
        let initial = *supervisor.lifecycle.borrow();
        wait_for_phase(&mut supervisor.lifecycle, ServerPhase::Conflict).await;

        supervisor
            .wake_sender
            .send(WakeRequest::observed(initial))
            .await
            .expect("stale startup wake should queue");
        let permit = supervisor
            .wake_sender
            .reserve()
            .await
            .expect("supervisor should drain the stale startup wake");
        permit.send(WakeRequest::observed(*supervisor.lifecycle.borrow()));
        wait_for_phase(&mut supervisor.lifecycle, ServerPhase::Cooldown).await;
        assert_eq!(supervisor.lifecycle.borrow().launch_generation, 1);
        supervisor.shutdown().await;
    }

    #[tokio::test]
    async fn blocked_final_gate_consumes_one_generation() {
        let (probe_sender, backend) = scripted_backend();
        for result in [
            test_reconciliation(
                RconProbeClassification::Refused,
                BackendProbeClassification::Refused,
            ),
            test_reconciliation(
                RconProbeClassification::AcceptedFailure,
                BackendProbeClassification::Refused,
            ),
        ] {
            probe_sender
                .send(result)
                .expect("probe result should queue");
        }
        let mut supervisor = TestSupervisor::spawn(launch_failure_config(), backend);
        wait_for_phase(&mut supervisor.lifecycle, ServerPhase::Stopped).await;
        supervisor
            .wake_sender
            .send(WakeRequest::observed(*supervisor.lifecycle.borrow()))
            .await
            .expect("stopped wake should queue");
        wait_for_phase(&mut supervisor.lifecycle, ServerPhase::Conflict).await;

        assert_eq!(supervisor.lifecycle.borrow().launch_generation, 1);
        assert_eq!(supervisor.lifecycle.borrow().failure, None);
        assert_eq!(supervisor.lifecycle.borrow().failure_streak, 0);
        supervisor.shutdown().await;
    }

    #[tokio::test]
    async fn positive_evidence_survives_double_refusal() {
        let (probe_sender, backend) = scripted_backend();
        for result in [
            test_reconciliation(
                RconProbeClassification::AcceptedFailure,
                BackendProbeClassification::Refused,
            ),
            test_reconciliation(
                RconProbeClassification::Refused,
                BackendProbeClassification::Refused,
            ),
            test_reconciliation(
                RconProbeClassification::Authenticated,
                BackendProbeClassification::Connected,
            ),
        ] {
            probe_sender
                .send(result)
                .expect("probe result should queue");
        }
        let mut supervisor = TestSupervisor::spawn(launch_failure_config(), backend);
        wait_for_phase(&mut supervisor.lifecycle, ServerPhase::Conflict).await;
        assert_eq!(
            supervisor.lifecycle.borrow().reconciliation_evidence,
            ReconciliationEvidence::Positive
        );
        supervisor
            .wake_sender
            .send(WakeRequest::observed(*supervisor.lifecycle.borrow()))
            .await
            .expect("conflict wake should queue");
        wait_for_lifecycle(&mut supervisor.lifecycle, |state| {
            state.launch_generation == 1
        })
        .await;

        assert_eq!(supervisor.lifecycle.borrow().phase, ServerPhase::Conflict);
        assert_eq!(
            supervisor.lifecycle.borrow().reconciliation_evidence,
            ReconciliationEvidence::Positive
        );
        assert_eq!(supervisor.lifecycle.borrow().failure, None);
        supervisor
            .wake_sender
            .send(WakeRequest::observed(*supervisor.lifecycle.borrow()))
            .await
            .expect("sticky conflict wake should queue");
        wait_for_phase(&mut supervisor.lifecycle, ServerPhase::External).await;
        assert_eq!(supervisor.lifecycle.borrow().launch_generation, 2);
        supervisor.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn stale_external_wake_waits_for_poll_interval() {
        let (probe_sender, backend) = scripted_backend();
        probe_sender
            .send(test_reconciliation(
                RconProbeClassification::Authenticated,
                BackendProbeClassification::Connected,
            ))
            .expect("startup ready result should queue");
        let config = launch_failure_config();
        let poll_interval = config.poll_interval;
        let mut supervisor = TestSupervisor::spawn(config, backend);
        let initial = *supervisor.lifecycle.borrow();
        wait_for_phase(&mut supervisor.lifecycle, ServerPhase::External).await;

        supervisor
            .wake_sender
            .send(WakeRequest::observed(initial))
            .await
            .expect("stale startup wake should queue");
        let permit = supervisor
            .wake_sender
            .reserve()
            .await
            .expect("external monitor should drain the stale wake");
        drop(permit);
        probe_sender
            .send(test_reconciliation(
                RconProbeClassification::Refused,
                BackendProbeClassification::Refused,
            ))
            .expect("periodic probe result should queue");
        tokio::task::yield_now().await;
        assert_eq!(supervisor.lifecycle.borrow().phase, ServerPhase::External);

        tokio::time::advance(
            poll_interval
                .checked_sub(Duration::from_secs(1))
                .expect("poll interval should exceed one second"),
        )
        .await;
        tokio::task::yield_now().await;
        assert_eq!(supervisor.lifecycle.borrow().phase, ServerPhase::External);
        tokio::time::advance(Duration::from_secs(1)).await;
        wait_for_phase(&mut supervisor.lifecycle, ServerPhase::Conflict).await;

        supervisor.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn external_poll_loss_enters_sticky_conflict() {
        let (probe_sender, backend) = scripted_backend();
        probe_sender
            .send(test_reconciliation(
                RconProbeClassification::Authenticated,
                BackendProbeClassification::Connected,
            ))
            .expect("startup ready result should queue");
        let mut supervisor = TestSupervisor::spawn(launch_failure_config(), backend);
        wait_for_phase(&mut supervisor.lifecycle, ServerPhase::External).await;
        tokio::time::advance(Duration::from_secs(29)).await;
        assert_eq!(supervisor.lifecycle.borrow().phase, ServerPhase::External);
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(supervisor.lifecycle.borrow().phase, ServerPhase::External);
        probe_sender
            .send(test_reconciliation(
                RconProbeClassification::Refused,
                BackendProbeClassification::Refused,
            ))
            .expect("lost external result should queue");
        wait_for_phase(&mut supervisor.lifecycle, ServerPhase::Conflict).await;

        assert_eq!(supervisor.lifecycle.borrow().launch_generation, 0);
        assert_eq!(supervisor.lifecycle.borrow().failure, None);
        assert_eq!(
            supervisor.lifecycle.borrow().reconciliation_evidence,
            ReconciliationEvidence::Positive
        );
        supervisor.shutdown().await;
    }

    #[tokio::test]
    async fn external_shutdown_sends_no_rcon_stop_and_launches_no_process() {
        let rcon_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test RCON listener should bind");
        let rcon_address = endpoint(
            rcon_listener
                .local_addr()
                .expect("RCON listener should have an address"),
        );
        let rcon_server = tokio::spawn(async move {
            let (mut stream, _) = rcon_listener
                .accept()
                .await
                .expect("reconciliation should connect to RCON");
            let auth = read_test_rcon_packet(&mut stream).await;
            assert_eq!(auth.kind, TEST_AUTH);
            write_test_rcon_packet(&mut stream, auth.id, TEST_AUTH_RESPONSE, b"").await;
            let error = try_read_test_rcon_packet(&mut stream)
                .await
                .expect_err("external shutdown must not send an RCON command");
            assert!(matches!(
                error.kind(),
                ErrorKind::UnexpectedEof
                    | ErrorKind::ConnectionReset
                    | ErrorKind::ConnectionAborted
            ));
        });

        let backend_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test backend listener should bind");
        let backend_address = backend_listener
            .local_addr()
            .expect("backend listener should have an address");
        let backend_server = tokio::spawn(async move {
            let _ = backend_listener
                .accept()
                .await
                .expect("reconciliation should connect to the backend");
        });

        let (wake_sender, wake_receiver) = mpsc::channel(1);
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let (lifecycle_sender, mut lifecycle) = watch::channel(LifecycleState::reconciling());
        let mut config = launch_failure_config();
        config.rcon.as_mut().unwrap().endpoint = rcon_address;
        let supervisor = tokio::spawn(run(
            wake_receiver,
            shutdown_receiver,
            lifecycle_sender,
            config,
            BackendEndpoint::network(
                Endpoint::new("127.0.0.1", backend_address.port()),
                Duration::from_secs(1),
            ),
            test_backend_use(),
        ));
        wait_for_phase(&mut lifecycle, ServerPhase::External).await;
        assert_eq!(lifecycle.borrow().launch_generation, 0);
        assert_eq!(lifecycle.borrow().failure, None);

        shutdown_sender
            .send(())
            .expect("supervisor should still await shutdown");
        drop(wake_sender);
        supervisor
            .await
            .expect("supervisor task should not panic")
            .expect("external shutdown should succeed");
        rcon_server
            .await
            .expect("test RCON server should not panic");
        backend_server
            .await
            .expect("test backend server should not panic");
    }

    #[tokio::test]
    async fn shutdown_during_reconciliation_prevents_launch() {
        for during_final in [false, true] {
            let (probe_sender, backend) = scripted_backend();
            if during_final {
                probe_sender
                    .send(test_reconciliation(
                        RconProbeClassification::Refused,
                        BackendProbeClassification::Refused,
                    ))
                    .expect("startup clean result should queue");
            }
            let mut supervisor = TestSupervisor::spawn(launch_failure_config(), backend);
            if during_final {
                wait_for_phase(&mut supervisor.lifecycle, ServerPhase::Stopped).await;
                supervisor
                    .wake_sender
                    .send(WakeRequest::observed(*supervisor.lifecycle.borrow()))
                    .await
                    .expect("stopped wake should queue");
                wait_for_phase(&mut supervisor.lifecycle, ServerPhase::Reconciling).await;
            } else {
                tokio::task::yield_now().await;
            }
            let lifecycle = supervisor.shutdown().await;
            assert_eq!(lifecycle.borrow().launch_generation, 0);
            assert_eq!(lifecycle.borrow().failure, None);
        }
    }

    #[test]
    fn conflict_warning_key_changes_only_with_classification() {
        let inconclusive = test_reconciliation(
            RconProbeClassification::Inconclusive,
            BackendProbeClassification::Refused,
        )
        .conflict_classification(false);
        let positive = test_reconciliation(
            RconProbeClassification::AcceptedFailure,
            BackendProbeClassification::Refused,
        )
        .conflict_classification(true);
        let mut last = None;

        assert!(conflict_warning_changed(&mut last, inconclusive));
        assert!(!conflict_warning_changed(&mut last, inconclusive));
        assert!(conflict_warning_changed(&mut last, positive));
    }

    #[tokio::test(start_paused = true)]
    async fn launch_failure_waits_for_new_demand() {
        let mut supervisor = spawn_launch_failure_supervisor();
        supervisor
            .wake_sender
            .send(WakeRequest::observed(*supervisor.lifecycle.borrow()))
            .await
            .expect("supervisor should accept the initial wake request");
        wait_for_phase(&mut supervisor.lifecycle, ServerPhase::Cooldown).await;

        let failure = *supervisor.lifecycle.borrow();
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
        wait_for_phase(&mut supervisor.lifecycle, ServerPhase::Stopped).await;
        assert_eq!(supervisor.lifecycle.borrow().launch_generation, 1);
        assert_eq!(supervisor.lifecycle.borrow().failure_streak, 1);

        supervisor.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn retry_demand_latches_once() {
        let mut supervisor = spawn_launch_failure_supervisor();
        supervisor
            .wake_sender
            .send(WakeRequest::observed(*supervisor.lifecycle.borrow()))
            .await
            .expect("supervisor should accept the initial wake request");
        wait_for_phase(&mut supervisor.lifecycle, ServerPhase::Cooldown).await;
        let first_failure = *supervisor.lifecycle.borrow();

        supervisor
            .wake_sender
            .send(WakeRequest::observed(first_failure))
            .await
            .expect("one post-failure wake should latch");
        let permit = supervisor
            .wake_sender
            .reserve()
            .await
            .expect("supervisor should consume the latched retry request");
        permit.send(WakeRequest::observed(first_failure));
        for _ in 0..16 {
            assert!(matches!(
                supervisor
                    .wake_sender
                    .try_send(WakeRequest::observed(first_failure)),
                Err(mpsc::error::TrySendError::Full(_))
            ));
        }

        tokio::time::advance(Duration::from_secs(5)).await;
        wait_for_lifecycle(&mut supervisor.lifecycle, |state| state.failure_streak == 2).await;
        assert_eq!(supervisor.lifecycle.borrow().phase, ServerPhase::Cooldown);
        assert_eq!(supervisor.lifecycle.borrow().launch_generation, 2);
        assert_eq!(
            supervisor.lifecycle.borrow().failure,
            Some(FailureCategory::LaunchFailed)
        );

        tokio::time::advance(Duration::from_secs(10)).await;
        wait_for_phase(&mut supervisor.lifecycle, ServerPhase::Stopped).await;
        assert_eq!(supervisor.lifecycle.borrow().launch_generation, 2);

        supervisor.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn wake_after_cooldown_launches_immediately() {
        let mut supervisor = spawn_launch_failure_supervisor();
        supervisor
            .wake_sender
            .send(WakeRequest::observed(*supervisor.lifecycle.borrow()))
            .await
            .expect("supervisor should accept the initial wake request");
        wait_for_phase(&mut supervisor.lifecycle, ServerPhase::Cooldown).await;

        tokio::time::advance(Duration::from_secs(5)).await;
        wait_for_phase(&mut supervisor.lifecycle, ServerPhase::Stopped).await;
        let expired = *supervisor.lifecycle.borrow();
        supervisor
            .wake_sender
            .send(WakeRequest::observed(expired))
            .await
            .expect("supervisor should accept later demand");
        wait_for_lifecycle(&mut supervisor.lifecycle, |state| state.failure_streak == 2).await;

        let failure = *supervisor.lifecycle.borrow();
        assert_eq!(failure.phase, ServerPhase::Cooldown);
        assert_eq!(failure.launch_generation, 2);
        assert_eq!(
            failure
                .retry_at
                .expect("second failure should have a deadline")
                - failure.phase_started_at,
            Duration::from_secs(10)
        );

        supervisor.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_interrupts_failure_cooldown() {
        let mut supervisor = spawn_launch_failure_supervisor();
        supervisor
            .wake_sender
            .send(WakeRequest::observed(*supervisor.lifecycle.borrow()))
            .await
            .expect("supervisor should accept the initial wake request");
        wait_for_phase(&mut supervisor.lifecycle, ServerPhase::Cooldown).await;

        let lifecycle = supervisor.shutdown().await;
        assert_eq!(lifecycle.borrow().phase, ServerPhase::Stopped);
        assert_eq!(lifecycle.borrow().launch_generation, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn wake_channel_closure_interrupts_latched_cooldown() {
        let retry_at = Instant::now() + Duration::from_secs(60);
        let mut cooldown = failed_lifecycle(ServerPhase::Cooldown, 5);
        cooldown.retry_at = Some(retry_at);
        let (mut control, wake_sender, shutdown_sender, lifecycle) = test_control(cooldown);
        let (started_sender, started_receiver) = oneshot::channel();
        let waiting = tokio::spawn(async move {
            started_sender
                .send(())
                .expect("test should still await cooldown startup");
            control.wait_for_cooldown(retry_at, true).await
        });

        started_receiver
            .await
            .expect("cooldown task should report startup");
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        drop(wake_sender);

        assert_eq!(
            waiting.await.expect("cooldown task should not panic"),
            CooldownOutcome::Shutdown
        );
        assert_eq!(lifecycle.borrow().phase, ServerPhase::Stopped);
        assert!(Instant::now() < retry_at);
        drop(shutdown_sender);
    }

    #[tokio::test(start_paused = true)]
    async fn wake_during_failed_cleanup_latches_for_cooldown() {
        let starting = LifecycleState {
            phase: ServerPhase::Starting,
            launch_generation: 1,
            ..LifecycleState::stopped()
        };
        let (mut control, wake_sender, _shutdown_sender, mut lifecycle) = test_control(starting);
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
    async fn demand_at_cooldown_expiry_launches_once() {
        let mut supervisor = spawn_launch_failure_supervisor();
        supervisor
            .wake_sender
            .send(WakeRequest::observed(*supervisor.lifecycle.borrow()))
            .await
            .expect("supervisor should accept the initial wake request");
        wait_for_phase(&mut supervisor.lifecycle, ServerPhase::Cooldown).await;
        let failure = *supervisor.lifecycle.borrow();
        let retry_at = failure.retry_at.expect("failure should have a deadline");
        let racing_sender = supervisor.wake_sender.clone();
        let demand = tokio::spawn(async move {
            sleep_until(retry_at).await;
            racing_sender
                .send(WakeRequest::observed(failure))
                .await
                .expect("racing demand should reach the supervisor");
        });

        tokio::time::advance(Duration::from_secs(5)).await;
        demand.await.expect("racing demand task should not panic");
        wait_for_lifecycle(&mut supervisor.lifecycle, |state| state.failure_streak == 2).await;
        assert_eq!(supervisor.lifecycle.borrow().launch_generation, 2);

        tokio::task::yield_now().await;
        assert_eq!(supervisor.lifecycle.borrow().launch_generation, 2);
        supervisor.shutdown().await;
    }

    #[tokio::test]
    async fn startup_timeout_enters_retryable_failure_cleanup() {
        let (rcon_address, backend) = refused_endpoints();
        let mut config = test_config(&rcon_address);
        config.startup_timeout = Duration::from_millis(50);
        config.command_timeout = Duration::from_millis(10);
        config.retry_interval = Duration::from_millis(10);
        let mut supervisor = TestSupervisor::spawn(config, backend);

        supervisor
            .wake_sender
            .send(WakeRequest::observed(*supervisor.lifecycle.borrow()))
            .await
            .expect("supervisor should accept the initial wake request");
        wait_for_phase(&mut supervisor.lifecycle, ServerPhase::Starting).await;
        wait_for_phase(&mut supervisor.lifecycle, ServerPhase::Cooldown).await;

        let failure = *supervisor.lifecycle.borrow();
        assert_eq!(failure.failure, Some(FailureCategory::StartupTimedOut));
        assert_eq!(failure.failure_streak, 1);
        assert_eq!(
            failure
                .retry_at
                .expect("startup timeout should have a deadline")
                - failure.phase_started_at,
            Duration::from_secs(5)
        );

        supervisor.shutdown().await;
    }

    #[tokio::test]
    async fn successful_exit_before_readiness_is_retryable() {
        let config = rapid_exit_config(&Endpoint::new("127.0.0.1", 9));
        let child = process::launch(&config.launch).expect("short-lived test server should launch");
        let (_wake_sender, wake_receiver) = mpsc::channel(1);
        let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
        let (lifecycle_sender, lifecycle) = watch::channel(LifecycleState::reconciling());
        let mut control = SupervisorControl {
            wake_requests: wake_receiver,
            shutdown: shutdown_receiver,
            lifecycle: lifecycle_sender,
        };
        control.record_launch_success();

        let outcome = run_server_cycle(
            child,
            &mut control,
            &config,
            &test_backend_endpoint(),
            &test_backend_use(),
        )
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
    async fn no_rcon_uses_tcp_readiness_and_console_stop() {
        let backend_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let backend_address = backend_listener.local_addr().unwrap();
        let backend = BackendEndpoint::network(
            Endpoint::new("127.0.0.1", backend_address.port()),
            Duration::from_secs(1),
        );
        let backend_accept = tokio::spawn(async move { backend_listener.accept().await.unwrap() });
        let config = no_rcon_config();
        let child = process::launch(&config.launch).unwrap();
        let (wake_sender, wake_receiver) = mpsc::channel(1);
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let starting = LifecycleState {
            phase: ServerPhase::Starting,
            launch_generation: 1,
            ..LifecycleState::stopped()
        };
        let (lifecycle_sender, mut lifecycle) = watch::channel(starting);
        let backend_use = test_backend_use();
        let cycle = tokio::spawn(async move {
            let mut control = SupervisorControl {
                wake_requests: wake_receiver,
                shutdown: shutdown_receiver,
                lifecycle: lifecycle_sender,
            };
            run_server_cycle(child, &mut control, &config, &backend, &backend_use).await
        });

        wait_for_phase(&mut lifecycle, ServerPhase::Running).await;
        shutdown_sender.send(()).unwrap();
        assert_eq!(cycle.await.unwrap().unwrap(), CycleOutcome::Shutdown);
        drop(wake_sender);
        backend_accept.await.unwrap();
        assert_eq!(lifecycle.borrow().phase, ServerPhase::Stopping);
    }

    #[tokio::test(start_paused = true)]
    async fn no_rcon_readiness_timeout_cleans_up_and_reaps() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let backend =
            BackendEndpoint::network(Endpoint::new("127.0.0.1", port), Duration::from_millis(10));
        let mut config = no_rcon_config();
        config.startup_timeout = Duration::from_millis(50);
        config.retry_interval = Duration::from_millis(10);
        config.command_timeout = Duration::from_millis(10);
        let child = process::launch(&config.launch).unwrap();
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

        let cycle = tokio::spawn(async move {
            run_server_cycle(child, &mut control, &config, &backend, &test_backend_use()).await
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(49)).await;
        assert_eq!(lifecycle.borrow().phase, ServerPhase::Starting);
        assert!(!cycle.is_finished());
        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::time::resume();

        let outcome = cycle.await.unwrap().unwrap();
        assert!(matches!(outcome, CycleOutcome::Failed { .. }));
        assert_eq!(
            lifecycle.borrow().failure,
            Some(FailureCategory::StartupTimedOut)
        );
        assert_eq!(lifecycle.borrow().phase, ServerPhase::Cooldown);
    }

    #[tokio::test]
    async fn no_rcon_running_child_exit_is_reaped_as_failure() {
        let mut config = no_rcon_config();
        config.launch.arguments[2] = "supervisor::tests::rapid_exit_fixture".into();
        let child = process::launch(&config.launch).unwrap();
        let (_wake_sender, wake_receiver) = mpsc::channel(1);
        let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
        let running = running_lifecycle(0);
        let (lifecycle_sender, lifecycle) = watch::channel(running);
        let mut control = SupervisorControl {
            wake_requests: wake_receiver,
            shutdown: shutdown_receiver,
            lifecycle: lifecycle_sender,
        };
        let backend_use = running_backend_use(running);

        let outcome = monitor_running_server(
            child,
            PlayerInspection::ProxyOnly,
            &mut control,
            &config,
            &backend_use,
            running.owned_running_cycle().unwrap(),
        )
        .await
        .unwrap();
        assert!(matches!(outcome, CycleOutcome::Failed { .. }));
        assert_eq!(
            lifecycle.borrow().failure,
            Some(FailureCategory::ExitedUnexpectedly)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn no_rcon_idle_stop_uses_only_proxy_activity() {
        let rcon_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = no_rcon_config();
        config.idle_timeout = Duration::from_secs(1);
        let child = process::launch(&config.launch).unwrap();
        let (wake_sender, wake_receiver) = mpsc::channel(1);
        let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
        let running = running_lifecycle(0);
        let (lifecycle_sender, lifecycle) = watch::channel(running);
        let mut control = SupervisorControl {
            wake_requests: wake_receiver,
            shutdown: shutdown_receiver,
            lifecycle: lifecycle_sender,
        };
        let backend_use = running_backend_use(running);
        let cycle = running.owned_running_cycle().unwrap();
        let active_session = backend_use
            .establish_login_transfer_session(backend_use.acquire_shared(cycle).await)
            .unwrap();
        let monitor = tokio::spawn(async move {
            monitor_running_server(
                child,
                PlayerInspection::ProxyOnly,
                &mut control,
                &config,
                &backend_use,
                cycle,
            )
            .await
        });

        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(lifecycle.borrow().phase, ServerPhase::Running);
        drop(active_session);
        tokio::time::advance(Duration::from_millis(999)).await;
        tokio::task::yield_now().await;
        assert_eq!(lifecycle.borrow().phase, ServerPhase::Running);
        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::time::resume();
        assert_eq!(monitor.await.unwrap().unwrap(), CycleOutcome::Stopped);
        assert_eq!(lifecycle.borrow().phase, ServerPhase::Stopping);
        let rcon_listener = rcon_listener.into_std().unwrap();
        assert!(matches!(
            rcon_listener.accept(),
            Err(error) if error.kind() == ErrorKind::WouldBlock
        ));
        drop(wake_sender);
    }

    #[tokio::test]
    async fn rcon_stop_failure_falls_back_to_console() {
        let mut config = no_rcon_config();
        let refused_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let refused_address = endpoint(refused_listener.local_addr().unwrap());
        drop(refused_listener);
        config.rcon = Some(RconConfig {
            endpoint: refused_address,
            password: RconSecret::new("secret".to_owned()).unwrap(),
        });
        let child = process::launch(&config.launch).unwrap();
        let (_wake_sender, wake_receiver) = mpsc::channel(1);
        let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
        let running = running_lifecycle(0);
        let (lifecycle_sender, _lifecycle) = watch::channel(running);
        let mut control = SupervisorControl {
            wake_requests: wake_receiver,
            shutdown: shutdown_receiver,
            lifecycle: lifecycle_sender,
        };

        assert_eq!(
            stop_for_shutdown(child, &mut control, &config)
                .await
                .unwrap(),
            CycleOutcome::Shutdown
        );
    }

    #[tokio::test]
    async fn console_failure_is_reaped_and_timeout_forces_termination() {
        let mut failed_config = no_rcon_config();
        failed_config.launch.arguments[2] = "supervisor::tests::rapid_exit_fixture".into();
        let mut failed_child = process::launch(&failed_config.launch).unwrap();
        let failed_pid = failed_child.id().unwrap();
        wait_for_test_process_exit(failed_pid);
        assert!(failed_child.send_console_stop().await.is_err());
        stop_and_reap(&mut failed_child, &failed_config)
            .await
            .unwrap();
        assert!(test_process_has_exited(failed_pid));

        let mut timeout_config = no_rcon_config();
        timeout_config.launch.arguments[2] = "supervisor::tests::server_process_fixture".into();
        timeout_config.command_timeout = Duration::ZERO;
        let mut timeout_child = process::launch(&timeout_config.launch).unwrap();
        let timeout_pid = timeout_child.id().unwrap();
        stop_and_reap(&mut timeout_child, &timeout_config)
            .await
            .unwrap();
        assert!(test_process_has_exited(timeout_pid));
    }

    #[tokio::test]
    async fn rapid_exits_increase_failure_streak() {
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
            let config = rapid_exit_config(&rcon_address);
            let child =
                process::launch(&config.launch).expect("short-lived test server should launch");
            control.begin_reconciliation();
            control.record_launch_success();

            let outcome = run_server_cycle(
                child,
                &mut control,
                &config,
                &test_backend_endpoint(),
                &test_backend_use(),
            )
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
    async fn exited_child_does_not_pass_stability_window() {
        let exit_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test exit-signal listener should bind");
        let exit_signal_address = exit_listener
            .local_addr()
            .expect("test exit-signal listener should have an address");
        let (rcon_address, poll_started, release_poll, rcon_server) =
            spawn_blocked_player_count_rcon_server().await;
        let mut config = signaled_exit_config(&rcon_address, exit_signal_address);
        config.command_timeout = Duration::from_secs(120);
        let child = process::launch(&config.launch).expect("signaled test server should launch");
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
        let backend_use = running_backend_use(running);
        let cycle = running.owned_running_cycle().unwrap();
        let player_inspection = connected_player_inspection(&rcon_address).await;
        let monitor = tokio::spawn(async move {
            monitor_running_server(
                child,
                player_inspection,
                &mut control,
                &config,
                &backend_use,
                cycle,
            )
            .await
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
    async fn stability_window_resets_failure_streak() {
        let (rcon_address, first_poll, rcon_server) = spawn_player_count_rcon_server(1).await;
        let config = stable_exit_config(&rcon_address);
        let child =
            process::launch(&config.launch).expect("stable-window test server should launch");
        let (_wake_sender, wake_receiver) = mpsc::channel(1);
        let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
        let running = failed_lifecycle(ServerPhase::Running, 3);
        let (lifecycle_sender, mut lifecycle) = watch::channel(running);
        let mut control = SupervisorControl {
            wake_requests: wake_receiver,
            shutdown: shutdown_receiver,
            lifecycle: lifecycle_sender,
        };
        let backend_use = running_backend_use(running);
        let cycle = running.owned_running_cycle().unwrap();
        let player_inspection = connected_player_inspection(&rcon_address).await;
        let monitor = tokio::spawn(async move {
            monitor_running_server(
                child,
                player_inspection,
                &mut control,
                &config,
                &backend_use,
                cycle,
            )
            .await
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
    async fn rcon_readiness_poll_and_stop_use_separate_sessions() {
        let (rcon_address, mut events, rcon_server) =
            spawn_scripted_list_rcon_server(vec![player_count_reply(0), player_count_reply(0)])
                .await;

        let mut config = test_config(&rcon_address);
        config.idle_timeout = Duration::from_millis(30);
        config.poll_interval = Duration::from_secs(1);
        let mut child = process::launch(&config.launch).unwrap();
        let (wake_sender, wake_receiver) = mpsc::channel(1);
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
        let backend_use = test_backend_use();
        assert!(matches!(
            wait_for_readiness(&mut child, &mut control, &config, &test_backend_endpoint()).await,
            ReadinessOutcome::Ready
        ));
        assert_eq!(events.recv().await, Some(TestRconEvent::Closed));

        let cycle = control.publish_running(&backend_use);
        let running = *lifecycle.borrow();
        let mut state =
            RunningMonitorState::new(cycle, PlayerInspection::from_config(&config), running);
        poll_player_presence(&mut state.player_inspection, &config).await;
        poll_player_presence(&mut state.player_inspection, &config).await;
        assert_eq!(events.recv().await, Some(TestRconEvent::List(1)));
        let idle_anchor = state
            .effective_idle_anchor(backend_use.activity_snapshot())
            .expect("confirmed zero players should establish an idle anchor");

        tokio::time::pause();
        tokio::time::advance(Duration::from_millis(29)).await;
        assert_eq!(lifecycle.borrow().phase, ServerPhase::Running);
        assert!(Instant::now() < idle_anchor + config.idle_timeout);
        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::time::resume();

        let outcome = attempt_expired_idle_stop(
            &mut child,
            &mut control,
            &config,
            &backend_use,
            &mut state,
            idle_anchor,
        )
        .await;
        assert!(matches!(outcome, RunningMonitorOutcome::Committed));
        assert_eq!(events.recv().await, Some(TestRconEvent::List(2)));
        assert_eq!(events.recv().await, Some(TestRconEvent::Closed));
        drop(state);

        let outcome = stop_committed_idle_server(child, &mut control, &config)
            .await
            .unwrap();
        assert_eq!(outcome, CycleOutcome::Stopped);
        assert_eq!(lifecycle.borrow().phase, ServerPhase::Stopping);
        assert_eq!(events.recv().await, Some(TestRconEvent::Stop));
        drop(wake_sender);
        rcon_server.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn idle_stop_takes_exclusive_lock_and_rechecks_rcon() {
        let (rcon_address, mut events, rcon_server) =
            spawn_scripted_list_rcon_server(vec![player_count_reply(0), player_count_reply(0)])
                .await;
        let mut config = test_config(&rcon_address);
        config.idle_timeout = Duration::from_secs(10);
        config.poll_interval = Duration::from_secs(100);
        let child = process::launch(&config.launch).expect("test server process should launch");
        let (wake_sender, wake_receiver) = mpsc::channel(1);
        let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
        let running = running_lifecycle(0);
        let (lifecycle_sender, mut lifecycle) = watch::channel(running);
        let mut control = SupervisorControl {
            wake_requests: wake_receiver,
            shutdown: shutdown_receiver,
            lifecycle: lifecycle_sender,
        };
        let backend_use = running_backend_use(running);
        let cycle = running.owned_running_cycle().unwrap();
        let replay_lease = backend_use.acquire_shared(cycle).await;
        let player_inspection = connected_player_inspection(&rcon_address).await;
        let monitor_backend_use = Arc::clone(&backend_use);
        let monitor = tokio::spawn(async move {
            monitor_running_server(
                child,
                player_inspection,
                &mut control,
                &config,
                &monitor_backend_use,
                cycle,
            )
            .await
        });

        assert_eq!(events.recv().await, Some(TestRconEvent::List(1)));
        tokio::time::advance(Duration::from_secs(9)).await;
        tokio::task::yield_now().await;
        assert_eq!(lifecycle.borrow().phase, ServerPhase::Running);
        assert!(matches!(
            events.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));

        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert!(matches!(
            events.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        drop(replay_lease);

        assert_eq!(events.recv().await, Some(TestRconEvent::List(2)));
        wait_for_phase(&mut lifecycle, ServerPhase::Stopping).await;
        let stopping_started_at = lifecycle.borrow().phase_started_at;
        assert_eq!(events.recv().await, Some(TestRconEvent::Closed));
        assert_eq!(events.recv().await, Some(TestRconEvent::Stop));
        let outcome = monitor
            .await
            .expect("running monitor task should not panic")
            .expect("idle stop should succeed");
        assert_eq!(outcome, CycleOutcome::Stopped);
        assert_eq!(lifecycle.borrow().phase, ServerPhase::Stopping);
        assert_eq!(lifecycle.borrow().phase_started_at, stopping_started_at);
        drop(wake_sender);
        rcon_server
            .await
            .expect("test RCON server should not panic");
    }

    #[tokio::test(start_paused = true)]
    async fn positive_final_rcon_check_vetoes_idle_stop() {
        assert_final_rcon_veto(player_count_reply(2)).await;
    }

    #[tokio::test(start_paused = true)]
    async fn malformed_final_rcon_check_vetoes_idle_stop() {
        assert_final_rcon_veto(TestListReply::Body("unexpected output".to_owned())).await;
    }

    #[tokio::test(start_paused = true)]
    async fn failed_final_rcon_check_vetoes_idle_stop() {
        assert_final_rcon_veto(TestListReply::Close).await;
    }

    #[tokio::test(start_paused = true)]
    async fn timed_out_final_rcon_is_discarded() {
        let (final_started, final_started_receiver) = oneshot::channel();
        let (rcon_address, mut events, rcon_server) = spawn_scripted_list_rcon_server(vec![
            player_count_reply(0),
            TestListReply::Block {
                started: final_started,
            },
        ])
        .await;
        let mut config = test_config(&rcon_address);
        config.idle_timeout = Duration::from_secs(1);
        config.poll_interval = Duration::from_secs(100);
        config.command_timeout = Duration::from_secs(10);
        let child = process::launch(&config.launch).unwrap();
        let (wake_sender, wake_receiver) = mpsc::channel(1);
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let running = running_lifecycle(0);
        let (lifecycle_sender, lifecycle) = watch::channel(running);
        let mut control = SupervisorControl {
            wake_requests: wake_receiver,
            shutdown: shutdown_receiver,
            lifecycle: lifecycle_sender,
        };
        let backend_use = running_backend_use(running);
        let cycle = running.owned_running_cycle().unwrap();
        let player_inspection = connected_player_inspection(&rcon_address).await;
        let monitor_backend_use = Arc::clone(&backend_use);
        let monitor = tokio::spawn(async move {
            monitor_running_server(
                child,
                player_inspection,
                &mut control,
                &config,
                &monitor_backend_use,
                cycle,
            )
            .await
        });

        assert_eq!(events.recv().await, Some(TestRconEvent::List(1)));
        tokio::time::advance(Duration::from_secs(1)).await;
        final_started_receiver.await.unwrap();
        assert_eq!(events.recv().await, Some(TestRconEvent::List(2)));
        tokio::time::advance(Duration::from_secs(10)).await;
        let observation = backend_use.acquire_exclusive().await;
        assert_eq!(lifecycle.borrow().phase, ServerPhase::Running);
        drop(observation);
        assert_eq!(events.recv().await, Some(TestRconEvent::Closed));

        shutdown_sender.send(()).unwrap();
        let outcome = monitor.await.unwrap().unwrap();
        assert_eq!(outcome, CycleOutcome::Shutdown);
        assert_eq!(events.recv().await, Some(TestRconEvent::Stop));
        drop(wake_sender);
        rcon_server.await.unwrap();
        assert!(matches!(
            events.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn unavailable_final_rcon_vetoes_idle_stop() {
        let (rcon_address, mut events, rcon_server) =
            spawn_scripted_list_rcon_server(Vec::new()).await;
        let mut config = test_config(&rcon_address);
        config.idle_timeout = Duration::from_secs(1);
        let mut child = process::launch(&config.launch).unwrap();
        let (wake_sender, wake_receiver) = mpsc::channel(1);
        let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
        let running = running_lifecycle(0);
        let (lifecycle_sender, lifecycle) = watch::channel(running);
        let mut control = SupervisorControl {
            wake_requests: wake_receiver,
            shutdown: shutdown_receiver,
            lifecycle: lifecycle_sender,
        };
        let backend_use = running_backend_use(running);
        let cycle = running.owned_running_cycle().unwrap();
        let mut state =
            RunningMonitorState::new(cycle, PlayerInspection::from_config(&config), running);
        let PlayerInspection::ConfiguredRcon { zero_anchor, .. } = &mut state.player_inspection
        else {
            panic!("test configuration should enable RCON player inspection");
        };
        *zero_anchor = Some(running.phase_started_at);

        tokio::time::advance(config.idle_timeout).await;
        let outcome = attempt_expired_idle_stop(
            &mut child,
            &mut control,
            &config,
            &backend_use,
            &mut state,
            running.phase_started_at,
        )
        .await;
        assert!(matches!(outcome, RunningMonitorOutcome::Continue));
        assert_eq!(lifecycle.borrow().phase, ServerPhase::Running);
        let PlayerInspection::ConfiguredRcon { zero_anchor, .. } = &state.player_inspection else {
            panic!("test configuration should enable RCON player inspection");
        };
        assert_eq!(*zero_anchor, None);

        tokio::time::resume();
        let outcome = stop_for_shutdown(child, &mut control, &config)
            .await
            .unwrap();
        assert_eq!(outcome, CycleOutcome::Shutdown);
        assert_eq!(events.recv().await, Some(TestRconEvent::Stop));
        drop(wake_sender);
        rcon_server.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_discards_pending_final_rcon_client() {
        let (final_started, final_started_receiver) = oneshot::channel();
        let (rcon_address, mut events, rcon_server) = spawn_scripted_list_rcon_server(vec![
            player_count_reply(0),
            TestListReply::Block {
                started: final_started,
            },
        ])
        .await;
        let mut config = test_config(&rcon_address);
        config.idle_timeout = Duration::from_secs(1);
        config.poll_interval = Duration::from_secs(100);
        config.command_timeout = Duration::from_secs(120);
        let child = process::launch(&config.launch).unwrap();
        let (wake_sender, wake_receiver) = mpsc::channel(1);
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let running = running_lifecycle(0);
        let (lifecycle_sender, lifecycle) = watch::channel(running);
        let mut control = SupervisorControl {
            wake_requests: wake_receiver,
            shutdown: shutdown_receiver,
            lifecycle: lifecycle_sender,
        };
        let backend_use = running_backend_use(running);
        let cycle = running.owned_running_cycle().unwrap();
        let player_inspection = connected_player_inspection(&rcon_address).await;
        let monitor = tokio::spawn(async move {
            monitor_running_server(
                child,
                player_inspection,
                &mut control,
                &config,
                &backend_use,
                cycle,
            )
            .await
        });

        assert_eq!(events.recv().await, Some(TestRconEvent::List(1)));
        tokio::time::advance(Duration::from_secs(1)).await;
        final_started_receiver.await.unwrap();
        assert_eq!(events.recv().await, Some(TestRconEvent::List(2)));
        shutdown_sender.send(()).unwrap();
        assert_eq!(events.recv().await, Some(TestRconEvent::Closed));
        let outcome = monitor.await.unwrap().unwrap();
        assert_eq!(outcome, CycleOutcome::Shutdown);
        assert_eq!(lifecycle.borrow().phase, ServerPhase::Stopping);
        assert_eq!(events.recv().await, Some(TestRconEvent::Stop));
        drop(wake_sender);
        rcon_server.await.unwrap();
        assert!(matches!(
            events.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn nonzero_or_unknown_rcon_resets_zero_interval() {
        for first_reply in [
            player_count_reply(3),
            TestListReply::Body("not a player count".to_owned()),
        ] {
            let (rcon_address, _events, rcon_server) =
                spawn_scripted_list_rcon_server(vec![first_reply, player_count_reply(0)]).await;
            let config = test_config(&rcon_address);
            let mut player_inspection = PlayerInspection::ConfiguredRcon {
                client: Some(RconClient::connect(&rcon_address, "secret").await.unwrap()),
                next_poll: Instant::now(),
                zero_anchor: Some(Instant::now()),
            };
            let old_anchor = Instant::now();
            tokio::time::advance(Duration::from_secs(5)).await;

            poll_player_presence(&mut player_inspection, &config).await;
            let PlayerInspection::ConfiguredRcon { zero_anchor, .. } = &player_inspection else {
                unreachable!();
            };
            assert_eq!(*zero_anchor, None);
            tokio::time::advance(Duration::from_secs(7)).await;
            poll_player_presence(&mut player_inspection, &config).await;
            let PlayerInspection::ConfiguredRcon { zero_anchor, .. } = &player_inspection else {
                unreachable!();
            };
            assert!(zero_anchor.is_some_and(|anchor| anchor > old_anchor));

            drop(player_inspection);
            rcon_server.abort();
            assert!(rcon_server.await.unwrap_err().is_cancelled());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn pending_idle_lock_handles_stability_and_shutdown() {
        let (rcon_address, mut events, rcon_server) =
            spawn_scripted_list_rcon_server(vec![player_count_reply(0)]).await;
        let mut config = test_config(&rcon_address);
        config.idle_timeout = Duration::from_secs(1);
        config.poll_interval = Duration::from_secs(100);
        let child = process::launch(&config.launch).unwrap();
        let (wake_sender, wake_receiver) = mpsc::channel(1);
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let running = running_lifecycle(2);
        let (lifecycle_sender, mut lifecycle) = watch::channel(running);
        let mut control = SupervisorControl {
            wake_requests: wake_receiver,
            shutdown: shutdown_receiver,
            lifecycle: lifecycle_sender,
        };
        let backend_use = running_backend_use(running);
        let cycle = running.owned_running_cycle().unwrap();
        let blocking_reader = backend_use.acquire_shared(cycle).await;
        let player_inspection = connected_player_inspection(&rcon_address).await;
        let monitor = tokio::spawn(async move {
            monitor_running_server(
                child,
                player_inspection,
                &mut control,
                &config,
                &backend_use,
                cycle,
            )
            .await
        });

        assert_eq!(events.recv().await, Some(TestRconEvent::List(1)));
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        tokio::time::advance(
            STABILITY_WINDOW
                .checked_sub(Duration::from_secs(1))
                .expect("stability window should exceed one second"),
        )
        .await;
        wait_for_lifecycle(&mut lifecycle, |state| state.failure_streak == 0).await;
        assert_eq!(lifecycle.borrow().phase, ServerPhase::Running);
        assert!(matches!(
            events.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));

        shutdown_sender.send(()).unwrap();
        assert_eq!(events.recv().await, Some(TestRconEvent::Closed));
        assert_eq!(events.recv().await, Some(TestRconEvent::Stop));
        let outcome = monitor.await.unwrap().unwrap();
        assert_eq!(outcome, CycleOutcome::Shutdown);
        drop((blocking_reader, wake_sender));
        rcon_server.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn child_exit_cancels_pending_exclusivity() {
        let exit_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let exit_signal_address = exit_listener.local_addr().unwrap();
        let (rcon_address, mut events, rcon_server) =
            spawn_scripted_list_rcon_server(vec![player_count_reply(0)]).await;
        let mut config = signaled_exit_config(&rcon_address, exit_signal_address);
        config.idle_timeout = Duration::from_secs(1);
        config.poll_interval = Duration::from_secs(100);
        let child = process::launch(&config.launch).unwrap();
        let (mut exit_signal, _) = exit_listener.accept().await.unwrap();
        let (wake_sender, wake_receiver) = mpsc::channel(1);
        let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
        let running = running_lifecycle(0);
        let (lifecycle_sender, lifecycle) = watch::channel(running);
        let mut control = SupervisorControl {
            wake_requests: wake_receiver,
            shutdown: shutdown_receiver,
            lifecycle: lifecycle_sender,
        };
        let backend_use = running_backend_use(running);
        let cycle = running.owned_running_cycle().unwrap();
        let blocking_reader = backend_use.acquire_shared(cycle).await;
        let player_inspection = connected_player_inspection(&rcon_address).await;
        let monitor = tokio::spawn(async move {
            monitor_running_server(
                child,
                player_inspection,
                &mut control,
                &config,
                &backend_use,
                cycle,
            )
            .await
        });

        assert_eq!(events.recv().await, Some(TestRconEvent::List(1)));
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        exit_signal.write_all(&[1]).await.unwrap();
        assert_eq!(events.recv().await, Some(TestRconEvent::Closed));
        let outcome = monitor.await.unwrap().unwrap();
        assert!(matches!(outcome, CycleOutcome::Failed { .. }));
        assert_eq!(lifecycle.borrow().phase, ServerPhase::Cooldown);
        drop((blocking_reader, wake_sender));
        rcon_server.abort();
        assert!(rcon_server.await.unwrap_err().is_cancelled());
    }

    #[tokio::test(start_paused = true)]
    async fn child_exit_cancels_a_pending_final_rcon_check() {
        let exit_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let exit_signal_address = exit_listener.local_addr().unwrap();
        let (final_started, final_started_receiver) = oneshot::channel();
        let (rcon_address, mut events, rcon_server) = spawn_scripted_list_rcon_server(vec![
            player_count_reply(0),
            TestListReply::Block {
                started: final_started,
            },
        ])
        .await;
        let mut config = signaled_exit_config(&rcon_address, exit_signal_address);
        config.idle_timeout = Duration::from_secs(1);
        config.poll_interval = Duration::from_secs(100);
        config.command_timeout = Duration::from_secs(120);
        let child = process::launch(&config.launch).unwrap();
        let (mut exit_signal, _) = exit_listener.accept().await.unwrap();
        let (wake_sender, wake_receiver) = mpsc::channel(1);
        let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
        let running = running_lifecycle(0);
        let (lifecycle_sender, lifecycle) = watch::channel(running);
        let mut control = SupervisorControl {
            wake_requests: wake_receiver,
            shutdown: shutdown_receiver,
            lifecycle: lifecycle_sender,
        };
        let backend_use = running_backend_use(running);
        let cycle = running.owned_running_cycle().unwrap();
        let player_inspection = connected_player_inspection(&rcon_address).await;
        let monitor = tokio::spawn(async move {
            monitor_running_server(
                child,
                player_inspection,
                &mut control,
                &config,
                &backend_use,
                cycle,
            )
            .await
        });

        assert_eq!(events.recv().await, Some(TestRconEvent::List(1)));
        tokio::time::advance(Duration::from_secs(1)).await;
        final_started_receiver.await.unwrap();
        assert_eq!(events.recv().await, Some(TestRconEvent::List(2)));
        tokio::time::resume();
        exit_signal.write_all(&[1]).await.unwrap();
        assert_eq!(events.recv().await, Some(TestRconEvent::Closed));
        let outcome = monitor.await.unwrap().unwrap();
        assert!(matches!(outcome, CycleOutcome::Failed { .. }));
        assert_eq!(lifecycle.borrow().phase, ServerPhase::Cooldown);
        drop(wake_sender);
        rcon_server.abort();
        assert!(rcon_server.await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn idle_stop_resets_failure_streak() {
        let (rcon_address, rcon_ready, rcon_server) = spawn_test_rcon_server().await;
        let config = test_config(&rcon_address);
        let child = process::launch(&config.launch).expect("test server process should launch");
        let (_wake_sender, wake_receiver) = mpsc::channel(1);
        let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
        let running = failed_lifecycle(ServerPhase::Running, 3);
        let (lifecycle_sender, lifecycle) = watch::channel(running);
        let mut control = SupervisorControl {
            wake_requests: wake_receiver,
            shutdown: shutdown_receiver,
            lifecycle: lifecycle_sender,
        };

        control.begin_idle_stop(running.owned_running_cycle().unwrap());
        let outcome = stop_committed_idle_server(child, &mut control, &config)
            .await
            .expect("controlled idle stop should succeed");
        rcon_ready
            .await
            .expect("test RCON server should authenticate the fresh stop client");
        assert_eq!(outcome, CycleOutcome::Stopped);
        assert_eq!(lifecycle.borrow().failure_streak, 0);
        assert_eq!(lifecycle.borrow().failure, None);
        rcon_server
            .await
            .expect("test RCON server should not panic");
    }

    #[tokio::test]
    async fn wait_error_is_fatal_after_cleanup() {
        let config = test_config(&Endpoint::new("127.0.0.1", 9));
        let child = process::launch(&config.launch).expect("test server process should launch");
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
            &mut control,
            &config,
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
    async fn failed_cleanup_error_is_fatal() {
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
    fn active_phase_wakes_cannot_start_new_cycle() {
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
    async fn closed_shutdown_source_precedes_queued_startup_wake() {
        let (wake_sender, wake_receiver) = mpsc::channel(1);
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let (lifecycle_sender, lifecycle) = watch::channel(LifecycleState::reconciling());
        wake_sender
            .try_send(WakeRequest::observed(*lifecycle.borrow()))
            .expect("first wake request should fit");
        drop(shutdown_sender);

        let (rcon_address, backend) = refused_endpoints();
        run(
            wake_receiver,
            shutdown_receiver,
            lifecycle_sender,
            test_config(&rcon_address),
            backend,
            test_backend_use(),
        )
        .await
        .expect("supervisor shutdown should succeed");

        assert_eq!(lifecycle.borrow().phase, ServerPhase::Reconciling);
        assert_eq!(lifecycle.borrow().launch_generation, 0);
    }

    #[tokio::test]
    async fn closed_wake_source_stops_and_reaps_an_active_child() {
        let (wake_sender, wake_receiver) = mpsc::channel(1);
        let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
        let (lifecycle_sender, mut lifecycle) = watch::channel(LifecycleState::reconciling());
        let (rcon_address, backend) = refused_endpoints();
        let supervisor = tokio::spawn(run(
            wake_receiver,
            shutdown_receiver,
            lifecycle_sender,
            test_config(&rcon_address),
            backend,
            test_backend_use(),
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
    async fn wake_bursts_launch_one_child() {
        let (wake_sender, wake_receiver) = mpsc::channel(1);
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let (lifecycle_sender, mut lifecycle) = watch::channel(LifecycleState::reconciling());
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
        let (rcon_address, backend) = refused_endpoints();
        let supervisor = tokio::spawn(run(
            wake_receiver,
            shutdown_receiver,
            lifecycle_sender,
            test_config(&rcon_address),
            backend,
            test_backend_use(),
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
        let config = test_config(&rcon_address);
        let child = process::launch(&config.launch).expect("test server process should launch");
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
        let backend_use = running_backend_use(running);
        let cycle = running.owned_running_cycle().unwrap();

        let outcome = timeout(
            Duration::from_secs(10),
            monitor_running_server(
                child,
                PlayerInspection::from_config(&config),
                &mut control,
                &config,
                &backend_use,
                cycle,
            ),
        )
        .await
        .expect("supervisor cleanup should be bounded")
        .expect("supervisor cleanup should succeed");
        rcon_ready
            .await
            .expect("test RCON server should authenticate the fresh stop client");
        assert_eq!(outcome, CycleOutcome::Shutdown);
        timeout(Duration::from_secs(2), rcon_server)
            .await
            .expect("test RCON server should finish")
            .expect("test RCON server should not panic");
        assert_eq!(lifecycle.borrow().phase, ServerPhase::Stopping);
        assert_eq!(lifecycle.borrow().launch_generation, 1);
    }

    #[tokio::test]
    async fn stopping_wake_flood_does_not_starve_cleanup() {
        let (rcon_address, rcon_ready, rcon_server) = spawn_test_rcon_server().await;
        let config = test_config(&rcon_address);
        let child = process::launch(&config.launch).expect("test server process should launch");
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
            control.begin_idle_stop(active.owned_running_cycle().unwrap());
            stop_committed_idle_server(child, &mut control, &config).await
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
        rcon_ready
            .await
            .expect("test RCON server should authenticate the fresh stop client");
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
    async fn shutdown_overrides_pending_restart() {
        let (rcon_address, rcon_ready, rcon_server) = spawn_test_rcon_server().await;
        let config = test_config(&rcon_address);
        let child = process::launch(&config.launch).expect("test server process should launch");
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
            control.begin_idle_stop(active.owned_running_cycle().unwrap());
            stop_committed_idle_server(child, &mut control, &config).await
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
        rcon_ready
            .await
            .expect("test RCON server should authenticate the fresh stop client");
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
