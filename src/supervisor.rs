use std::future::Future;
use std::io::ErrorKind;
use std::process::ExitStatus;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::net::TcpStream;
use tokio::sync::oneshot::error::TryRecvError;
use tokio::sync::{Notify, OwnedRwLockWriteGuard, mpsc, oneshot, watch};
use tokio::time::{Instant, sleep_until, timeout, timeout_at};

use crate::backend_use::{BackendCycle, BackendUseCoordinator, CurrentCycleActivity};
use crate::context::ServerContext;
use crate::endpoint::Endpoint;
use crate::process::{self, LaunchCommand, OwnedProcess};
use crate::rcon::{self, RconClient, RconProbeClassification, RconSecret};

const STABILITY_WINDOW: Duration = Duration::from_secs(60);
pub(crate) const MUTATION_QUEUE_CAPACITY: usize = 32;

const ADMISSION_PENDING: u8 = 0;
const ADMISSION_CANCELLED: u8 = 1;
const ADMISSION_ADMITTED: u8 = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MutationOperation {
    Start,
    Stop,
    Restart,
}

impl MutationOperation {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::Stop => "stop",
            Self::Restart => "restart",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MutationDisposition {
    Accepted,
    AlreadySatisfied,
}

impl MutationDisposition {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::AlreadySatisfied => "already_satisfied",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MutationSuccess {
    pub(crate) server_id: String,
    pub(crate) operation: MutationOperation,
    pub(crate) request_id: String,
    pub(crate) command_revision: u64,
    pub(crate) disposition: MutationDisposition,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MutationError {
    OperationRejected,
    ExternalBackendNotOwned,
    ConflictingBackendEvidence,
    OperationInProgress,
    StaleCommandRevision,
    RequestIdConflict,
    InternalFailure,
}

pub(crate) type MutationReply = std::result::Result<MutationSuccess, MutationError>;

#[derive(Debug)]
pub(crate) struct AdmissionGate(AtomicU8);

impl AdmissionGate {
    pub(crate) const fn pending() -> Self {
        Self(AtomicU8::new(ADMISSION_PENDING))
    }

    pub(crate) fn cancel(&self) -> bool {
        self.0
            .compare_exchange(
                ADMISSION_PENDING,
                ADMISSION_CANCELLED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    fn admit(&self) -> bool {
        self.0
            .compare_exchange(
                ADMISSION_PENDING,
                ADMISSION_ADMITTED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire) == ADMISSION_CANCELLED
    }

    #[cfg(test)]
    pub(crate) fn is_admitted(&self) -> bool {
        self.0.load(Ordering::Acquire) == ADMISSION_ADMITTED
    }
}

pub(crate) struct MutationEnvelope {
    pub(crate) operation: MutationOperation,
    pub(crate) request_id: String,
    pub(crate) expected_command_revision: u64,
    pub(crate) reply: oneshot::Sender<MutationReply>,
    pub(crate) admission: Arc<AdmissionGate>,
}

#[derive(Clone)]
pub(crate) struct FatalSignal {
    sender: watch::Sender<bool>,
}

impl FatalSignal {
    pub(crate) fn new() -> (Self, watch::Receiver<bool>) {
        let (sender, receiver) = watch::channel(false);
        (Self { sender }, receiver)
    }

    pub(crate) fn raise(&self) {
        self.sender.send_replace(true);
    }
}

pub(crate) struct QuiescenceRequest {
    pub(crate) acknowledged: oneshot::Sender<Result<()>>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct FreshnessKey {
    command_revision: u64,
    launch_generation: u64,
}

struct WakeLatchState {
    pending: Option<WakeRequest>,
    closed: bool,
}

struct WakeLatchInner {
    state: Mutex<WakeLatchState>,
    changed: Notify,
}

pub(crate) struct WakeLatchRoot {
    inner: Arc<WakeLatchInner>,
}

#[derive(Clone)]
pub(crate) struct WakeLatchSender {
    inner: Arc<WakeLatchInner>,
}

pub(crate) struct WakeLatchReceiver {
    inner: Arc<WakeLatchInner>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WakeLatchError {
    Closed,
}

pub(crate) fn wake_latch() -> (WakeLatchRoot, WakeLatchSender, WakeLatchReceiver) {
    let inner = Arc::new(WakeLatchInner {
        state: Mutex::new(WakeLatchState {
            pending: None,
            closed: false,
        }),
        changed: Notify::new(),
    });
    (
        WakeLatchRoot {
            inner: Arc::clone(&inner),
        },
        WakeLatchSender {
            inner: Arc::clone(&inner),
        },
        WakeLatchReceiver { inner },
    )
}

impl WakeLatchRoot {
    pub(crate) fn close(&self) {
        let mut state = self.inner.state.lock().expect("wake latch mutex poisoned");
        state.closed = true;
        state.pending = None;
        drop(state);
        self.inner.changed.notify_waiters();
    }
}

impl WakeLatchSender {
    pub(crate) fn enqueue(&self, request: WakeRequest) -> Result<(), WakeLatchError> {
        let mut state = self.inner.state.lock().expect("wake latch mutex poisoned");
        if state.closed {
            return Err(WakeLatchError::Closed);
        }
        let notify = match state.pending {
            None => {
                state.pending = Some(request);
                true
            }
            Some(pending) if request.freshness() > pending.freshness() => {
                state.pending = Some(request);
                true
            }
            Some(_) => false,
        };
        drop(state);
        if notify {
            self.inner.changed.notify_one();
        }
        Ok(())
    }

    #[cfg(test)]
    fn close(&self) {
        let mut state = self.inner.state.lock().expect("wake latch mutex poisoned");
        state.closed = true;
        state.pending = None;
        drop(state);
        self.inner.changed.notify_waiters();
    }

    #[cfg(test)]
    async fn wait_until_empty(&self) -> Result<(), WakeLatchError> {
        loop {
            let notified = self.inner.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let state = self.inner.state.lock().expect("wake latch mutex poisoned");
                if state.closed {
                    return Err(WakeLatchError::Closed);
                }
                if state.pending.is_none() {
                    return Ok(());
                }
            }
            notified.await;
        }
    }
}

impl WakeLatchReceiver {
    async fn recv(&self) -> Option<WakeRequest> {
        loop {
            let notified = self.inner.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut state = self.inner.state.lock().expect("wake latch mutex poisoned");
                if state.closed {
                    return None;
                }
                if let Some(request) = state.pending.take() {
                    drop(state);
                    self.inner.changed.notify_waiters();
                    return Some(request);
                }
            }
            notified.await;
        }
    }

    pub(crate) fn try_recv(&self) -> Option<WakeRequest> {
        let mut state = self.inner.state.lock().expect("wake latch mutex poisoned");
        let request = state.pending.take();
        drop(state);
        if request.is_some() {
            self.inner.changed.notify_waiters();
        }
        request
    }

    fn is_closed(&self) -> bool {
        self.inner
            .state
            .lock()
            .expect("wake latch mutex poisoned")
            .closed
    }
}

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
pub(crate) enum ServerPhase {
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
    command_revision: u64,
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
    pub(crate) command_revision: u64,
}

impl LifecycleState {
    pub(crate) fn reconciling() -> Self {
        Self {
            phase: ServerPhase::Reconciling,
            command_revision: 0,
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
            command_revision: 0,
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
            command_revision: self.command_revision,
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
            command_revision: 0,
            launch_generation: 0,
            failure: None,
            failure_streak: 0,
            phase_started_at: Instant::now(),
            retry_at: None,
            reconciliation_evidence,
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
        let base = Self::test_phase(phase);
        Self {
            phase: base.phase,
            command_revision: base.command_revision,
            launch_generation: base.launch_generation,
            failure,
            failure_streak,
            phase_started_at: base.phase_started_at,
            retry_at,
            reconciliation_evidence: base.reconciliation_evidence,
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
            && self.observed.command_revision == current.command_revision
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
        current.phase == ServerPhase::Stopping && self.observed == current
    }

    fn requests_retry(self, current: LifecycleState) -> bool {
        current.failure.is_some()
            && self.observed.command_revision == current.command_revision
            && self.observed.failure == current.failure
            && self.observed.failure_streak == current.failure_streak
            && self.observed.launch_generation == current.launch_generation
            && matches!(
                self.observed.phase,
                ServerPhase::Stopping | ServerPhase::Cooldown
            )
            && matches!(current.phase, ServerPhase::Stopping | ServerPhase::Cooldown)
    }

    const fn freshness(self) -> FreshnessKey {
        FreshnessKey {
            command_revision: self.observed.command_revision,
            launch_generation: self.observed.launch_generation,
        }
    }
}

pub(crate) struct SupervisorConfig {
    pub(crate) context: ServerContext,
    pub(crate) launch: LaunchCommand,
    pub(crate) rcon: Option<RconConfig>,
    pub(crate) poll_interval: Duration,
    pub(crate) idle_timeout: Duration,
    pub(crate) startup_timeout: Duration,
    pub(crate) retry_interval: Duration,
    pub(crate) command_timeout: Duration,
    pub(crate) shutdown_timeout: Duration,
}

pub(crate) struct RconConfig {
    pub(crate) endpoint: Endpoint,
    pub(crate) password: RconSecret,
}

pub(crate) struct BackendEndpoint {
    endpoint: Endpoint,
    connect_timeout: Duration,
    #[cfg(test)]
    scripted_probes: Option<Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<ReconciliationResult>>>>,
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
    ExplicitMutation,
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
    wake_requests: WakeLatchReceiver,
    mutations: mpsc::Receiver<MutationEnvelope>,
    shutdown: oneshot::Receiver<()>,
    lifecycle: watch::Sender<LifecycleState>,
    // Replay is limited to the last admitted (request ID, expected revision, operation).
    // Older retries become stale after another admission; conflicts apply only to this
    // replay slot, so reusing an older ID at the current revision is a new request.
    last_admission: Option<MutationSuccess>,
    launch_demand: bool,
    stopping_reason: Option<StoppingReason>,
    reconciliation_mode: ReconciliationMode,
    fatal: FatalSignal,
    runtime_drained: watch::Receiver<bool>,
    mutations_closed: bool,
    quiescence: mpsc::Sender<QuiescenceRequest>,
    fatal_error: Option<anyhow::Error>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StoppingReason {
    AutomaticIdle,
    FailureCleanup,
    ExplicitStop,
    ExplicitRestart,
    DaemonShutdown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReconciliationMode {
    Initial,
    FinalGate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MutationEffect {
    None,
    StopOwned,
}

struct AdmissionPlan {
    next: LifecycleState,
    disposition: MutationDisposition,
    effect: MutationEffect,
    launch_demand: bool,
    stopping_reason: Option<StoppingReason>,
}

impl SupervisorControl {
    fn raise_fatal(&mut self, message: &'static str) {
        if self.fatal_error.is_none() {
            self.fatal_error = Some(anyhow::anyhow!(message));
        }
        self.fatal.raise();
    }

    fn terminal_result(&mut self) -> Result<()> {
        match self.fatal_error.take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn next_launch_generation(&mut self) -> Option<u64> {
        let current = self.current_lifecycle();
        if let Some(next) = current.launch_generation.checked_add(1) {
            Some(next)
        } else {
            self.raise_fatal("lifecycle launch generation exhausted");
            None
        }
    }

    fn handle_mutation(
        &mut self,
        envelope: MutationEnvelope,
        config: &SupervisorConfig,
    ) -> MutationEffect {
        if envelope.admission.is_cancelled() {
            return MutationEffect::None;
        }

        if let Some(last) = &self.last_admission
            && last.request_id == envelope.request_id
        {
            let original_expected = last.command_revision.checked_sub(1);
            if last.operation == envelope.operation
                && original_expected == Some(envelope.expected_command_revision)
            {
                let _ = envelope.reply.send(Ok(last.clone()));
            } else {
                let _ = envelope.reply.send(Err(MutationError::RequestIdConflict));
            }
            return MutationEffect::None;
        }

        let current = self.current_lifecycle();
        if envelope.expected_command_revision != current.command_revision {
            let _ = envelope
                .reply
                .send(Err(MutationError::StaleCommandRevision));
            return MutationEffect::None;
        }
        let Some(next_revision) = current.command_revision.checked_add(1) else {
            self.raise_fatal("lifecycle command revision exhausted");
            let _ = envelope.reply.send(Err(MutationError::InternalFailure));
            return MutationEffect::None;
        };

        let plan = match self.plan_mutation(envelope.operation, current, next_revision) {
            Ok(plan) => plan,
            Err(error) => {
                let _ = envelope.reply.send(Err(error));
                return MutationEffect::None;
            }
        };
        if !envelope.admission.admit() {
            return MutationEffect::None;
        }

        self.launch_demand = plan.launch_demand;
        self.stopping_reason = plan.stopping_reason;
        self.publish(plan.next);
        let success = MutationSuccess {
            server_id: config.context.id().to_owned(),
            operation: envelope.operation,
            request_id: envelope.request_id,
            command_revision: next_revision,
            disposition: plan.disposition,
        };
        self.last_admission = Some(success.clone());
        let _ = envelope.reply.send(Ok(success));
        plan.effect
    }

    fn plan_mutation(
        &self,
        operation: MutationOperation,
        current: LifecycleState,
        next_revision: u64,
    ) -> std::result::Result<AdmissionPlan, MutationError> {
        let next = LifecycleState {
            phase: current.phase,
            command_revision: next_revision,
            launch_generation: current.launch_generation,
            failure: current.failure,
            failure_streak: current.failure_streak,
            phase_started_at: current.phase_started_at,
            retry_at: current.retry_at,
            reconciliation_evidence: current.reconciliation_evidence,
        };
        match operation {
            MutationOperation::Start => self.plan_start(current, next),
            MutationOperation::Stop => self.plan_stop(current, next),
            MutationOperation::Restart => self.plan_restart(current, next),
        }
    }

    fn plan_start(
        &self,
        current: LifecycleState,
        next: LifecycleState,
    ) -> std::result::Result<AdmissionPlan, MutationError> {
        let mut launch_demand = self.launch_demand;
        let disposition = match current.phase {
            ServerPhase::Reconciling
                if self.reconciliation_mode == ReconciliationMode::Initial && !launch_demand =>
            {
                launch_demand = true;
                MutationDisposition::Accepted
            }
            ServerPhase::Reconciling | ServerPhase::Starting => {
                return Err(MutationError::OperationInProgress);
            }
            ServerPhase::Stopped => {
                launch_demand = true;
                MutationDisposition::Accepted
            }
            ServerPhase::Running => MutationDisposition::AlreadySatisfied,
            ServerPhase::Stopping => match self.stopping_reason {
                Some(StoppingReason::AutomaticIdle | StoppingReason::FailureCleanup) => {
                    if launch_demand {
                        return Err(MutationError::OperationInProgress);
                    }
                    launch_demand = true;
                    MutationDisposition::Accepted
                }
                Some(StoppingReason::ExplicitStop | StoppingReason::ExplicitRestart) => {
                    return Err(MutationError::OperationInProgress);
                }
                Some(StoppingReason::DaemonShutdown) | None => {
                    return Err(MutationError::OperationRejected);
                }
            },
            ServerPhase::Cooldown => {
                if launch_demand {
                    return Err(MutationError::OperationInProgress);
                }
                launch_demand = true;
                MutationDisposition::Accepted
            }
            ServerPhase::External => return Err(MutationError::ExternalBackendNotOwned),
            ServerPhase::Conflict
                if current.reconciliation_evidence == ReconciliationEvidence::Inconclusive =>
            {
                launch_demand = true;
                MutationDisposition::Accepted
            }
            ServerPhase::Conflict => {
                return Err(MutationError::ConflictingBackendEvidence);
            }
        };

        Ok(AdmissionPlan {
            next,
            disposition,
            effect: MutationEffect::None,
            launch_demand,
            stopping_reason: self.stopping_reason,
        })
    }

    fn plan_stop(
        &self,
        current: LifecycleState,
        mut next: LifecycleState,
    ) -> std::result::Result<AdmissionPlan, MutationError> {
        let launch_demand = false;
        let mut stopping_reason = self.stopping_reason;
        let mut effect = MutationEffect::None;
        let disposition = match current.phase {
            ServerPhase::Reconciling if self.reconciliation_mode == ReconciliationMode::Initial => {
                return Err(MutationError::OperationRejected);
            }
            ServerPhase::Reconciling | ServerPhase::Starting | ServerPhase::Running => {
                if current.phase == ServerPhase::Reconciling {
                    return Err(MutationError::OperationInProgress);
                }
                next.phase = ServerPhase::Stopping;
                next.phase_started_at = Instant::now();
                next.retry_at = None;
                next.reconciliation_evidence = ReconciliationEvidence::None;
                stopping_reason = Some(StoppingReason::ExplicitStop);
                effect = MutationEffect::StopOwned;
                MutationDisposition::Accepted
            }
            ServerPhase::Stopped | ServerPhase::Cooldown => MutationDisposition::AlreadySatisfied,
            ServerPhase::Stopping => match stopping_reason {
                Some(StoppingReason::AutomaticIdle | StoppingReason::ExplicitRestart) => {
                    stopping_reason = Some(StoppingReason::ExplicitStop);
                    MutationDisposition::Accepted
                }
                Some(StoppingReason::FailureCleanup) => {
                    if self.launch_demand {
                        MutationDisposition::Accepted
                    } else {
                        MutationDisposition::AlreadySatisfied
                    }
                }
                Some(StoppingReason::ExplicitStop) => {
                    return Err(MutationError::OperationInProgress);
                }
                Some(StoppingReason::DaemonShutdown) | None => {
                    return Err(MutationError::OperationRejected);
                }
            },
            ServerPhase::External => return Err(MutationError::ExternalBackendNotOwned),
            ServerPhase::Conflict => {
                return Err(MutationError::ConflictingBackendEvidence);
            }
        };

        Ok(AdmissionPlan {
            next,
            disposition,
            effect,
            launch_demand,
            stopping_reason,
        })
    }

    fn plan_restart(
        &self,
        current: LifecycleState,
        mut next: LifecycleState,
    ) -> std::result::Result<AdmissionPlan, MutationError> {
        let mut launch_demand = self.launch_demand;
        let mut stopping_reason = self.stopping_reason;
        let mut effect = MutationEffect::None;
        match current.phase {
            ServerPhase::Reconciling if self.reconciliation_mode == ReconciliationMode::Initial => {
                return Err(MutationError::OperationRejected);
            }
            ServerPhase::Reconciling => return Err(MutationError::OperationInProgress),
            ServerPhase::Stopped | ServerPhase::Cooldown => {
                return Err(MutationError::OperationRejected);
            }
            ServerPhase::Starting | ServerPhase::Running => {
                next.phase = ServerPhase::Stopping;
                next.phase_started_at = Instant::now();
                next.retry_at = None;
                next.reconciliation_evidence = ReconciliationEvidence::None;
                launch_demand = true;
                stopping_reason = Some(StoppingReason::ExplicitRestart);
                effect = MutationEffect::StopOwned;
            }
            ServerPhase::Stopping => match stopping_reason {
                Some(StoppingReason::AutomaticIdle) if !launch_demand => {
                    launch_demand = true;
                    stopping_reason = Some(StoppingReason::ExplicitRestart);
                }
                Some(
                    StoppingReason::AutomaticIdle
                    | StoppingReason::ExplicitStop
                    | StoppingReason::ExplicitRestart,
                ) => {
                    return Err(MutationError::OperationInProgress);
                }
                Some(StoppingReason::FailureCleanup | StoppingReason::DaemonShutdown) | None => {
                    return Err(MutationError::OperationRejected);
                }
            },
            ServerPhase::External => return Err(MutationError::ExternalBackendNotOwned),
            ServerPhase::Conflict => {
                return Err(MutationError::ConflictingBackendEvidence);
            }
        }

        Ok(AdmissionPlan {
            next,
            disposition: MutationDisposition::Accepted,
            effect,
            launch_demand,
            stopping_reason,
        })
    }

    fn receive_mutation(
        &mut self,
        envelope: Option<MutationEnvelope>,
        config: &SupervisorConfig,
    ) -> MutationEffect {
        if let Some(envelope) = envelope {
            self.handle_mutation(envelope, config)
        } else {
            self.mutations_closed = true;
            MutationEffect::None
        }
    }

    async fn wait_for_reconciliation_demand(&mut self, config: &SupervisorConfig) -> bool {
        loop {
            if self.shutdown_pending() {
                return false;
            }
            if self.launch_demand {
                self.launch_demand = false;
                return true;
            }
            // Closure is terminal even when the one-slot wake latch is still occupied.
            if self.wake_requests.is_closed() {
                return false;
            }

            tokio::select! {
                biased;

                _ = &mut self.shutdown => return false,
                envelope = self.mutations.recv(), if !self.mutations_closed => {
                    let _ = self.receive_mutation(envelope, config);
                }
                request = self.wake_requests.recv() => {
                    let Some(request) = request else {
                        return false;
                    };
                    let current = self.current_lifecycle();
                    if request.can_start(current)
                        || request.requests_conflict_reconciliation(current)
                    {
                        self.launch_demand = false;
                        return true;
                    }
                }
            }
        }
    }

    fn shutdown_pending(&mut self) -> bool {
        if self.fatal_error.is_some() {
            return true;
        }
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
        let current = self.current_lifecycle();
        assert!(
            next.command_revision >= current.command_revision
                && next.launch_generation >= current.launch_generation,
            "lifecycle counter moved backwards: {current:?} -> {next:?}"
        );
        self.lifecycle.send_replace(next);
    }

    fn record_launch_success(&self, next_generation: u64) {
        let current = self.current_lifecycle();
        assert_eq!(current.phase, ServerPhase::Reconciling);
        assert_ne!(
            current.reconciliation_evidence,
            ReconciliationEvidence::Positive
        );
        // Every request that observed the pre-launch phase belongs to the same coalesced burst.
        self.publish(LifecycleState {
            phase: ServerPhase::Starting,
            command_revision: current.command_revision,
            launch_generation: next_generation,
            failure: current.failure,
            failure_streak: current.failure_streak,
            phase_started_at: Instant::now(),
            retry_at: None,
            reconciliation_evidence: ReconciliationEvidence::None,
        });
    }

    fn record_launch_failure(&self, next_generation: u64) -> Instant {
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
            command_revision: current.command_revision,
            launch_generation: next_generation,
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
            command_revision: current.command_revision,
            launch_generation: current.launch_generation,
            failure: Some(failure),
            failure_streak,
            phase_started_at: now,
            retry_at: Some(retry_at),
            reconciliation_evidence: ReconciliationEvidence::None,
        });
        retry_at
    }

    fn begin_failure_cleanup(&mut self, failure: FailureCategory) -> Duration {
        let current = self.current_lifecycle();
        assert_eq!(current.phase, ServerPhase::Starting);
        assert_eq!(
            current.reconciliation_evidence,
            ReconciliationEvidence::None
        );
        let failure_streak = current.failure_streak.saturating_add(1);
        self.stopping_reason = Some(StoppingReason::FailureCleanup);
        self.publish(LifecycleState {
            phase: ServerPhase::Stopping,
            command_revision: current.command_revision,
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
            command_revision: current.command_revision,
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
            command_revision: current.command_revision,
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
            command_revision: current.command_revision,
            launch_generation: current.launch_generation,
            failure: current.failure,
            failure_streak: current.failure_streak,
            phase_started_at: running_since,
            retry_at: None,
            reconciliation_evidence: ReconciliationEvidence::None,
        });
        cycle
    }

    fn begin_idle_stop(&mut self, cycle: BackendCycle) {
        let current = self.current_lifecycle();
        assert_eq!(current.owned_running_cycle(), Some(cycle));
        self.stopping_reason = Some(StoppingReason::AutomaticIdle);
        self.publish(LifecycleState {
            phase: ServerPhase::Stopping,
            command_revision: current.command_revision,
            launch_generation: current.launch_generation,
            failure: current.failure,
            failure_streak: current.failure_streak,
            phase_started_at: Instant::now(),
            retry_at: None,
            reconciliation_evidence: ReconciliationEvidence::None,
        });
    }

    fn set_phase(&mut self, phase: ServerPhase) {
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
            command_revision: current.command_revision,
            launch_generation: current.launch_generation,
            failure: current.failure,
            failure_streak: current.failure_streak,
            phase_started_at: Instant::now(),
            retry_at: None,
            reconciliation_evidence: ReconciliationEvidence::None,
        });
        if phase != ServerPhase::Stopping {
            self.stopping_reason = None;
        }
    }

    fn begin_reconciliation(&mut self) -> ReconciliationEvidence {
        let current = self.current_lifecycle();
        assert!(matches!(
            current.phase,
            ServerPhase::Stopped | ServerPhase::Cooldown | ServerPhase::Conflict
        ));
        self.reconciliation_mode = ReconciliationMode::FinalGate;
        self.publish(LifecycleState {
            phase: ServerPhase::Reconciling,
            command_revision: current.command_revision,
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
            command_revision: current.command_revision,
            launch_generation: current.launch_generation,
            failure: current.failure,
            failure_streak: current.failure_streak,
            phase_started_at: Instant::now(),
            retry_at: None,
            reconciliation_evidence: ReconciliationEvidence::None,
        });
    }

    fn publish_clean_stopped_after_reap(&mut self) {
        let current = self.current_lifecycle();
        assert_eq!(current.phase, ServerPhase::Stopping);
        self.publish(LifecycleState {
            phase: ServerPhase::Stopped,
            command_revision: current.command_revision,
            launch_generation: current.launch_generation,
            failure: None,
            failure_streak: 0,
            phase_started_at: Instant::now(),
            retry_at: None,
            reconciliation_evidence: ReconciliationEvidence::None,
        });
        self.stopping_reason = None;
    }

    fn publish_reaped_for_restart(&mut self) {
        let current = self.current_lifecycle();
        assert_eq!(current.phase, ServerPhase::Stopping);
        self.publish(LifecycleState {
            phase: ServerPhase::Stopped,
            command_revision: current.command_revision,
            launch_generation: current.launch_generation,
            failure: current.failure,
            failure_streak: current.failure_streak,
            phase_started_at: Instant::now(),
            retry_at: None,
            reconciliation_evidence: ReconciliationEvidence::None,
        });
        self.stopping_reason = None;
    }

    fn publish_external(&mut self, consume_demand: bool) -> bool {
        let current = self.current_lifecycle();
        assert!(matches!(
            current.phase,
            ServerPhase::Reconciling | ServerPhase::External
        ));
        let launch_generation = if consume_demand {
            let Some(next) = self.next_launch_generation() else {
                return false;
            };
            next
        } else {
            current.launch_generation
        };
        self.publish(LifecycleState {
            phase: ServerPhase::External,
            command_revision: current.command_revision,
            launch_generation,
            failure: current.failure,
            failure_streak: current.failure_streak,
            phase_started_at: Instant::now(),
            retry_at: None,
            reconciliation_evidence: ReconciliationEvidence::Positive,
        });
        true
    }

    fn publish_conflict(&mut self, evidence: ReconciliationEvidence, consume_demand: bool) -> bool {
        assert_ne!(evidence, ReconciliationEvidence::None);
        let current = self.current_lifecycle();
        assert!(matches!(
            current.phase,
            ServerPhase::Reconciling | ServerPhase::External
        ));
        let launch_generation = if consume_demand {
            let Some(next) = self.next_launch_generation() else {
                return false;
            };
            next
        } else {
            current.launch_generation
        };
        self.publish(LifecycleState {
            phase: ServerPhase::Conflict,
            command_revision: current.command_revision,
            launch_generation,
            failure: current.failure,
            failure_streak: current.failure_streak,
            phase_started_at: Instant::now(),
            retry_at: None,
            reconciliation_evidence: evidence,
        });
        true
    }

    async fn wait_for_cooldown(
        &mut self,
        config: &SupervisorConfig,
        retry_at: Instant,
        retry_pending: bool,
    ) -> CooldownOutcome {
        self.launch_demand |= retry_pending;
        loop {
            if self.shutdown_pending() {
                self.set_phase(ServerPhase::Stopped);
                return CooldownOutcome::Shutdown;
            }

            if !self.mutations_closed {
                match self.mutations.try_recv() {
                    Ok(envelope) => {
                        let _ = self.handle_mutation(envelope, config);
                    }
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        self.mutations_closed = true;
                    }
                    Err(mpsc::error::TryRecvError::Empty) => {}
                }
            }

            if Instant::now() >= retry_at {
                if self.launch_demand {
                    self.launch_demand = false;
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
                envelope = self.mutations.recv(), if !self.mutations_closed => {
                    let _ = self.receive_mutation(envelope, config);
                }
                request = self.wake_requests.recv() => {
                    let Some(request) = request else {
                        self.set_phase(ServerPhase::Stopped);
                        return CooldownOutcome::Shutdown;
                    };
                    if request.requests_retry(self.current_lifecycle()) {
                        self.launch_demand = true;
                    }
                }
                () = sleep_until(retry_at) => {}
            }
        }
    }
}

enum ReconciliationWaitOutcome {
    Completed(ReconciliationResult),
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
    let mut mutation_preceded_probe = false;

    loop {
        if control.shutdown_pending() {
            return ReconciliationWaitOutcome::Shutdown;
        }
        if mutation_preceded_probe {
            if let Some(request) = control.wake_requests.try_recv() {
                latch_reconciliation_wake(control, config, request, latch_startup_demand);
            } else if control.wake_requests.is_closed() {
                return ReconciliationWaitOutcome::Shutdown;
            }
            let completed = std::future::poll_fn(|task| {
                Poll::Ready(match probe.as_mut().poll(task) {
                    Poll::Ready(result) => Some(result),
                    Poll::Pending => None,
                })
            })
            .await;
            if let Some(result) = completed {
                return ReconciliationWaitOutcome::Completed(result);
            }
        }

        tokio::select! {
            biased;

            _ = &mut control.shutdown => {
                return ReconciliationWaitOutcome::Shutdown;
            }
            envelope = control.mutations.recv(), if !control.mutations_closed => {
                let _ = control.receive_mutation(envelope, config);
                mutation_preceded_probe = true;
            }
            request = control.wake_requests.recv() => {
                let Some(request) = request else {
                    return ReconciliationWaitOutcome::Shutdown;
                };
                latch_reconciliation_wake(control, config, request, latch_startup_demand);
            }
            result = &mut probe => {
                return ReconciliationWaitOutcome::Completed(result);
            }
        }
    }
}

fn latch_reconciliation_wake(
    control: &mut SupervisorControl,
    config: &SupervisorConfig,
    request: WakeRequest,
    latch_startup_demand: bool,
) {
    if latch_startup_demand
        && !control.launch_demand
        && request.observed == control.current_lifecycle()
    {
        control.launch_demand = true;
        server_info!(
            config,
            "Latched one server wake-up request during startup reconciliation"
        );
    } else {
        server_debug!(
            config,
            "Ignoring coalesced server wake-up request during reconciliation"
        );
    }
}

fn publish_conflict_observation(
    control: &mut SupervisorControl,
    config: &SupervisorConfig,
    result: &ReconciliationResult,
    evidence: ReconciliationEvidence,
    consume_demand: bool,
    last_conflict: &mut Option<ConflictClassification>,
) {
    let sticky_positive = evidence == ReconciliationEvidence::Positive;
    let classification = result.conflict_classification(sticky_positive);
    if !control.publish_conflict(evidence, consume_demand) {
        return;
    }
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
    control: &mut SupervisorControl,
    config: &SupervisorConfig,
    consume_demand: bool,
    last_conflict: &mut Option<ConflictClassification>,
) {
    if !control.publish_external(consume_demand) {
        return;
    }
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
    let ReconciliationWaitOutcome::Completed(result) =
        wait_for_reconciliation(control, config, backend, true).await
    else {
        return StartupReconciliationOutcome::Shutdown;
    };
    let startup_demand = std::mem::take(&mut control.launch_demand);
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
    let ReconciliationWaitOutcome::Completed(result) =
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
        if control.shutdown_pending() {
            return ExternalMonitorOutcome::Shutdown;
        }

        tokio::select! {
            biased;

            _ = &mut control.shutdown => return ExternalMonitorOutcome::Shutdown,
            envelope = control.mutations.recv(), if !control.mutations_closed => {
                let _ = control.receive_mutation(envelope, config);
            }
            request = control.wake_requests.recv() => {
                if request.is_none() {
                    return ExternalMonitorOutcome::Shutdown;
                }
                server_debug!(config, "Ignoring stale server wake-up request while proxying an external backend");
                continue;
            }
            () = sleep_until(next_poll) => {}
        }

        let ReconciliationWaitOutcome::Completed(result) =
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

// This is the single composition root for the supervisor's owned channels and state-machine loop.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(crate) async fn run(
    wake_requests: WakeLatchReceiver,
    mutations: mpsc::Receiver<MutationEnvelope>,
    quiescence: mpsc::Sender<QuiescenceRequest>,
    fatal: FatalSignal,
    runtime_drained: watch::Receiver<bool>,
    shutdown: oneshot::Receiver<()>,
    lifecycle: watch::Sender<LifecycleState>,
    config: SupervisorConfig,
    backend: BackendEndpoint,
    backend_use: Arc<BackendUseCoordinator>,
) -> Result<()> {
    let mut control = SupervisorControl {
        wake_requests,
        mutations,
        shutdown,
        lifecycle,
        last_admission: None,
        launch_demand: false,
        stopping_reason: None,
        reconciliation_mode: ReconciliationMode::Initial,
        fatal,
        runtime_drained,
        mutations_closed: false,
        quiescence,
        fatal_error: None,
    };
    assert_eq!(control.current_lifecycle().phase, ServerPhase::Reconciling);
    assert!(control.current_lifecycle().invariant_holds());
    let mut last_conflict = None;
    let mut launch_pending =
        match reconcile_startup(&mut control, &config, &backend, &mut last_conflict).await {
            StartupReconciliationOutcome::Launch => true,
            StartupReconciliationOutcome::Idle => false,
            StartupReconciliationOutcome::Shutdown => return control.terminal_result(),
        };

    loop {
        if control.current_lifecycle().phase == ServerPhase::External {
            match monitor_external_server(&mut control, &config, &backend, &mut last_conflict).await
            {
                ExternalMonitorOutcome::Conflict => {}
                ExternalMonitorOutcome::Shutdown => return control.terminal_result(),
            }
        }

        if !launch_pending && !control.wait_for_reconciliation_demand(&config).await {
            if control.current_lifecycle().phase == ServerPhase::Stopped {
                control.set_phase(ServerPhase::Stopped);
            }
            return control.terminal_result();
        }
        launch_pending = false;

        if control.shutdown_pending() {
            return control.terminal_result();
        }

        match reconcile_final_gate(&mut control, &config, &backend, &mut last_conflict).await {
            FinalGateOutcome::Launch => {}
            FinalGateOutcome::Blocked => continue,
            FinalGateOutcome::Shutdown => return control.terminal_result(),
        }

        let Some(next_generation) = control.next_launch_generation() else {
            return control.terminal_result();
        };
        let child = match process::launch(&config.launch) {
            Ok(child) => {
                server_info!(
                    config,
                    "Launched server command {:?} {:?}",
                    config.launch.program,
                    config.launch.arguments
                );
                control.record_launch_success(next_generation);
                child
            }
            Err(error) => {
                server_error!(config, "Unable to start the Minecraft server: {error:#}");
                let retry_at = control.record_launch_failure(next_generation);
                log_current_failure_cooldown(&config, &control);
                match control.wait_for_cooldown(&config, retry_at, false).await {
                    CooldownOutcome::Launch => launch_pending = true,
                    CooldownOutcome::Stopped => {}
                    CooldownOutcome::Shutdown => return control.terminal_result(),
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
                launch_pending = true;
            }
            CycleOutcome::Failed {
                retry_at,
                retry_pending,
            } => {
                log_current_failure_cooldown(&config, &control);
                match control
                    .wait_for_cooldown(&config, retry_at, retry_pending)
                    .await
                {
                    CooldownOutcome::Launch => launch_pending = true,
                    CooldownOutcome::Stopped => {}
                    CooldownOutcome::Shutdown => return control.terminal_result(),
                }
            }
            CycleOutcome::Shutdown => {
                if control.fatal_error.is_none() {
                    control.set_phase(ServerPhase::Stopped);
                }
                return control.terminal_result();
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
            return stop_for_shutdown(child, control, config, backend_use).await;
        }
        ReadinessOutcome::ExplicitMutation => {
            return stop_explicit_server(child, control, config, backend_use).await;
        }
    }

    if control.shutdown_pending() {
        return stop_for_shutdown(child, control, config, backend_use).await;
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

fn readiness_deadline_outcome(
    child: &mut OwnedProcess,
    control: &mut SupervisorControl,
    config: &SupervisorConfig,
) -> ReadinessOutcome {
    match child.try_wait() {
        Ok(Some(status)) => return ReadinessOutcome::Exited(status),
        Ok(None) => {}
        Err(error) => return ReadinessOutcome::ProcessWaitFailed(error),
    }
    if !control.mutations_closed {
        match control.mutations.try_recv() {
            Ok(envelope) => {
                if control.handle_mutation(envelope, config) == MutationEffect::StopOwned {
                    return ReadinessOutcome::ExplicitMutation;
                }
            }
            Err(mpsc::error::TryRecvError::Disconnected) => {
                control.mutations_closed = true;
            }
            Err(mpsc::error::TryRecvError::Empty) => {}
        }
    }
    if control.shutdown_pending() {
        return ReadinessOutcome::Shutdown;
    }
    match child.try_wait() {
        Ok(Some(status)) => ReadinessOutcome::Exited(status),
        Ok(None) => ReadinessOutcome::StartupTimedOut,
        Err(error) => ReadinessOutcome::ProcessWaitFailed(error),
    }
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
        if control.shutdown_pending() {
            return ReadinessOutcome::Shutdown;
        }
        if Instant::now() >= deadline {
            return readiness_deadline_outcome(child, control, config);
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
            status = child.wait() => {
                return match status {
                    Ok(status) => ReadinessOutcome::Exited(status),
                    Err(error) => ReadinessOutcome::ProcessWaitFailed(error),
                };
            }
            envelope = control.mutations.recv(), if !control.mutations_closed => {
                if control.receive_mutation(envelope, config) == MutationEffect::StopOwned {
                    return ReadinessOutcome::ExplicitMutation;
                }
                continue;
            }
            request = control.wake_requests.recv() => {
                if request.is_none() {
                    return ReadinessOutcome::Shutdown;
                }
                server_debug!(config, "Ignoring duplicate start request during startup");
                continue;
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
            status = child.wait() => {
                return match status {
                    Ok(status) => ReadinessOutcome::Exited(status),
                    Err(error) => ReadinessOutcome::ProcessWaitFailed(error),
                };
            }
            envelope = control.mutations.recv(), if !control.mutations_closed => {
                if control.receive_mutation(envelope, config) == MutationEffect::StopOwned {
                    return ReadinessOutcome::ExplicitMutation;
                }
            }
            request = control.wake_requests.recv() => {
                if request.is_none() {
                    return ReadinessOutcome::Shutdown;
                }
                server_debug!(config, "Ignoring duplicate start request during startup");
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
    ExplicitMutation,
}

enum FinalRconCheck {
    Zero,
    Veto(Option<RconClient>),
    Shutdown,
    ChildWait(std::io::Result<ExitStatus>),
    ExplicitMutation,
}

enum IdlePlayerCheck {
    Confirmed,
    Veto,
    Shutdown,
    ChildWait(std::io::Result<ExitStatus>),
    ExplicitMutation,
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
    ExplicitMutation,
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
        if control.shutdown_pending() {
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
            status = child.wait() => return IdleExclusiveOutcome::ChildWait(status),
            envelope = control.mutations.recv(), if !control.mutations_closed => {
                if control.receive_mutation(envelope, config) == MutationEffect::StopOwned {
                    return IdleExclusiveOutcome::ExplicitMutation;
                }
            }
            request = control.wake_requests.recv() => {
                if request.is_none() {
                    return IdleExclusiveOutcome::Shutdown;
                }
                server_debug!(config, "Ignoring start request because the server is already running");
            }
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
        ExplicitMutation,
    }

    let outcome = {
        let command = timeout(config.command_timeout, client.command("list"));
        tokio::pin!(command);
        loop {
            if control.shutdown_pending() {
                break WaitOutcome::Shutdown;
            }
            tokio::select! {
                biased;

                _ = &mut control.shutdown => break WaitOutcome::Shutdown,
                status = child.wait() => break WaitOutcome::ChildWait(status),
                envelope = control.mutations.recv(), if !control.mutations_closed => {
                    if control.receive_mutation(envelope, config) == MutationEffect::StopOwned {
                        break WaitOutcome::ExplicitMutation;
                    }
                }
                request = control.wake_requests.recv() => {
                    if request.is_none() {
                        break WaitOutcome::Shutdown;
                    }
                    server_debug!(config, "Ignoring start request because the server is already running");
                }
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
        WaitOutcome::ExplicitMutation => FinalRconCheck::ExplicitMutation,
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
        FinalRconCheck::ExplicitMutation => IdlePlayerCheck::ExplicitMutation,
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
            IdleExclusiveOutcome::ExplicitMutation => {
                return RunningMonitorOutcome::ExplicitMutation;
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
        IdlePlayerCheck::ExplicitMutation => return RunningMonitorOutcome::ExplicitMutation,
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
    if control.shutdown_pending() {
        return RunningMonitorOutcome::Shutdown;
    }
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
    if next_poll.is_some_and(|deadline| Instant::now() >= deadline) {
        return poll_player_presence_responsive(child, control, config, state).await;
    }
    let stable_at = state.stable_at;
    let stability_pending = state.stability_pending;
    tokio::select! {
        biased;

        _ = &mut control.shutdown => RunningMonitorOutcome::Shutdown,
        status = child.wait() => RunningMonitorOutcome::ChildWait(status),
        envelope = control.mutations.recv(), if !control.mutations_closed => {
            if control.receive_mutation(envelope, config) == MutationEffect::StopOwned {
                RunningMonitorOutcome::ExplicitMutation
            } else {
                RunningMonitorOutcome::Continue
            }
        }
        request = control.wake_requests.recv() => {
            if request.is_none() {
                RunningMonitorOutcome::Shutdown
            } else {
                server_debug!(config, "Ignoring start request because the server is already running");
                RunningMonitorOutcome::Continue
            }
        }
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

async fn poll_player_presence_responsive(
    child: &mut OwnedProcess,
    control: &mut SupervisorControl,
    config: &SupervisorConfig,
    state: &mut RunningMonitorState,
) -> RunningMonitorOutcome {
    let poll = poll_player_presence(&mut state.player_inspection, config);
    tokio::pin!(poll);
    loop {
        if control.shutdown_pending() {
            return RunningMonitorOutcome::Shutdown;
        }
        tokio::select! {
            biased;

            _ = &mut control.shutdown => return RunningMonitorOutcome::Shutdown,
            status = child.wait() => return RunningMonitorOutcome::ChildWait(status),
            envelope = control.mutations.recv(), if !control.mutations_closed => {
                if control.receive_mutation(envelope, config) == MutationEffect::StopOwned {
                    return RunningMonitorOutcome::ExplicitMutation;
                }
            }
            request = control.wake_requests.recv() => {
                if request.is_none() {
                    return RunningMonitorOutcome::Shutdown;
                }
            }
            () = &mut poll => return RunningMonitorOutcome::Continue,
        }
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
                return stop_for_shutdown(child, control, config, backend_use).await;
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
            RunningMonitorOutcome::ExplicitMutation => {
                drop(state);
                return stop_explicit_server(child, control, config, backend_use).await;
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
    if let Err(cleanup_error) = stop_and_reap(&mut child, config).await {
        let forced_cleanup = force_and_reap(&mut child, config).await;
        return match forced_cleanup {
            Ok(()) => Err(wait_error).context(format!(
                "cleanup failed after server process wait error: {cleanup_error:#}"
            )),
            Err(forced_error) => Err(wait_error).context(format!(
                "cleanup failed after server process wait error: {cleanup_error:#}; forced cleanup also failed: {forced_error:#}"
            )),
        };
    }
    control.set_phase(ServerPhase::Stopped);
    Err(wait_error).context("failed to wait for the server process")
}

struct QuiescenceOutcome {
    exclusive: OwnedRwLockWriteGuard<()>,
    child_status: Option<std::io::Result<ExitStatus>>,
    shutdown_received: bool,
}

#[allow(clippy::too_many_lines)]
async fn quiesce_runtime_and_acquire_exclusive(
    child: &mut OwnedProcess,
    control: &mut SupervisorControl,
    config: &SupervisorConfig,
    backend_use: &BackendUseCoordinator,
) -> Result<QuiescenceOutcome> {
    let deadline = Instant::now() + config.command_timeout;
    let mut exclusive = Box::pin(backend_use.acquire_exclusive());
    let mut guard = std::future::poll_fn(|task| {
        Poll::Ready(match exclusive.as_mut().poll(task) {
            Poll::Ready(guard) => Some(guard),
            Poll::Pending => None,
        })
    })
    .await;

    let (acknowledged, mut acknowledgement) = oneshot::channel();
    control
        .quiescence
        .try_send(QuiescenceRequest { acknowledged })
        .map_err(|error| anyhow::anyhow!("runtime quiescence request failed: {error}"))?;
    let mut child_status = None;
    let mut shutdown_received = false;
    let mut acknowledgement_open = true;

    loop {
        if shutdown_received && *control.runtime_drained.borrow_and_update() {
            break;
        }
        tokio::select! {
            biased;

            _ = &mut control.shutdown, if !shutdown_received => {
                shutdown_received = true;
            }
            status = child.wait(), if child_status.is_none() => {
                child_status = Some(status);
            }
            envelope = control.mutations.recv(), if !control.mutations_closed => {
                let _ = control.receive_mutation(envelope, config);
                if control.fatal_error.is_some() {
                    anyhow::bail!("supervisor entered fatal shutdown during runtime quiescence");
                }
            }
            request = control.wake_requests.recv(), if !shutdown_received => {
                let Some(request) = request else {
                    shutdown_received = true;
                    continue;
                };
                if request.requests_restart(control.current_lifecycle()) {
                    control.launch_demand = true;
                }
            }
            result = &mut acknowledgement, if acknowledgement_open => {
                match result {
                    Ok(result) => {
                        result?;
                        break;
                    }
                    Err(_) if shutdown_received => {
                        acknowledgement_open = false;
                    }
                    Err(error) => {
                        return Err(error)
                            .context("runtime quiescence acknowledgement channel closed");
                    }
                }
            }
            changed = control.runtime_drained.changed(), if shutdown_received => {
                changed.context("runtime-drain indication closed during daemon shutdown")?;
            }
            () = sleep_until(deadline) => {
                anyhow::bail!("runtime quiescence timed out");
            }
        }
    }

    if let Some(guard) = guard.take() {
        return Ok(QuiescenceOutcome {
            exclusive: guard,
            child_status,
            shutdown_received,
        });
    }
    loop {
        tokio::select! {
            biased;

            _ = &mut control.shutdown, if !shutdown_received => {
                shutdown_received = true;
            }
            status = child.wait(), if child_status.is_none() => {
                child_status = Some(status);
            }
            envelope = control.mutations.recv(), if !control.mutations_closed => {
                let _ = control.receive_mutation(envelope, config);
                if control.fatal_error.is_some() {
                    anyhow::bail!("supervisor entered fatal shutdown while acquiring backend-use exclusivity");
                }
            }
            request = control.wake_requests.recv(), if !shutdown_received => {
                let Some(request) = request else {
                    shutdown_received = true;
                    continue;
                };
                if request.requests_restart(control.current_lifecycle()) {
                    control.launch_demand = true;
                }
            }
            acquired = &mut exclusive => return Ok(QuiescenceOutcome {
                exclusive: acquired,
                child_status,
                shutdown_received,
            }),
            () = sleep_until(deadline) => {
                anyhow::bail!("exclusive backend-use acquisition timed out after runtime quiescence");
            }
        }
    }
}

async fn stop_explicit_server(
    mut child: OwnedProcess,
    control: &mut SupervisorControl,
    config: &SupervisorConfig,
    backend_use: &BackendUseCoordinator,
) -> Result<CycleOutcome> {
    assert!(matches!(
        control.stopping_reason,
        Some(StoppingReason::ExplicitStop | StoppingReason::ExplicitRestart)
    ));
    let quiescence =
        match quiesce_runtime_and_acquire_exclusive(&mut child, control, config, backend_use).await
        {
            Ok(quiescence) => quiescence,
            Err(error) => {
                let fatal =
                    error.context("explicit lifecycle mutation could not safely quiesce proxy use");
                if control.fatal_error.is_none() {
                    control.fatal_error = Some(fatal);
                }
                control.fatal.raise();
                return stop_for_shutdown(child, control, config, backend_use).await;
            }
        };
    let QuiescenceOutcome {
        exclusive,
        child_status,
        mut shutdown_received,
    } = quiescence;

    let child_already_reaped = child_status.is_some();
    if let Some(status) = child_status {
        match status {
            Ok(status) => log_process_exit(config, status),
            Err(wait_error) => {
                let cleanup = force_and_reap(&mut child, config).await;
                control.fatal.raise();
                return match cleanup {
                    Ok(()) => Err(wait_error).context("failed to wait for the server process"),
                    Err(cleanup_error) => Err(wait_error).context(format!(
                        "failed to wait for the server process; forced cleanup also failed: {cleanup_error:#}"
                    )),
                };
            }
        }
    }

    let cleanup = async {
        if child_already_reaped {
            Ok(())
        } else {
            stop_and_reap(&mut child, config).await
        }
    };
    let (cleanup_result, observed_shutdown) =
        wait_for_explicit_cleanup(cleanup, control, config, shutdown_received).await;
    shutdown_received = observed_shutdown;
    drop(exclusive);
    if let Err(error) = cleanup_result {
        let fatal = error.context("explicit lifecycle cleanup or reap failed");
        control.fatal.raise();
        return match force_and_reap(&mut child, config).await {
            Ok(()) => Err(fatal),
            Err(cleanup_error) => {
                Err(fatal.context(format!("forced cleanup also failed: {cleanup_error:#}")))
            }
        };
    }
    let restart = control.stopping_reason == Some(StoppingReason::ExplicitRestart);
    if restart {
        control.publish_reaped_for_restart();
    } else {
        control.publish_clean_stopped_after_reap();
    }
    Ok(if shutdown_received {
        control.launch_demand = false;
        CycleOutcome::Shutdown
    } else if control.launch_demand {
        control.launch_demand = false;
        CycleOutcome::Restart
    } else {
        CycleOutcome::Stopped
    })
}

async fn wait_for_explicit_cleanup<F>(
    cleanup: F,
    control: &mut SupervisorControl,
    config: &SupervisorConfig,
    mut shutdown_received: bool,
) -> (Result<()>, bool)
where
    F: Future<Output = Result<()>>,
{
    tokio::pin!(cleanup);
    let result = loop {
        tokio::select! {
            biased;

            _ = &mut control.shutdown, if !shutdown_received => {
                shutdown_received = true;
            }
            result = &mut cleanup => break result,
            envelope = control.mutations.recv(), if !control.mutations_closed => {
                let _ = control.receive_mutation(envelope, config);
            }
            request = control.wake_requests.recv() => {
                if let Some(request) = request
                    && request.requests_restart(control.current_lifecycle())
                {
                    control.launch_demand = true;
                }
            }
        }
    };
    (result, shutdown_received)
}

async fn force_and_reap(child: &mut OwnedProcess, config: &SupervisorConfig) -> Result<()> {
    let termination_result = process::terminate(child).await;
    match timeout(Duration::from_secs(5), child.wait()).await {
        Ok(Ok(status)) => log_process_exit(config, status),
        Ok(Err(error)) => return Err(error).context("failed to reap the server process"),
        Err(_) => anyhow::bail!("server process did not exit after forced termination"),
    }
    termination_result
}

async fn stop_for_shutdown(
    mut child: OwnedProcess,
    control: &mut SupervisorControl,
    config: &SupervisorConfig,
    backend_use: &BackendUseCoordinator,
) -> Result<CycleOutcome> {
    control.stopping_reason = Some(StoppingReason::DaemonShutdown);
    control.set_phase(ServerPhase::Stopping);
    if control.fatal_error.is_some() {
        let deadline = Instant::now() + config.command_timeout;
        let drain_result = async {
            while !*control.runtime_drained.borrow_and_update() {
                control
                    .runtime_drained
                    .changed()
                    .await
                    .context("fatal runtime-drain indication closed")?;
            }
            Result::<()>::Ok(())
        };
        let drain_confirmed = timeout_at(deadline, drain_result).await;
        let exclusive = timeout_at(deadline, backend_use.acquire_exclusive()).await;
        if !matches!(drain_confirmed, Ok(Ok(()))) || exclusive.is_err() {
            let original = control
                .fatal_error
                .take()
                .expect("fatal cleanup must retain its original cause");
            control.fatal_error = Some(original.context(
                "runtime drain or backend-use exclusivity remained uncertain during fatal cleanup",
            ));
        }
        if let Err(cleanup_error) = force_and_reap(&mut child, config).await {
            let original = control
                .fatal_error
                .take()
                .expect("fatal cleanup must retain its original cause");
            return Err(original.context(format!(
                "forced process cleanup failed during fatal shutdown: {cleanup_error:#}"
            )));
        }
        return Ok(CycleOutcome::Shutdown);
    }
    if let Err(cleanup_error) = stop_and_reap(&mut child, config).await {
        return match force_and_reap(&mut child, config).await {
            Ok(()) => Err(cleanup_error).context("server shutdown cleanup initially failed"),
            Err(forced_error) => Err(cleanup_error).context(format!(
                "server shutdown cleanup failed; forced cleanup also failed: {forced_error:#}"
            )),
        };
    }
    Ok(CycleOutcome::Shutdown)
}

async fn stop_failed_server(
    mut child: OwnedProcess,
    control: &mut SupervisorControl,
    config: &SupervisorConfig,
    failure: FailureCategory,
) -> Result<CycleOutcome> {
    let retry_delay = control.begin_failure_cleanup(failure);
    let outcome = {
        let cleanup = stop_and_reap(&mut child, config);
        finish_failed_cleanup(cleanup, control, config, retry_delay).await
    };
    match outcome {
        Ok(outcome) => Ok(outcome),
        Err(error) => {
            control.fatal.raise();
            match force_and_reap(&mut child, config).await {
                Ok(()) => Err(error),
                Err(forced_error) => Err(error.context(format!(
                    "forced cleanup also failed after failure cleanup: {forced_error:#}"
                ))),
            }
        }
    }
}

async fn finish_failed_cleanup<F>(
    cleanup: F,
    control: &mut SupervisorControl,
    config: &SupervisorConfig,
    retry_delay: Duration,
) -> Result<CycleOutcome>
where
    F: Future<Output = Result<()>>,
{
    tokio::pin!(cleanup);
    let mut shutdown_received = false;
    loop {
        tokio::select! {
            biased;

            _ = &mut control.shutdown, if !shutdown_received => {
                shutdown_received = true;
            }
            result = &mut cleanup => {
                result?;
                break;
            }
            envelope = control.mutations.recv(), if !control.mutations_closed => {
                let _ = control.receive_mutation(envelope, config);
            }
            request = control.wake_requests.recv(), if !control.launch_demand && !shutdown_received => {
                let Some(request) = request else {
                    shutdown_received = true;
                    continue;
                };
                if request.requests_retry(control.current_lifecycle()) {
                    control.launch_demand = true;
                }
            }
        }
    }
    if shutdown_received || control.shutdown_pending() {
        return Ok(CycleOutcome::Shutdown);
    }
    assert_eq!(
        control.stopping_reason,
        Some(StoppingReason::FailureCleanup)
    );
    let retry_at = control.finish_failure_cleanup(retry_delay);
    Ok(CycleOutcome::Failed {
        retry_at,
        retry_pending: control.launch_demand,
    })
}

async fn stop_committed_idle_server(
    mut child: OwnedProcess,
    control: &mut SupervisorControl,
    config: &SupervisorConfig,
) -> Result<CycleOutcome> {
    let stopping = control.current_lifecycle();
    assert_eq!(stopping.phase, ServerPhase::Stopping);
    let cleanup = stop_and_reap(&mut child, config);
    let (cleanup_result, mut restart, _shutdown_received) =
        wait_for_idle_cleanup(cleanup, control, config).await;
    if let Err(error) = cleanup_result {
        control.fatal.raise();
        return match force_and_reap(&mut child, config).await {
            Ok(()) => Err(error),
            Err(forced_error) => Err(error.context(format!(
                "forced cleanup also failed after automatic idle cleanup: {forced_error:#}"
            ))),
        };
    }

    if control.shutdown_pending() {
        return Ok(CycleOutcome::Shutdown);
    }

    loop {
        match control.wake_requests.try_recv() {
            Some(request) if request.requests_restart(control.current_lifecycle()) => {
                restart = true;
            }
            Some(_) => server_debug!(
                config,
                "Ignoring stale server wake-up request while stopping"
            ),
            None => break,
        }
    }

    Ok(if restart || control.launch_demand {
        control.launch_demand = false;
        control.publish_reaped_for_restart();
        CycleOutcome::Restart
    } else {
        CycleOutcome::Stopped
    })
}

async fn wait_for_idle_cleanup<F>(
    cleanup: F,
    control: &mut SupervisorControl,
    config: &SupervisorConfig,
) -> (Result<()>, bool, bool)
where
    F: Future<Output = Result<()>>,
{
    tokio::pin!(cleanup);
    let mut restart = false;
    let mut shutdown_received = false;
    let result = loop {
        tokio::select! {
            biased;

            _ = &mut control.shutdown, if !shutdown_received => {
                shutdown_received = true;
            }
            result = &mut cleanup => break result,
            envelope = control.mutations.recv(), if !control.mutations_closed => {
                let _ = control.receive_mutation(envelope, config);
                restart = control.launch_demand;
            }
            request = control.wake_requests.recv(), if !restart && !shutdown_received => {
                let Some(request) = request else {
                    shutdown_received = true;
                    continue;
                };
                if request.requests_restart(control.current_lifecycle()) {
                    restart = true;
                    control.launch_demand = true;
                } else {
                    server_debug!(config, "Ignoring stale server wake-up request while stopping");
                }
            }
        }
    };
    (result, restart, shutdown_received)
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum GracefulStopDelivery {
    NotSent,
    Sent,
    Uncertain,
}

async fn stop_and_reap(child: &mut OwnedProcess, config: &SupervisorConfig) -> Result<()> {
    let mut wait_error = None;
    let command_deadline = Instant::now() + config.command_timeout;
    let graceful_stop = if let Some(rcon_config) = &config.rcon {
        match timeout_at(
            command_deadline,
            RconClient::connect(&rcon_config.endpoint, rcon_config.password.expose()),
        )
        .await
        {
            Ok(Ok(mut client)) => match timeout_at(command_deadline, client.stop()).await {
                Ok(Ok(())) => GracefulStopDelivery::Sent,
                Ok(Err(error)) => {
                    server_warn!(
                        config,
                        "RCON stop delivery became uncertain; not sending a duplicate console command: {error:#}"
                    );
                    GracefulStopDelivery::Uncertain
                }
                Err(_) => {
                    server_warn!(
                        config,
                        "RCON stop delivery timed out and is uncertain; not sending a duplicate console command"
                    );
                    GracefulStopDelivery::Uncertain
                }
            },
            Ok(Err(error)) => {
                server_warn!(
                    config,
                    "Failed to establish the RCON stop session; trying the owned console: {error:#}"
                );
                if send_console_stop_before(child, config, command_deadline).await {
                    GracefulStopDelivery::Sent
                } else {
                    GracefulStopDelivery::NotSent
                }
            }
            Err(_) => {
                server_warn!(
                    config,
                    "Establishing the RCON stop session timed out; trying the owned console"
                );
                if send_console_stop_before(child, config, command_deadline).await {
                    GracefulStopDelivery::Sent
                } else {
                    GracefulStopDelivery::NotSent
                }
            }
        }
    } else if send_console_stop_before(child, config, command_deadline).await {
        GracefulStopDelivery::Sent
    } else {
        GracefulStopDelivery::NotSent
    };

    if graceful_stop != GracefulStopDelivery::NotSent {
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

fn parse_player_count(response: &str) -> Result<u32> {
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
mod tests;
