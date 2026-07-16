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
}

impl MutationOperation {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::Stop => "stop",
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
        current.phase == ServerPhase::Stopping
            && current.failure.is_none()
            && self.observed == current
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
    ExplicitStop,
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
    Explicit,
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
                Some(StoppingReason::Explicit) => {
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
                stopping_reason = Some(StoppingReason::Explicit);
                effect = MutationEffect::StopOwned;
                MutationDisposition::Accepted
            }
            ServerPhase::Stopped | ServerPhase::Cooldown => MutationDisposition::AlreadySatisfied,
            ServerPhase::Stopping => match stopping_reason {
                Some(StoppingReason::AutomaticIdle) => {
                    stopping_reason = Some(StoppingReason::Explicit);
                    MutationDisposition::Accepted
                }
                Some(StoppingReason::FailureCleanup) => {
                    if self.launch_demand {
                        MutationDisposition::Accepted
                    } else {
                        MutationDisposition::AlreadySatisfied
                    }
                }
                Some(StoppingReason::Explicit) => {
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
            failure: None,
            failure_streak: 0,
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
                control.set_phase(ServerPhase::Stopped);
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
        ReadinessOutcome::ExplicitStop => {
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
                    return ReadinessOutcome::ExplicitStop;
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
                    return ReadinessOutcome::ExplicitStop;
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
                    return ReadinessOutcome::ExplicitStop;
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
    ExplicitStop,
}

enum FinalRconCheck {
    Zero,
    Veto(Option<RconClient>),
    Shutdown,
    ChildWait(std::io::Result<ExitStatus>),
    ExplicitStop,
}

enum IdlePlayerCheck {
    Confirmed,
    Veto,
    Shutdown,
    ChildWait(std::io::Result<ExitStatus>),
    ExplicitStop,
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
    ExplicitStop,
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
                    return IdleExclusiveOutcome::ExplicitStop;
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
        ExplicitStop,
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
                        break WaitOutcome::ExplicitStop;
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
        WaitOutcome::ExplicitStop => FinalRconCheck::ExplicitStop,
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
        FinalRconCheck::ExplicitStop => IdlePlayerCheck::ExplicitStop,
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
            IdleExclusiveOutcome::ExplicitStop => return RunningMonitorOutcome::ExplicitStop,
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
        IdlePlayerCheck::ExplicitStop => return RunningMonitorOutcome::ExplicitStop,
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
                RunningMonitorOutcome::ExplicitStop
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
                    return RunningMonitorOutcome::ExplicitStop;
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
            RunningMonitorOutcome::ExplicitStop => {
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
    assert_eq!(control.stopping_reason, Some(StoppingReason::Explicit));
    let quiescence =
        match quiesce_runtime_and_acquire_exclusive(&mut child, control, config, backend_use).await
        {
            Ok(quiescence) => quiescence,
            Err(error) => {
                let fatal = error.context("explicit stop could not safely quiesce proxy use");
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
        let fatal = error.context("explicit stop cleanup or reap failed");
        control.fatal.raise();
        return match force_and_reap(&mut child, config).await {
            Ok(()) => Err(fatal),
            Err(cleanup_error) => {
                Err(fatal.context(format!("forced cleanup also failed: {cleanup_error:#}")))
            }
        };
    }
    control.publish_clean_stopped_after_reap();
    Ok(if shutdown_received {
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
    assert_eq!(stopping.failure, None);
    assert_eq!(stopping.failure_streak, 0);
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
    const TEST_RCON_END_MARKER_DELAY: Duration = Duration::from_millis(3);

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

    struct PausedTimeAutoAdvanceGuard {
        release: Option<std::sync::mpsc::Sender<()>>,
        blocking_task: JoinHandle<()>,
    }

    impl PausedTimeAutoAdvanceGuard {
        async fn start() -> Self {
            let (started_sender, started_receiver) = oneshot::channel();
            let (release, wait_for_release) = std::sync::mpsc::channel();
            let blocking_task = tokio::task::spawn_blocking(move || {
                let _ = started_sender.send(());
                let _ = wait_for_release.recv();
            });
            started_receiver
                .await
                .expect("paused-time auto-advance inhibitor should start");

            Self {
                release: Some(release),
                blocking_task,
            }
        }

        fn release(&mut self) {
            if let Some(release) = self.release.take() {
                let _ = release.send(());
            }
        }

        async fn finish(mut self) {
            self.release();
            (&mut self.blocking_task)
                .await
                .expect("paused-time auto-advance inhibitor should not panic");
        }
    }

    impl Drop for PausedTimeAutoAdvanceGuard {
        fn drop(&mut self) {
            self.release();
        }
    }

    struct ScriptedRconServer {
        task: Option<JoinHandle<()>>,
    }

    impl ScriptedRconServer {
        fn abort(&self) {
            self.task
                .as_ref()
                .expect("scripted RCON server task should be present")
                .abort();
        }

        async fn join(mut self) -> std::result::Result<(), tokio::task::JoinError> {
            let result = self
                .task
                .as_mut()
                .expect("scripted RCON server task should be present")
                .await;
            self.task.take();
            result
        }
    }

    impl Drop for ScriptedRconServer {
        fn drop(&mut self) {
            if let Some(task) = &self.task {
                task.abort();
            }
        }
    }

    type ScriptedRconFixture = (
        Endpoint,
        mpsc::UnboundedReceiver<TestRconEvent>,
        ScriptedRconServer,
    );

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

    async fn try_write_test_rcon_packet(
        stream: &mut TcpStream,
        id: i32,
        kind: i32,
        body: &[u8],
    ) -> std::io::Result<()> {
        stream
            .write_i32_le(i32::try_from(body.len() + 10).unwrap())
            .await?;
        stream.write_i32_le(id).await?;
        stream.write_i32_le(kind).await?;
        stream.write_all(body).await?;
        stream.write_all(&[0, 0]).await
    }

    async fn write_test_rcon_packet(stream: &mut TcpStream, id: i32, kind: i32, body: &[u8]) {
        try_write_test_rcon_packet(stream, id, kind, body)
            .await
            .expect("test RCON packet should be written");
    }

    fn expected_test_peer_close(error: &std::io::Error) -> bool {
        matches!(
            error.kind(),
            std::io::ErrorKind::UnexpectedEof
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::BrokenPipe
        )
    }

    async fn accept_authenticated_test_rcon_client(listener: &TcpListener) -> TcpStream {
        let (mut stream, _) = listener
            .accept()
            .await
            .expect("test RCON client should connect");
        let auth = read_test_rcon_packet(&mut stream).await;
        assert_eq!(auth.kind, TEST_AUTH);
        assert_eq!(auth.body, b"secret");
        write_test_rcon_packet(&mut stream, auth.id, TEST_AUTH_RESPONSE, b"").await;
        stream
    }

    async fn spawn_scripted_list_rcon_server(replies: Vec<TestListReply>) -> ScriptedRconFixture {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test RCON listener should bind");
        let address = endpoint(listener.local_addr().unwrap());
        let (events, event_receiver) = mpsc::unbounded_channel();
        // Tokio keeps paused time fixed while a blocking task is active, but explicit time
        // advances remain available to tests that exercise idle deadlines.
        let auto_advance_guard = PausedTimeAutoAdvanceGuard::start().await;
        let task = tokio::spawn(async move {
            let auto_advance_guard = auto_advance_guard;
            let mut replies = std::collections::VecDeque::from(replies);
            let mut list_index = 0;
            'connections: loop {
                let mut stream = accept_authenticated_test_rcon_client(&listener).await;

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
                    // RconClient intentionally paces its end marker; the inhibitor makes that
                    // otherwise automatic virtual-time advance explicit.
                    tokio::time::advance(TEST_RCON_END_MARKER_DELAY).await;
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
            auto_advance_guard.finish().await;
        });
        (
            address,
            event_receiver,
            ScriptedRconServer { task: Some(task) },
        )
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
    ) -> (Endpoint, oneshot::Receiver<()>, ScriptedRconServer) {
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
                    Err(error) if expected_test_peer_close(&error) => {
                        break;
                    }
                    Err(error) => panic!("test RCON packet read failed: {error}"),
                };
                assert_eq!(packet.kind, TEST_EXEC_COMMAND);
                if packet.body == b"stop" {
                    break;
                }
                assert_eq!(packet.body, b"list");

                let end_marker = match try_read_test_rcon_packet(&mut stream).await {
                    Ok(packet) => packet,
                    Err(error) if expected_test_peer_close(&error) => {
                        break;
                    }
                    Err(error) => panic!("test RCON end-marker read failed: {error}"),
                };
                assert_eq!(end_marker.kind, TEST_EXEC_COMMAND);
                assert!(end_marker.body.is_empty());
                let response = format!("There are {player_count} of a max of 20 players online:");
                if let Err(error) = try_write_test_rcon_packet(
                    &mut stream,
                    packet.id,
                    TEST_RESPONSE_VALUE,
                    response.as_bytes(),
                )
                .await
                {
                    assert!(
                        expected_test_peer_close(&error),
                        "test RCON response write failed: {error}"
                    );
                    break;
                }
                if let Err(error) =
                    try_write_test_rcon_packet(&mut stream, end_marker.id, TEST_RESPONSE_VALUE, b"")
                        .await
                {
                    assert!(
                        expected_test_peer_close(&error),
                        "test RCON end-marker response write failed: {error}"
                    );
                    break;
                }
                if let Some(sender) = first_poll_sender.take() {
                    let _ = sender.send(());
                }
            }
        });
        (
            address,
            first_poll_receiver,
            ScriptedRconServer { task: Some(server) },
        )
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
            command_revision: 0,
            launch_generation: 1,
            failure: Some(FailureCategory::ExitedUnexpectedly),
            failure_streak,
            phase_started_at: Instant::now(),
            retry_at: None,
            reconciliation_evidence: ReconciliationEvidence::None,
        }
    }

    fn running_lifecycle(failure_streak: u32) -> LifecycleState {
        LifecycleState {
            phase: ServerPhase::Running,
            command_revision: 0,
            launch_generation: 1,
            failure: (failure_streak > 0).then_some(FailureCategory::ExitedUnexpectedly),
            failure_streak,
            phase_started_at: Instant::now(),
            retry_at: None,
            reconciliation_evidence: ReconciliationEvidence::None,
        }
    }

    fn lifecycle_with_generation(phase: ServerPhase, launch_generation: u64) -> LifecycleState {
        let mut lifecycle = LifecycleState::test_phase(phase);
        lifecycle.launch_generation = launch_generation;
        lifecycle
    }

    fn test_backend_use() -> Arc<BackendUseCoordinator> {
        Arc::new(BackendUseCoordinator::new())
    }

    fn test_backend_endpoint() -> BackendEndpoint {
        BackendEndpoint::network(Endpoint::new("127.0.0.1", 9), Duration::from_secs(1))
    }

    async fn run_test(
        wake_requests: WakeLatchReceiver,
        shutdown: oneshot::Receiver<()>,
        lifecycle: watch::Sender<LifecycleState>,
        config: SupervisorConfig,
        backend: BackendEndpoint,
        backend_use: Arc<BackendUseCoordinator>,
    ) -> Result<()> {
        let (_mutations, mutation_receiver) = mpsc::channel(MUTATION_QUEUE_CAPACITY);
        run_test_with_mutations(
            wake_requests,
            mutation_receiver,
            shutdown,
            lifecycle,
            config,
            backend,
            backend_use,
        )
        .await
    }

    async fn run_test_with_mutations(
        wake_requests: WakeLatchReceiver,
        mutation_receiver: mpsc::Receiver<MutationEnvelope>,
        shutdown: oneshot::Receiver<()>,
        lifecycle: watch::Sender<LifecycleState>,
        config: SupervisorConfig,
        backend: BackendEndpoint,
        backend_use: Arc<BackendUseCoordinator>,
    ) -> Result<()> {
        let (quiescence, _quiescence_receiver) = mpsc::channel(1);
        let (fatal, _fatal_receiver) = FatalSignal::new();
        let (_runtime_drained, runtime_drained) = watch::channel(false);
        run(
            wake_requests,
            mutation_receiver,
            quiescence,
            fatal,
            runtime_drained,
            shutdown,
            lifecycle,
            config,
            backend,
            backend_use,
        )
        .await
    }

    fn test_control(
        initial: LifecycleState,
    ) -> (
        SupervisorControl,
        WakeLatchSender,
        oneshot::Sender<()>,
        watch::Receiver<LifecycleState>,
    ) {
        let (_wake_root, wake_sender, wake_requests) = wake_latch();
        let (shutdown_sender, shutdown) = oneshot::channel();
        let (lifecycle, lifecycle_receiver) = watch::channel(initial);
        (
            test_control_from_parts(wake_requests, shutdown, lifecycle),
            wake_sender,
            shutdown_sender,
            lifecycle_receiver,
        )
    }

    fn test_control_with_mutations(
        initial: LifecycleState,
    ) -> (
        SupervisorControl,
        mpsc::Sender<MutationEnvelope>,
        WakeLatchSender,
        oneshot::Sender<()>,
        watch::Receiver<LifecycleState>,
    ) {
        let (mut control, wake_sender, shutdown_sender, lifecycle) = test_control(initial);
        let (mutations, mutation_receiver) = mpsc::channel(MUTATION_QUEUE_CAPACITY);
        control.mutations = mutation_receiver;
        (control, mutations, wake_sender, shutdown_sender, lifecycle)
    }

    fn test_control_from_parts(
        wake_requests: WakeLatchReceiver,
        shutdown: oneshot::Receiver<()>,
        lifecycle: watch::Sender<LifecycleState>,
    ) -> SupervisorControl {
        let (_mutations, mutation_receiver) = mpsc::channel(MUTATION_QUEUE_CAPACITY);
        let (quiescence, _quiescence_receiver) = mpsc::channel(1);
        let (fatal, _fatal_receiver) = FatalSignal::new();
        let (_runtime_drained, runtime_drained) = watch::channel(false);
        let initial = *lifecycle.borrow();
        SupervisorControl {
            wake_requests,
            mutations: mutation_receiver,
            shutdown,
            lifecycle,
            last_admission: None,
            launch_demand: false,
            stopping_reason: (initial.phase == ServerPhase::Stopping).then_some(
                if initial.failure.is_some() {
                    StoppingReason::FailureCleanup
                } else {
                    StoppingReason::AutomaticIdle
                },
            ),
            reconciliation_mode: if initial.phase == ServerPhase::Reconciling {
                ReconciliationMode::Initial
            } else {
                ReconciliationMode::FinalGate
            },
            fatal,
            runtime_drained,
            mutations_closed: false,
            quiescence,
            fatal_error: None,
        }
    }

    struct TestSupervisor {
        wake_root: WakeLatchRoot,
        wake_sender: WakeLatchSender,
        shutdown_sender: oneshot::Sender<()>,
        mutations: mpsc::Sender<MutationEnvelope>,
        lifecycle: watch::Receiver<LifecycleState>,
        task: JoinHandle<Result<()>>,
    }

    impl TestSupervisor {
        fn spawn(config: SupervisorConfig, backend: BackendEndpoint) -> Self {
            let (wake_root, wake_sender, wake_receiver) = wake_latch();
            let (shutdown_sender, shutdown_receiver) = oneshot::channel();
            let (lifecycle_sender, lifecycle) = watch::channel(LifecycleState::reconciling());
            let (mutations, mutation_receiver) = mpsc::channel(MUTATION_QUEUE_CAPACITY);
            let task = tokio::spawn(run_test_with_mutations(
                wake_receiver,
                mutation_receiver,
                shutdown_receiver,
                lifecycle_sender,
                config,
                backend,
                test_backend_use(),
            ));
            Self {
                wake_root,
                wake_sender,
                shutdown_sender,
                mutations,
                lifecycle,
                task,
            }
        }

        async fn shutdown(self) -> watch::Receiver<LifecycleState> {
            self.wake_root.close();
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

    async fn initially_polled_player_inspection(config: &SupervisorConfig) -> PlayerInspection {
        let rcon_endpoint = &config
            .rcon
            .as_ref()
            .expect("test configuration should enable RCON")
            .endpoint;
        let mut player_inspection = connected_player_inspection(rcon_endpoint).await;
        poll_player_presence(&mut player_inspection, config).await;
        player_inspection
    }

    fn test_mutation(
        operation: MutationOperation,
        request_id: &str,
        expected_command_revision: u64,
        admission: Arc<AdmissionGate>,
    ) -> (MutationEnvelope, oneshot::Receiver<MutationReply>) {
        let (reply, receiver) = oneshot::channel();
        (
            MutationEnvelope {
                operation,
                request_id: request_id.to_owned(),
                expected_command_revision,
                reply,
                admission,
            },
            receiver,
        )
    }

    async fn process_queued_mutation(
        control: &mut SupervisorControl,
        config: &SupervisorConfig,
    ) -> MutationEffect {
        let envelope = control.mutations.recv().await;
        control.receive_mutation(envelope, config)
    }

    fn ready_cleanup(result: Result<()>) -> impl Future<Output = Result<()>> {
        let (release, released) = oneshot::channel();
        release
            .send(result)
            .expect("test cleanup receiver should still exist");
        async move {
            released
                .await
                .expect("test cleanup release should remain available")
        }
    }

    #[tokio::test]
    async fn successful_mutations_increment_once_and_replay_exactly() {
        let (mut control, _wake_sender, _shutdown_sender, lifecycle) =
            test_control(LifecycleState::stopped());
        let config = test_config(&Endpoint::new("127.0.0.1", 9));
        let first_gate = Arc::new(AdmissionGate::pending());
        let (first, first_reply) = test_mutation(
            MutationOperation::Start,
            "11111111111111111111111111111111",
            0,
            Arc::clone(&first_gate),
        );
        assert_eq!(
            control.handle_mutation(first, &config),
            MutationEffect::None
        );
        let success = first_reply.await.unwrap().unwrap();
        assert_eq!(success.command_revision, 1);
        assert_eq!(success.disposition, MutationDisposition::Accepted);
        assert!(first_gate.is_admitted());
        assert_eq!(lifecycle.borrow().command_revision, 1);

        let replay_gate = Arc::new(AdmissionGate::pending());
        let (replay, replay_reply) = test_mutation(
            MutationOperation::Start,
            "11111111111111111111111111111111",
            0,
            Arc::clone(&replay_gate),
        );
        control.handle_mutation(replay, &config);
        assert_eq!(replay_reply.await.unwrap().unwrap(), success);
        assert_eq!(lifecycle.borrow().command_revision, 1);
        assert!(!replay_gate.is_admitted());

        let (conflict, conflict_reply) = test_mutation(
            MutationOperation::Stop,
            "11111111111111111111111111111111",
            1,
            Arc::new(AdmissionGate::pending()),
        );
        control.handle_mutation(conflict, &config);
        assert_eq!(
            conflict_reply.await.unwrap(),
            Err(MutationError::RequestIdConflict)
        );
        assert_eq!(lifecycle.borrow().command_revision, 1);
    }

    #[tokio::test]
    async fn start_phase_table_covers_every_phase_and_stopping_reason() {
        for (phase, expected) in [
            (ServerPhase::Reconciling, Ok(MutationDisposition::Accepted)),
            (ServerPhase::Stopped, Ok(MutationDisposition::Accepted)),
            (
                ServerPhase::Starting,
                Err(MutationError::OperationInProgress),
            ),
            (
                ServerPhase::Running,
                Ok(MutationDisposition::AlreadySatisfied),
            ),
            (ServerPhase::Stopping, Ok(MutationDisposition::Accepted)),
            (ServerPhase::Cooldown, Ok(MutationDisposition::Accepted)),
            (
                ServerPhase::External,
                Err(MutationError::ExternalBackendNotOwned),
            ),
            (ServerPhase::Conflict, Ok(MutationDisposition::Accepted)),
        ] {
            let (control, _, _, _) = test_control(LifecycleState::test_phase(phase));
            let result = control
                .plan_mutation(MutationOperation::Start, control.current_lifecycle(), 1)
                .map(|plan| plan.disposition);
            assert_eq!(result, expected, "unexpected start policy for {phase:?}");
        }

        for (reason, expected) in [
            (
                StoppingReason::AutomaticIdle,
                Ok(MutationDisposition::Accepted),
            ),
            (
                StoppingReason::FailureCleanup,
                Ok(MutationDisposition::Accepted),
            ),
            (
                StoppingReason::Explicit,
                Err(MutationError::OperationInProgress),
            ),
            (
                StoppingReason::DaemonShutdown,
                Err(MutationError::OperationRejected),
            ),
        ] {
            let (mut control, _, _, _) =
                test_control(LifecycleState::test_phase(ServerPhase::Stopping));
            control.stopping_reason = Some(reason);
            let result = control
                .plan_mutation(MutationOperation::Start, control.current_lifecycle(), 1)
                .map(|plan| plan.disposition);
            assert_eq!(result, expected, "unexpected start policy for {reason:?}");
        }

        let mut positive_conflict = LifecycleState::test_phase(ServerPhase::Conflict);
        positive_conflict.reconciliation_evidence = ReconciliationEvidence::Positive;
        let (control, _, _, _) = test_control(positive_conflict);
        assert!(matches!(
            control.plan_mutation(MutationOperation::Start, positive_conflict, 1),
            Err(MutationError::ConflictingBackendEvidence)
        ));
    }

    #[tokio::test]
    async fn stop_phase_table_covers_every_phase_and_stopping_reason() {
        for (phase, expected) in [
            (
                ServerPhase::Reconciling,
                Err(MutationError::OperationRejected),
            ),
            (
                ServerPhase::Stopped,
                Ok(MutationDisposition::AlreadySatisfied),
            ),
            (ServerPhase::Starting, Ok(MutationDisposition::Accepted)),
            (ServerPhase::Running, Ok(MutationDisposition::Accepted)),
            (ServerPhase::Stopping, Ok(MutationDisposition::Accepted)),
            (
                ServerPhase::Cooldown,
                Ok(MutationDisposition::AlreadySatisfied),
            ),
            (
                ServerPhase::External,
                Err(MutationError::ExternalBackendNotOwned),
            ),
            (
                ServerPhase::Conflict,
                Err(MutationError::ConflictingBackendEvidence),
            ),
        ] {
            let (control, _, _, _) = test_control(LifecycleState::test_phase(phase));
            let result = control
                .plan_mutation(MutationOperation::Stop, control.current_lifecycle(), 1)
                .map(|plan| plan.disposition);
            assert_eq!(result, expected, "unexpected stop policy for {phase:?}");
        }

        let (mut final_gate, _, _, _) =
            test_control(LifecycleState::test_phase(ServerPhase::Reconciling));
        final_gate.reconciliation_mode = ReconciliationMode::FinalGate;
        assert!(matches!(
            final_gate.plan_mutation(MutationOperation::Stop, final_gate.current_lifecycle(), 1,),
            Err(MutationError::OperationInProgress)
        ));

        for (reason, expected_reason, expected) in [
            (
                StoppingReason::AutomaticIdle,
                Some(StoppingReason::Explicit),
                Ok(MutationDisposition::Accepted),
            ),
            (
                StoppingReason::FailureCleanup,
                Some(StoppingReason::FailureCleanup),
                Ok(MutationDisposition::AlreadySatisfied),
            ),
            (
                StoppingReason::Explicit,
                Some(StoppingReason::Explicit),
                Err(MutationError::OperationInProgress),
            ),
            (
                StoppingReason::DaemonShutdown,
                Some(StoppingReason::DaemonShutdown),
                Err(MutationError::OperationRejected),
            ),
        ] {
            let (mut control, _, _, _) =
                test_control(LifecycleState::test_phase(ServerPhase::Stopping));
            control.stopping_reason = Some(reason);
            let result =
                control.plan_mutation(MutationOperation::Stop, control.current_lifecycle(), 1);
            match (result, expected) {
                (Ok(plan), Ok(disposition)) => {
                    assert_eq!(plan.disposition, disposition);
                    assert_eq!(plan.stopping_reason, expected_reason);
                }
                (Err(actual), Err(expected)) => assert_eq!(actual, expected),
                _ => panic!("unexpected stop policy for {reason:?}"),
            }
        }

        let (mut failed_cleanup, _, _, _) =
            test_control(LifecycleState::test_phase(ServerPhase::Stopping));
        failed_cleanup.stopping_reason = Some(StoppingReason::FailureCleanup);
        failed_cleanup.launch_demand = true;
        let plan = failed_cleanup
            .plan_mutation(
                MutationOperation::Stop,
                failed_cleanup.current_lifecycle(),
                1,
            )
            .unwrap();
        assert_eq!(plan.disposition, MutationDisposition::Accepted);
        assert_eq!(plan.stopping_reason, Some(StoppingReason::FailureCleanup));
        assert!(!plan.launch_demand);
    }

    #[tokio::test]
    async fn cancellation_and_rejection_do_not_publish_a_revision() {
        let (mut control, _wake_sender, _shutdown_sender, lifecycle) =
            test_control(LifecycleState::test_phase(ServerPhase::External));
        let config = test_config(&Endpoint::new("127.0.0.1", 9));
        let cancelled_gate = Arc::new(AdmissionGate::pending());
        assert!(cancelled_gate.cancel());
        let (cancelled, cancelled_reply) = test_mutation(
            MutationOperation::Start,
            "22222222222222222222222222222222",
            0,
            cancelled_gate,
        );
        control.handle_mutation(cancelled, &config);
        assert!(cancelled_reply.await.is_err());
        assert_eq!(lifecycle.borrow().command_revision, 0);

        let (rejected, rejected_reply) = test_mutation(
            MutationOperation::Stop,
            "33333333333333333333333333333333",
            0,
            Arc::new(AdmissionGate::pending()),
        );
        control.handle_mutation(rejected, &config);
        assert_eq!(
            rejected_reply.await.unwrap(),
            Err(MutationError::ExternalBackendNotOwned)
        );
        assert_eq!(lifecycle.borrow().command_revision, 0);
    }

    #[tokio::test]
    async fn counter_exhaustion_is_fatal_without_publication() {
        let mut exhausted = LifecycleState::stopped();
        exhausted.command_revision = u64::MAX;
        exhausted.launch_generation = u64::MAX;
        let (mut control, _wake_sender, _shutdown_sender, lifecycle) = test_control(exhausted);
        let config = test_config(&Endpoint::new("127.0.0.1", 9));
        let gate = Arc::new(AdmissionGate::pending());
        let (mutation, reply) = test_mutation(
            MutationOperation::Start,
            "44444444444444444444444444444444",
            u64::MAX,
            Arc::clone(&gate),
        );
        control.handle_mutation(mutation, &config);
        assert_eq!(reply.await.unwrap(), Err(MutationError::InternalFailure));
        assert!(!gate.is_admitted());
        assert_eq!(lifecycle.borrow().command_revision, u64::MAX);
        assert_eq!(control.next_launch_generation(), None);
        assert_eq!(lifecycle.borrow().launch_generation, u64::MAX);
        assert!(control.fatal_error.is_some());
    }

    #[tokio::test]
    async fn wake_latch_is_non_lossy_and_newer_freshness_replaces_stale() {
        let (root, sender, receiver) = wake_latch();
        let mut stale = LifecycleState::stopped();
        stale.command_revision = 7;
        stale.launch_generation = 3;
        sender.enqueue(WakeRequest::observed(stale)).unwrap();
        sender.enqueue(WakeRequest::observed(stale)).unwrap();

        let mut newer_generation = stale;
        newer_generation.launch_generation = 4;
        sender
            .enqueue(WakeRequest::observed(newer_generation))
            .unwrap();
        let mut older_revision = stale;
        older_revision.command_revision = 6;
        older_revision.launch_generation = u64::MAX;
        sender
            .enqueue(WakeRequest::observed(older_revision))
            .unwrap();
        assert_eq!(
            receiver.try_recv(),
            Some(WakeRequest::observed(newer_generation))
        );

        sender
            .enqueue(WakeRequest::observed(newer_generation))
            .unwrap();
        assert_eq!(
            receiver.recv().await,
            Some(WakeRequest::observed(newer_generation))
        );
        root.close();
        assert_eq!(
            sender.enqueue(WakeRequest::observed(stale)),
            Err(WakeLatchError::Closed)
        );
        assert_eq!(receiver.recv().await, None);
    }

    #[tokio::test]
    async fn post_stop_wake_replaces_a_stale_pending_wake() {
        let (_root, sender, receiver) = wake_latch();
        let mut before_stop = LifecycleState::stopped();
        before_stop.command_revision = 12;
        sender.enqueue(WakeRequest::observed(before_stop)).unwrap();
        let mut after_stop = before_stop;
        after_stop.command_revision = 13;
        sender.enqueue(WakeRequest::observed(after_stop)).unwrap();
        assert_eq!(
            receiver.recv().await,
            Some(WakeRequest::observed(after_stop))
        );
        assert!(WakeRequest::observed(after_stop).can_start(after_stop));
    }

    #[tokio::test]
    async fn explicit_quiescence_queues_writer_before_request_and_retains_it() {
        let running = running_lifecycle(0);
        let backend_use = running_backend_use(running);
        let initial_reader = backend_use
            .acquire_shared(running.owned_running_cycle().unwrap())
            .await;
        let (mut control, _wake_sender, _shutdown_sender, _lifecycle) = test_control(running);
        control.stopping_reason = Some(StoppingReason::Explicit);
        let (quiescence, mut runtime_requests) = mpsc::channel(1);
        control.quiescence = quiescence;
        let mut config = test_config(&Endpoint::new("127.0.0.1", 9));
        config.command_timeout = Duration::from_secs(5);
        let mut child = process::launch(&config.launch).expect("test server process should launch");
        let task_backend_use = Arc::clone(&backend_use);
        let quiescence = tokio::spawn(async move {
            let result = quiesce_runtime_and_acquire_exclusive(
                &mut child,
                &mut control,
                &config,
                &task_backend_use,
            )
            .await;
            (result, control, child, config)
        });

        let request = runtime_requests
            .recv()
            .await
            .expect("supervisor should request runtime quiescence");
        let later_reader = {
            let backend_use = Arc::clone(&backend_use);
            tokio::spawn(async move {
                backend_use
                    .acquire_shared(BackendCycle::from_launch_generation(1))
                    .await
            })
        };
        tokio::task::yield_now().await;
        drop(initial_reader);
        request.acknowledged.send(Ok(())).unwrap();
        let (result, _control, mut child, config) = quiescence.await.unwrap();
        let outcome = result.unwrap();
        assert!(outcome.child_status.is_none());
        let writer = outcome.exclusive;
        assert!(!later_reader.is_finished());
        drop(writer);
        drop(later_reader.await.unwrap());
        force_and_reap(&mut child, &config).await.unwrap();
    }

    #[tokio::test]
    async fn permanent_runtime_drain_breaks_shutdown_quiescence_wait_without_acknowledgement() {
        let running = running_lifecycle(0);
        let backend_use = running_backend_use(running);
        let (mut control, _wake_sender, shutdown_sender, _lifecycle) = test_control(running);
        control.stopping_reason = Some(StoppingReason::Explicit);
        let (quiescence, mut runtime_requests) = mpsc::channel(1);
        control.quiescence = quiescence;
        let (runtime_drained, runtime_drain) = watch::channel(false);
        control.runtime_drained = runtime_drain;
        let config = no_rcon_config();
        let mut child = process::launch(&config.launch).unwrap();
        let task_backend_use = Arc::clone(&backend_use);
        let quiescence = tokio::spawn(async move {
            let result = quiesce_runtime_and_acquire_exclusive(
                &mut child,
                &mut control,
                &config,
                &task_backend_use,
            )
            .await;
            (result, child, config)
        });

        let request = runtime_requests
            .recv()
            .await
            .expect("supervisor should enqueue quiescence before shutdown");
        runtime_drained.send_replace(true);
        shutdown_sender.send(()).unwrap();

        let (result, mut child, config) = quiescence.await.unwrap();
        let outcome = result.expect("permanent runtime drainage should satisfy shutdown");
        assert!(outcome.shutdown_received);
        drop(outcome.exclusive);
        drop(request);
        force_and_reap(&mut child, &config).await.unwrap();
    }

    #[tokio::test]
    async fn genuine_quiescence_failure_is_fatal_and_reaps_the_owned_child() {
        let running = running_lifecycle(0);
        let backend_use = running_backend_use(running);
        let (mut control, _wake_sender, _shutdown_sender, _lifecycle) = test_control(running);
        control.stopping_reason = Some(StoppingReason::Explicit);
        let (closed_quiescence, runtime_requests) = mpsc::channel(1);
        drop(runtime_requests);
        control.quiescence = closed_quiescence;
        let (runtime_drained, runtime_drain) = watch::channel(true);
        control.runtime_drained = runtime_drain;
        let config = no_rcon_config();
        let child = process::launch(&config.launch).unwrap();
        let process_id = child.id().unwrap();

        assert_eq!(
            stop_explicit_server(child, &mut control, &config, &backend_use)
                .await
                .expect("fatal cleanup should still reap the child"),
            CycleOutcome::Shutdown
        );
        assert!(control.fatal_error.is_some());
        assert!(test_process_has_exited(process_id));
        drop(runtime_drained);
    }

    #[tokio::test(start_paused = true)]
    async fn scripted_rcon_io_inhibits_automatic_but_not_manual_time_advance() {
        let started_at = Instant::now();
        let (rcon_address, mut events, rcon_server) =
            spawn_scripted_list_rcon_server(vec![player_count_reply(0)]).await;
        let config = test_config(&rcon_address);
        let mut player_inspection = connected_player_inspection(&rcon_address).await;

        poll_player_presence(&mut player_inspection, &config).await;
        assert_eq!(events.recv().await, Some(TestRconEvent::List(1)));
        assert_eq!(Instant::now() - started_at, TEST_RCON_END_MARKER_DELAY);

        let idle_deadline = tokio::time::sleep(Duration::from_secs(1));
        tokio::pin!(idle_deadline);
        tokio::time::advance(Duration::from_secs(1)).await;
        idle_deadline.await;
        assert_eq!(
            Instant::now() - started_at,
            Duration::from_secs(1) + TEST_RCON_END_MARKER_DELAY
        );

        drop(player_inspection);
        assert_eq!(events.recv().await, Some(TestRconEvent::Closed));
        rcon_server.abort();
        assert!(rcon_server.join().await.unwrap_err().is_cancelled());

        let cleanup_finished_at = Instant::now();
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(Instant::now() - cleanup_finished_at, Duration::from_secs(1));
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_scripted_rcon_server_releases_time_inhibitor() {
        let started_at = Instant::now();
        let (_rcon_address, events, rcon_server) =
            spawn_scripted_list_rcon_server(Vec::new()).await;

        drop((events, rcon_server));
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(Instant::now() - started_at, Duration::from_secs(1));
    }

    #[tokio::test]
    async fn aborted_player_count_fixture_is_stopped_and_joined() {
        let (_rcon_address, first_poll, rcon_server) = spawn_player_count_rcon_server(1).await;
        rcon_server.abort();
        assert!(rcon_server.join().await.unwrap_err().is_cancelled());
        assert!(first_poll.await.is_err());
    }

    async fn assert_final_rcon_veto(reply: TestListReply) {
        let (rcon_address, mut events, rcon_server) =
            spawn_scripted_list_rcon_server(vec![player_count_reply(0), reply]).await;
        let mut config = test_config(&rcon_address);
        config.idle_timeout = Duration::from_secs(1);
        config.poll_interval = Duration::from_secs(100);
        let child = process::launch(&config.launch).expect("test server process should launch");
        let (_wake_root, wake_sender, wake_receiver) = wake_latch();
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let running = running_lifecycle(0);
        let (lifecycle_sender, lifecycle) = watch::channel(running);
        let mut control =
            test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);
        let backend_use = running_backend_use(running);
        let cycle = running.owned_running_cycle().unwrap();
        let player_inspection = initially_polled_player_inspection(&config).await;
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
        wake_sender.close();
        assert_eq!(events.recv().await, Some(TestRconEvent::Closed));
        assert_eq!(events.recv().await, Some(TestRconEvent::Stop));
        rcon_server
            .join()
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
        let mut initial = LifecycleState::test_phase(ServerPhase::Conflict);
        initial.command_revision = 77;
        let (mut control, _wake_sender, _shutdown_sender, lifecycle) = test_control(initial);

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
        control.record_launch_success(1);
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
        let (mut control, _wake_sender, _shutdown_sender, lifecycle) = test_control(cooldown);

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
        let (_wake_root, wake_sender, wake_receiver) = wake_latch();
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let initial = LifecycleState::reconciling();
        let (lifecycle_sender, mut lifecycle) = watch::channel(initial);
        wake_sender
            .enqueue(WakeRequest::observed(initial))
            .expect("startup wake should fit");
        probe_sender
            .send(test_reconciliation(
                RconProbeClassification::Refused,
                BackendProbeClassification::Refused,
            ))
            .expect("startup clean result should queue");
        let supervisor = tokio::spawn(run_test(
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
    async fn initial_reconciliation_keeps_one_authoritative_start_demand() {
        let (probe_sender, backend) = scripted_backend();
        let initial = LifecycleState::reconciling();
        let (mut control, mutations, _wake_sender, _shutdown_sender, mut lifecycle) =
            test_control_with_mutations(initial);
        let config = launch_failure_config();
        let waiter = tokio::spawn(async move {
            let outcome = wait_for_reconciliation(&mut control, &config, &backend, true).await;
            (outcome, control)
        });

        let first_gate = Arc::new(AdmissionGate::pending());
        let (first, first_reply) = test_mutation(
            MutationOperation::Start,
            "10101010101010101010101010101010",
            0,
            Arc::clone(&first_gate),
        );
        mutations.send(first).await.unwrap();
        let admitted = first_reply.await.unwrap().unwrap();
        assert_eq!(admitted.disposition, MutationDisposition::Accepted);
        assert_eq!(admitted.command_revision, 1);
        assert!(first_gate.is_admitted());
        wait_for_lifecycle(&mut lifecycle, |state| state.command_revision == 1).await;

        for (request_id, expected_revision) in [
            ("20202020202020202020202020202020", 1),
            ("30303030303030303030303030303030", 1),
            ("40404040404040404040404040404040", 1),
        ] {
            let (distinct, reply) = test_mutation(
                MutationOperation::Start,
                request_id,
                expected_revision,
                Arc::new(AdmissionGate::pending()),
            );
            mutations.send(distinct).await.unwrap();
            assert_eq!(
                reply.await.unwrap(),
                Err(MutationError::OperationInProgress)
            );
            assert_eq!(lifecycle.borrow().command_revision, 1);
        }

        let replay_gate = Arc::new(AdmissionGate::pending());
        let (replay, replay_reply) = test_mutation(
            MutationOperation::Start,
            "10101010101010101010101010101010",
            0,
            Arc::clone(&replay_gate),
        );
        mutations.send(replay).await.unwrap();
        assert_eq!(replay_reply.await.unwrap().unwrap(), admitted);
        assert!(!replay_gate.is_admitted());
        assert_eq!(lifecycle.borrow().command_revision, 1);

        probe_sender
            .send(test_reconciliation(
                RconProbeClassification::Refused,
                BackendProbeClassification::Refused,
            ))
            .unwrap();
        let (outcome, control) = waiter.await.unwrap();
        assert!(matches!(outcome, ReconciliationWaitOutcome::Completed(_)));
        assert!(control.launch_demand);
        assert_eq!(control.current_lifecycle().command_revision, 1);
    }

    #[tokio::test]
    async fn one_initial_start_demand_reaches_one_final_gate_and_launch_attempt() {
        let (probe_sender, backend) = scripted_backend();
        let mut supervisor = TestSupervisor::spawn(launch_failure_config(), backend);
        let initial = *supervisor.lifecycle.borrow();

        let (first, first_reply) = test_mutation(
            MutationOperation::Start,
            "41414141414141414141414141414141",
            0,
            Arc::new(AdmissionGate::pending()),
        );
        supervisor.mutations.send(first).await.unwrap();
        let admitted = first_reply.await.unwrap().unwrap();
        assert_eq!(admitted.disposition, MutationDisposition::Accepted);

        for request_id in [
            "42424242424242424242424242424242",
            "43434343434343434343434343434343",
        ] {
            let (distinct, reply) = test_mutation(
                MutationOperation::Start,
                request_id,
                1,
                Arc::new(AdmissionGate::pending()),
            );
            supervisor.mutations.send(distinct).await.unwrap();
            assert_eq!(
                reply.await.unwrap(),
                Err(MutationError::OperationInProgress)
            );
        }
        let (replay, replay_reply) = test_mutation(
            MutationOperation::Start,
            "41414141414141414141414141414141",
            0,
            Arc::new(AdmissionGate::pending()),
        );
        supervisor.mutations.send(replay).await.unwrap();
        assert_eq!(replay_reply.await.unwrap().unwrap(), admitted);
        assert_eq!(supervisor.lifecycle.borrow().command_revision, 1);

        probe_sender
            .send(test_reconciliation(
                RconProbeClassification::Refused,
                BackendProbeClassification::Refused,
            ))
            .unwrap();
        wait_for_lifecycle(&mut supervisor.lifecycle, |state| {
            state.phase == ServerPhase::Reconciling
                && state.phase_started_at != initial.phase_started_at
        })
        .await;
        assert_eq!(supervisor.lifecycle.borrow().launch_generation, 0);

        probe_sender
            .send(test_reconciliation(
                RconProbeClassification::Refused,
                BackendProbeClassification::Refused,
            ))
            .unwrap();
        wait_for_phase(&mut supervisor.lifecycle, ServerPhase::Cooldown).await;
        assert_eq!(supervisor.lifecycle.borrow().launch_generation, 1);
        assert_eq!(supervisor.lifecycle.borrow().command_revision, 1);
        tokio::task::yield_now().await;
        assert_eq!(supervisor.lifecycle.borrow().launch_generation, 1);
        supervisor.shutdown().await;
    }

    #[tokio::test]
    async fn initial_login_wake_and_operator_start_coalesce_in_both_orders() {
        for operator_first in [false, true] {
            let (probe_sender, backend) = scripted_backend();
            let initial = LifecycleState::reconciling();
            let (mut control, mutations, wake_sender, _shutdown_sender, lifecycle) =
                test_control_with_mutations(initial);
            let config = launch_failure_config();
            let waiter = tokio::spawn(async move {
                let outcome = wait_for_reconciliation(&mut control, &config, &backend, true).await;
                (outcome, control)
            });

            let (start, start_reply) = test_mutation(
                MutationOperation::Start,
                if operator_first {
                    "50505050505050505050505050505050"
                } else {
                    "60606060606060606060606060606060"
                },
                0,
                Arc::new(AdmissionGate::pending()),
            );
            if operator_first {
                mutations.send(start).await.unwrap();
                assert_eq!(
                    start_reply.await.unwrap().unwrap().disposition,
                    MutationDisposition::Accepted
                );
                let observed = *lifecycle.borrow();
                wake_sender
                    .enqueue(WakeRequest::observed(observed))
                    .unwrap();
                wake_sender.wait_until_empty().await.unwrap();
            } else {
                wake_sender.enqueue(WakeRequest::observed(initial)).unwrap();
                wake_sender.wait_until_empty().await.unwrap();
                mutations.send(start).await.unwrap();
                assert_eq!(
                    start_reply.await.unwrap(),
                    Err(MutationError::OperationInProgress)
                );
            }

            probe_sender
                .send(test_reconciliation(
                    RconProbeClassification::Refused,
                    BackendProbeClassification::Refused,
                ))
                .unwrap();
            let (outcome, control) = waiter.await.unwrap();
            assert!(matches!(outcome, ReconciliationWaitOutcome::Completed(_)));
            assert!(control.launch_demand);
            assert_eq!(
                control.current_lifecycle().command_revision,
                u64::from(operator_first)
            );
        }
    }

    #[tokio::test]
    async fn mutation_precedes_ready_final_reconciliation_result() {
        let (probe_sender, backend) = scripted_backend();
        probe_sender
            .send(test_reconciliation(
                RconProbeClassification::Refused,
                BackendProbeClassification::Refused,
            ))
            .unwrap();
        let initial = LifecycleState::test_phase(ServerPhase::Reconciling);
        let (mut control, mutations, _wake_sender, _shutdown_sender, lifecycle) =
            test_control_with_mutations(initial);
        control.reconciliation_mode = ReconciliationMode::FinalGate;
        let (stop, stop_reply) = test_mutation(
            MutationOperation::Stop,
            "88888888888888888888888888888888",
            0,
            Arc::new(AdmissionGate::pending()),
        );
        mutations.send(stop).await.unwrap();

        assert!(matches!(
            wait_for_reconciliation(&mut control, &launch_failure_config(), &backend, false).await,
            ReconciliationWaitOutcome::Completed(_)
        ));
        assert_eq!(
            stop_reply.await.unwrap(),
            Err(MutationError::OperationInProgress)
        );
        assert_eq!(lifecycle.borrow().command_revision, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn command_precedes_ready_cooldown_expiry() {
        let retry_at = Instant::now();
        let mut initial = failed_lifecycle(ServerPhase::Cooldown, 2);
        initial.retry_at = Some(retry_at);
        let (mut control, mutations, _wake_sender, _shutdown_sender, lifecycle) =
            test_control_with_mutations(initial);
        let config = test_config(&Endpoint::new("127.0.0.1", 9));
        let (stop, stop_reply) = test_mutation(
            MutationOperation::Stop,
            "99999999999999999999999999999999",
            0,
            Arc::new(AdmissionGate::pending()),
        );
        mutations.send(stop).await.unwrap();

        assert_eq!(
            control.wait_for_cooldown(&config, retry_at, true).await,
            CooldownOutcome::Stopped
        );
        assert_eq!(
            stop_reply.await.unwrap().unwrap().disposition,
            MutationDisposition::AlreadySatisfied
        );
        let stopped = *lifecycle.borrow();
        assert_eq!(stopped.phase, ServerPhase::Stopped);
        assert_eq!(stopped.command_revision, 1);
        assert_eq!(stopped.failure, initial.failure);
        assert_eq!(stopped.failure_streak, initial.failure_streak);
    }

    #[tokio::test]
    async fn explicit_cleanup_completion_precedes_ready_commands_and_errors_win() {
        for operation in [MutationOperation::Stop, MutationOperation::Start] {
            let initial = lifecycle_with_generation(ServerPhase::Stopping, 1);
            let (mut control, mutations, _wake_sender, _shutdown_sender, lifecycle) =
                test_control_with_mutations(initial);
            control.stopping_reason = Some(StoppingReason::Explicit);
            let config = test_config(&Endpoint::new("127.0.0.1", 9));
            let (envelope, mut reply) = test_mutation(
                operation,
                if operation == MutationOperation::Stop {
                    "81818181818181818181818181818181"
                } else {
                    "82828282828282828282828282828282"
                },
                0,
                Arc::new(AdmissionGate::pending()),
            );
            mutations.send(envelope).await.unwrap();

            let (cleanup, shutdown) =
                wait_for_explicit_cleanup(ready_cleanup(Ok(())), &mut control, &config, false)
                    .await;
            cleanup.unwrap();
            assert!(!shutdown);
            assert!(matches!(
                reply.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ));
            assert_eq!(lifecycle.borrow().command_revision, 0);

            control.publish_clean_stopped_after_reap();
            process_queued_mutation(&mut control, &config).await;
            let success = reply.await.unwrap().unwrap();
            assert_eq!(success.command_revision, 1);
            assert_eq!(
                success.disposition,
                if operation == MutationOperation::Stop {
                    MutationDisposition::AlreadySatisfied
                } else {
                    MutationDisposition::Accepted
                }
            );
            assert_eq!(lifecycle.borrow().phase, ServerPhase::Stopped);
        }

        let initial = lifecycle_with_generation(ServerPhase::Stopping, 1);
        let (mut control, mutations, _wake_sender, _shutdown_sender, lifecycle) =
            test_control_with_mutations(initial);
        control.stopping_reason = Some(StoppingReason::Explicit);
        let config = test_config(&Endpoint::new("127.0.0.1", 9));
        let (envelope, mut reply) = test_mutation(
            MutationOperation::Start,
            "83838383838383838383838383838383",
            0,
            Arc::new(AdmissionGate::pending()),
        );
        mutations.send(envelope).await.unwrap();
        let (cleanup, _) = wait_for_explicit_cleanup(
            ready_cleanup(Err(anyhow::anyhow!("synthetic cleanup error"))),
            &mut control,
            &config,
            false,
        )
        .await;
        assert_eq!(cleanup.unwrap_err().to_string(), "synthetic cleanup error");
        assert!(matches!(
            reply.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert_eq!(lifecycle.borrow().command_revision, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn failure_cleanup_completion_precedes_ready_commands_and_errors_win() {
        for operation in [MutationOperation::Stop, MutationOperation::Start] {
            let starting = lifecycle_with_generation(ServerPhase::Starting, 1);
            let (mut control, mutations, _wake_sender, _shutdown_sender, lifecycle) =
                test_control_with_mutations(starting);
            let retry_delay = control.begin_failure_cleanup(FailureCategory::StartupTimedOut);
            control.launch_demand = operation == MutationOperation::Stop;
            let config = test_config(&Endpoint::new("127.0.0.1", 9));
            let (envelope, mut reply) = test_mutation(
                operation,
                if operation == MutationOperation::Stop {
                    "84848484848484848484848484848484"
                } else {
                    "85858585858585858585858585858585"
                },
                0,
                Arc::new(AdmissionGate::pending()),
            );
            mutations.send(envelope).await.unwrap();

            let outcome =
                finish_failed_cleanup(ready_cleanup(Ok(())), &mut control, &config, retry_delay)
                    .await
                    .unwrap();
            assert!(matches!(
                reply.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ));
            assert!(matches!(outcome, CycleOutcome::Failed { .. }));
            assert_eq!(lifecycle.borrow().phase, ServerPhase::Cooldown);
            assert_eq!(lifecycle.borrow().command_revision, 0);

            process_queued_mutation(&mut control, &config).await;
            let success = reply.await.unwrap().unwrap();
            assert_eq!(success.command_revision, 1);
            assert_eq!(
                success.disposition,
                if operation == MutationOperation::Start {
                    MutationDisposition::Accepted
                } else {
                    MutationDisposition::AlreadySatisfied
                }
            );
            assert_eq!(lifecycle.borrow().phase, ServerPhase::Cooldown);
            assert_eq!(
                lifecycle.borrow().failure,
                Some(FailureCategory::StartupTimedOut)
            );
            assert_eq!(lifecycle.borrow().failure_streak, 1);
            assert_eq!(control.launch_demand, operation == MutationOperation::Start);
        }

        let starting = lifecycle_with_generation(ServerPhase::Starting, 1);
        let (mut control, mutations, _wake_sender, _shutdown_sender, lifecycle) =
            test_control_with_mutations(starting);
        let retry_delay = control.begin_failure_cleanup(FailureCategory::StartupTimedOut);
        let config = test_config(&Endpoint::new("127.0.0.1", 9));
        let (envelope, mut reply) = test_mutation(
            MutationOperation::Stop,
            "86868686868686868686868686868686",
            0,
            Arc::new(AdmissionGate::pending()),
        );
        mutations.send(envelope).await.unwrap();
        let error = finish_failed_cleanup(
            ready_cleanup(Err(anyhow::anyhow!("synthetic failed cleanup error"))),
            &mut control,
            &config,
            retry_delay,
        )
        .await
        .unwrap_err();
        assert_eq!(error.to_string(), "synthetic failed cleanup error");
        assert!(matches!(
            reply.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert_eq!(lifecycle.borrow().phase, ServerPhase::Stopping);
        assert_eq!(lifecycle.borrow().command_revision, 0);
    }

    #[tokio::test]
    async fn idle_cleanup_completion_precedes_ready_commands_and_errors_win() {
        for operation in [MutationOperation::Start, MutationOperation::Stop] {
            let running = lifecycle_with_generation(ServerPhase::Running, 1);
            let (mut control, mutations, _wake_sender, _shutdown_sender, lifecycle) =
                test_control_with_mutations(running);
            control.begin_idle_stop(running.owned_running_cycle().unwrap());
            let config = test_config(&Endpoint::new("127.0.0.1", 9));
            let (envelope, mut reply) = test_mutation(
                operation,
                if operation == MutationOperation::Start {
                    "87878787878787878787878787878787"
                } else {
                    "89898989898989898989898989898989"
                },
                0,
                Arc::new(AdmissionGate::pending()),
            );
            mutations.send(envelope).await.unwrap();

            let (cleanup, restart, shutdown) =
                wait_for_idle_cleanup(ready_cleanup(Ok(())), &mut control, &config).await;
            cleanup.unwrap();
            assert!(!restart);
            assert!(!shutdown);
            assert!(matches!(
                reply.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ));

            control.set_phase(ServerPhase::Stopped);
            process_queued_mutation(&mut control, &config).await;
            let success = reply.await.unwrap().unwrap();
            assert_eq!(success.command_revision, 1);
            assert_eq!(lifecycle.borrow().phase, ServerPhase::Stopped);
            assert_eq!(
                success.disposition,
                if operation == MutationOperation::Start {
                    MutationDisposition::Accepted
                } else {
                    MutationDisposition::AlreadySatisfied
                }
            );
        }

        let running = lifecycle_with_generation(ServerPhase::Running, 1);
        let (mut control, mutations, _wake_sender, _shutdown_sender, lifecycle) =
            test_control_with_mutations(running);
        control.begin_idle_stop(running.owned_running_cycle().unwrap());
        let config = test_config(&Endpoint::new("127.0.0.1", 9));
        let (envelope, mut reply) = test_mutation(
            MutationOperation::Start,
            "90909090909090909090909090909090",
            0,
            Arc::new(AdmissionGate::pending()),
        );
        mutations.send(envelope).await.unwrap();
        let (cleanup, _, _) = wait_for_idle_cleanup(
            ready_cleanup(Err(anyhow::anyhow!("synthetic idle cleanup error"))),
            &mut control,
            &config,
        )
        .await;
        assert_eq!(
            cleanup.unwrap_err().to_string(),
            "synthetic idle cleanup error"
        );
        assert!(matches!(
            reply.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert_eq!(lifecycle.borrow().phase, ServerPhase::Stopping);
        assert_eq!(lifecycle.borrow().command_revision, 0);
    }

    #[tokio::test]
    async fn blocked_startup_requires_new_demand() {
        let (probe_sender, backend) = scripted_backend();
        let (_wake_root, wake_sender, wake_receiver) = wake_latch();
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let initial = LifecycleState::reconciling();
        let (lifecycle_sender, mut lifecycle) = watch::channel(initial);
        wake_sender
            .enqueue(WakeRequest::observed(initial))
            .expect("startup wake should fit");
        let supervisor = tokio::spawn(run_test(
            wake_receiver,
            shutdown_receiver,
            lifecycle_sender,
            launch_failure_config(),
            backend,
            test_backend_use(),
        ));
        wake_sender
            .wait_until_empty()
            .await
            .expect("supervisor should accept startup demand");
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
            .enqueue(WakeRequest::observed(*lifecycle.borrow()))
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
            .enqueue(WakeRequest::observed(initial))
            .expect("stale startup wake should queue");
        supervisor
            .wake_sender
            .wait_until_empty()
            .await
            .expect("supervisor should drain the stale startup wake");
        supervisor
            .wake_sender
            .enqueue(WakeRequest::observed(*supervisor.lifecycle.borrow()))
            .unwrap();
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
            .enqueue(WakeRequest::observed(*supervisor.lifecycle.borrow()))
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
            .enqueue(WakeRequest::observed(*supervisor.lifecycle.borrow()))
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
            .enqueue(WakeRequest::observed(*supervisor.lifecycle.borrow()))
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
            .enqueue(WakeRequest::observed(initial))
            .expect("stale startup wake should queue");
        supervisor
            .wake_sender
            .wait_until_empty()
            .await
            .expect("external monitor should drain the stale wake");
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

        let (wake_root, wake_sender, wake_receiver) = wake_latch();
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let (lifecycle_sender, mut lifecycle) = watch::channel(LifecycleState::reconciling());
        let mut config = launch_failure_config();
        config.rcon.as_mut().unwrap().endpoint = rcon_address;
        let supervisor = tokio::spawn(run_test(
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
        wake_root.close();
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
                    .enqueue(WakeRequest::observed(*supervisor.lifecycle.borrow()))
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
            .enqueue(WakeRequest::observed(*supervisor.lifecycle.borrow()))
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
            .enqueue(WakeRequest::observed(*supervisor.lifecycle.borrow()))
            .expect("supervisor should accept the initial wake request");
        wait_for_phase(&mut supervisor.lifecycle, ServerPhase::Cooldown).await;
        let first_failure = *supervisor.lifecycle.borrow();

        supervisor
            .wake_sender
            .enqueue(WakeRequest::observed(first_failure))
            .expect("one post-failure wake should latch");
        supervisor
            .wake_sender
            .wait_until_empty()
            .await
            .expect("supervisor should consume the latched retry request");
        supervisor
            .wake_sender
            .enqueue(WakeRequest::observed(first_failure))
            .unwrap();
        for _ in 0..16 {
            supervisor
                .wake_sender
                .enqueue(WakeRequest::observed(first_failure))
                .expect("equal wake keys should coalesce");
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
            .enqueue(WakeRequest::observed(*supervisor.lifecycle.borrow()))
            .expect("supervisor should accept the initial wake request");
        wait_for_phase(&mut supervisor.lifecycle, ServerPhase::Cooldown).await;

        tokio::time::advance(Duration::from_secs(5)).await;
        wait_for_phase(&mut supervisor.lifecycle, ServerPhase::Stopped).await;
        let expired = *supervisor.lifecycle.borrow();
        supervisor
            .wake_sender
            .enqueue(WakeRequest::observed(expired))
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
            .enqueue(WakeRequest::observed(*supervisor.lifecycle.borrow()))
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
        let config = test_config(&Endpoint::new("127.0.0.1", 9));
        let (started_sender, started_receiver) = oneshot::channel();
        let waiting = tokio::spawn(async move {
            started_sender
                .send(())
                .expect("test should still await cooldown startup");
            control.wait_for_cooldown(&config, retry_at, true).await
        });

        started_receiver
            .await
            .expect("cooldown task should report startup");
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        wake_sender.close();

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
        let starting = lifecycle_with_generation(ServerPhase::Starting, 1);
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
            let config = test_config(&Endpoint::new("127.0.0.1", 9));
            let outcome = finish_failed_cleanup(
                async move {
                    cleanup_receiver
                        .await
                        .expect("test should release failed-process cleanup");
                    Ok(())
                },
                &mut control,
                &config,
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
            Ok::<_, anyhow::Error>(
                control
                    .wait_for_cooldown(&config, retry_at, retry_pending)
                    .await,
            )
        });

        wake_sender
            .enqueue(WakeRequest::observed(failed_cleanup))
            .expect("wake during failure cleanup should be accepted");
        wake_sender
            .wait_until_empty()
            .await
            .expect("failure cleanup should consume and latch the first wake");
        wake_sender
            .enqueue(WakeRequest::observed(failed_cleanup))
            .unwrap();

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
    async fn failure_cleanup_without_takeover_enters_original_cooldown() {
        let starting = lifecycle_with_generation(ServerPhase::Starting, 1);
        let (mut control, _wake_sender, _shutdown_sender, lifecycle) = test_control(starting);
        let retry_delay = control.begin_failure_cleanup(FailureCategory::StartupTimedOut);
        let config = test_config(&Endpoint::new("127.0.0.1", 9));

        let outcome = finish_failed_cleanup(async { Ok(()) }, &mut control, &config, retry_delay)
            .await
            .expect("successful failure cleanup should settle");
        let CycleOutcome::Failed {
            retry_at,
            retry_pending,
        } = outcome
        else {
            panic!("failure cleanup without takeover should enter cooldown");
        };
        assert!(!retry_pending);
        let cooldown = *lifecycle.borrow();
        assert_eq!(cooldown.phase, ServerPhase::Cooldown);
        assert_eq!(cooldown.failure, Some(FailureCategory::StartupTimedOut));
        assert_eq!(cooldown.failure_streak, 1);
        assert_eq!(cooldown.retry_at, Some(retry_at));
    }

    #[tokio::test(start_paused = true)]
    async fn stop_during_failure_cleanup_clears_demand_but_preserves_cooldown() {
        let starting = lifecycle_with_generation(ServerPhase::Starting, 1);
        let (mut control, mutations, _wake_sender, _shutdown_sender, lifecycle) =
            test_control_with_mutations(starting);
        let retry_delay = control.begin_failure_cleanup(FailureCategory::StartupTimedOut);
        control.launch_demand = true;
        let retained = *lifecycle.borrow();
        let (release_cleanup, cleanup_released) = oneshot::channel();
        let cleanup = tokio::spawn(async move {
            let config = test_config(&Endpoint::new("127.0.0.1", 9));
            let result = finish_failed_cleanup(
                async move {
                    cleanup_released
                        .await
                        .expect("test should release process cleanup");
                    Ok(())
                },
                &mut control,
                &config,
                retry_delay,
            )
            .await;
            (result, control)
        });

        let first_gate = Arc::new(AdmissionGate::pending());
        let (first, first_reply) = test_mutation(
            MutationOperation::Stop,
            "55555555555555555555555555555555",
            0,
            Arc::clone(&first_gate),
        );
        mutations.send(first).await.unwrap();
        let accepted = first_reply.await.unwrap().unwrap();
        assert_eq!(accepted.disposition, MutationDisposition::Accepted);
        assert!(first_gate.is_admitted());

        let during_takeover = *lifecycle.borrow();
        assert_eq!(during_takeover.phase, ServerPhase::Stopping);
        assert_eq!(during_takeover.command_revision, 1);
        assert_eq!(during_takeover.failure, retained.failure);
        assert_eq!(during_takeover.failure_streak, retained.failure_streak);
        assert_eq!(during_takeover.retry_at, retained.retry_at);

        let replay_gate = Arc::new(AdmissionGate::pending());
        let (replay, replay_reply) = test_mutation(
            MutationOperation::Stop,
            "55555555555555555555555555555555",
            0,
            Arc::clone(&replay_gate),
        );
        mutations.send(replay).await.unwrap();
        assert_eq!(replay_reply.await.unwrap().unwrap(), accepted);
        assert!(!replay_gate.is_admitted());
        assert_eq!(lifecycle.borrow().command_revision, 1);

        let (distinct, distinct_reply) = test_mutation(
            MutationOperation::Stop,
            "66666666666666666666666666666666",
            1,
            Arc::new(AdmissionGate::pending()),
        );
        mutations.send(distinct).await.unwrap();
        assert_eq!(
            distinct_reply.await.unwrap().unwrap().disposition,
            MutationDisposition::AlreadySatisfied
        );
        assert_eq!(lifecycle.borrow().command_revision, 2);

        release_cleanup.send(()).unwrap();
        let (outcome, control) = cleanup.await.unwrap();
        let CycleOutcome::Failed {
            retry_at,
            retry_pending,
        } = outcome.unwrap()
        else {
            panic!("failure cleanup must retain its cooldown classification");
        };
        assert!(!retry_pending);
        assert!(!control.launch_demand);
        assert_eq!(
            control.stopping_reason,
            Some(StoppingReason::FailureCleanup)
        );
        let cooldown = *lifecycle.borrow();
        assert_eq!(cooldown.phase, ServerPhase::Cooldown);
        assert_eq!(cooldown.command_revision, 2);
        assert_eq!(cooldown.failure, retained.failure);
        assert_eq!(cooldown.failure_streak, retained.failure_streak);
        assert_eq!(cooldown.retry_at, Some(retry_at));
    }

    #[tokio::test(start_paused = true)]
    async fn stop_then_start_during_failure_cleanup_remains_cooldown_gated() {
        let starting = lifecycle_with_generation(ServerPhase::Starting, 1);
        let (mut control, mutations, _wake_sender, _shutdown_sender, lifecycle) =
            test_control_with_mutations(starting);
        let retry_delay = control.begin_failure_cleanup(FailureCategory::StartupTimedOut);
        control.launch_demand = true;
        let retained = *lifecycle.borrow();
        let (release_cleanup, cleanup_released) = oneshot::channel();
        let cleanup = tokio::spawn(async move {
            let config = test_config(&Endpoint::new("127.0.0.1", 9));
            let outcome = finish_failed_cleanup(
                async move {
                    cleanup_released.await.unwrap();
                    Ok(())
                },
                &mut control,
                &config,
                retry_delay,
            )
            .await;
            (outcome, control, config)
        });

        let (stop, stop_reply) = test_mutation(
            MutationOperation::Stop,
            "71717171717171717171717171717171",
            0,
            Arc::new(AdmissionGate::pending()),
        );
        mutations.send(stop).await.unwrap();
        assert_eq!(
            stop_reply.await.unwrap().unwrap().disposition,
            MutationDisposition::Accepted
        );

        let (start, start_reply) = test_mutation(
            MutationOperation::Start,
            "72727272727272727272727272727272",
            1,
            Arc::new(AdmissionGate::pending()),
        );
        mutations.send(start).await.unwrap();
        assert_eq!(
            start_reply.await.unwrap().unwrap().disposition,
            MutationDisposition::Accepted
        );
        assert_eq!(lifecycle.borrow().phase, ServerPhase::Stopping);
        assert_eq!(lifecycle.borrow().failure, retained.failure);
        assert_eq!(lifecycle.borrow().failure_streak, retained.failure_streak);

        release_cleanup.send(()).unwrap();
        let (outcome, mut control, config) = cleanup.await.unwrap();
        let CycleOutcome::Failed {
            retry_at,
            retry_pending,
        } = outcome.unwrap()
        else {
            panic!("failure cleanup must enter cooldown");
        };
        assert!(retry_pending);
        assert_eq!(lifecycle.borrow().phase, ServerPhase::Cooldown);
        assert_eq!(lifecycle.borrow().failure, retained.failure);
        assert_eq!(lifecycle.borrow().failure_streak, retained.failure_streak);

        let cooldown = tokio::spawn(async move {
            control
                .wait_for_cooldown(&config, retry_at, retry_pending)
                .await
        });
        tokio::task::yield_now().await;
        assert!(!cooldown.is_finished());
        tokio::time::advance(retry_delay.checked_sub(Duration::from_millis(1)).unwrap()).await;
        assert!(!cooldown.is_finished());
        tokio::time::advance(Duration::from_millis(1)).await;
        assert_eq!(cooldown.await.unwrap(), CooldownOutcome::Launch);
    }

    #[tokio::test(start_paused = true)]
    async fn failure_cleanup_stop_barrier_rejects_stale_wake() {
        let starting = lifecycle_with_generation(ServerPhase::Starting, 1);
        let (mut control, mutations, wake_sender, _shutdown_sender, lifecycle) =
            test_control_with_mutations(starting);
        let retry_delay = control.begin_failure_cleanup(FailureCategory::StartupTimedOut);
        control.launch_demand = true;
        let before_stop = *lifecycle.borrow();
        let (release_cleanup, cleanup_released) = oneshot::channel();
        let cleanup = tokio::spawn(async move {
            let config = test_config(&Endpoint::new("127.0.0.1", 9));
            let outcome = finish_failed_cleanup(
                async move {
                    cleanup_released.await.unwrap();
                    Ok(())
                },
                &mut control,
                &config,
                retry_delay,
            )
            .await;
            (outcome, control)
        });

        let (stop, stop_reply) = test_mutation(
            MutationOperation::Stop,
            "73737373737373737373737373737373",
            0,
            Arc::new(AdmissionGate::pending()),
        );
        mutations.send(stop).await.unwrap();
        stop_reply.await.unwrap().unwrap();
        assert_eq!(lifecycle.borrow().command_revision, 1);

        wake_sender
            .enqueue(WakeRequest::observed(before_stop))
            .unwrap();
        wake_sender.wait_until_empty().await.unwrap();
        release_cleanup.send(()).unwrap();

        let (outcome, control) = cleanup.await.unwrap();
        let CycleOutcome::Failed { retry_pending, .. } = outcome.unwrap() else {
            panic!("failure cleanup must enter cooldown");
        };
        assert!(!retry_pending);
        assert!(!control.launch_demand);
        assert_eq!(lifecycle.borrow().phase, ServerPhase::Cooldown);
    }

    #[tokio::test(start_paused = true)]
    async fn genuinely_later_wake_relatched_after_failure_cleanup_stop_is_cooldown_gated() {
        let starting = lifecycle_with_generation(ServerPhase::Starting, 1);
        let (mut control, mutations, wake_sender, _shutdown_sender, lifecycle) =
            test_control_with_mutations(starting);
        let retry_delay = control.begin_failure_cleanup(FailureCategory::StartupTimedOut);
        control.launch_demand = true;
        let (release_cleanup, cleanup_released) = oneshot::channel();
        let cleanup = tokio::spawn(async move {
            let config = test_config(&Endpoint::new("127.0.0.1", 9));
            let outcome = finish_failed_cleanup(
                async move {
                    cleanup_released.await.unwrap();
                    Ok(())
                },
                &mut control,
                &config,
                retry_delay,
            )
            .await;
            (outcome, control)
        });

        let (stop, stop_reply) = test_mutation(
            MutationOperation::Stop,
            "74747474747474747474747474747474",
            0,
            Arc::new(AdmissionGate::pending()),
        );
        mutations.send(stop).await.unwrap();
        stop_reply.await.unwrap().unwrap();
        let after_stop = *lifecycle.borrow();
        assert_eq!(after_stop.command_revision, 1);

        wake_sender
            .enqueue(WakeRequest::observed(after_stop))
            .unwrap();
        wake_sender.wait_until_empty().await.unwrap();
        release_cleanup.send(()).unwrap();

        let (outcome, control) = cleanup.await.unwrap();
        let CycleOutcome::Failed { retry_pending, .. } = outcome.unwrap() else {
            panic!("failure cleanup must enter cooldown");
        };
        assert!(retry_pending);
        assert!(control.launch_demand);
        assert_eq!(lifecycle.borrow().phase, ServerPhase::Cooldown);
    }

    #[tokio::test(start_paused = true)]
    async fn failure_cleanup_error_after_stop_never_publishes_cooldown_or_stopped() {
        let starting = lifecycle_with_generation(ServerPhase::Starting, 1);
        let (mut control, mutations, _wake_sender, _shutdown_sender, lifecycle) =
            test_control_with_mutations(starting);
        let retry_delay = control.begin_failure_cleanup(FailureCategory::StartupTimedOut);
        control.launch_demand = true;
        let (release_cleanup, cleanup_released) = oneshot::channel();
        let cleanup = tokio::spawn(async move {
            let config = test_config(&Endpoint::new("127.0.0.1", 9));
            let result = finish_failed_cleanup(
                async move {
                    cleanup_released
                        .await
                        .expect("test should release uncertain cleanup");
                    Err(anyhow::anyhow!("test cleanup remained uncertain"))
                },
                &mut control,
                &config,
                retry_delay,
            )
            .await;
            (result, control)
        });

        let (stop, stop_reply) = test_mutation(
            MutationOperation::Stop,
            "77777777777777777777777777777777",
            0,
            Arc::new(AdmissionGate::pending()),
        );
        mutations.send(stop).await.unwrap();
        assert_eq!(
            stop_reply.await.unwrap().unwrap().disposition,
            MutationDisposition::Accepted
        );
        release_cleanup.send(()).unwrap();

        let (result, control) = cleanup.await.unwrap();
        assert!(result.is_err());
        assert_eq!(
            control.stopping_reason,
            Some(StoppingReason::FailureCleanup)
        );
        let uncertain = *lifecycle.borrow();
        assert_eq!(uncertain.phase, ServerPhase::Stopping);
        assert_eq!(uncertain.failure, Some(FailureCategory::StartupTimedOut));
        assert_eq!(uncertain.failure_streak, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn demand_at_cooldown_expiry_launches_once() {
        let mut supervisor = spawn_launch_failure_supervisor();
        supervisor
            .wake_sender
            .enqueue(WakeRequest::observed(*supervisor.lifecycle.borrow()))
            .expect("supervisor should accept the initial wake request");
        wait_for_phase(&mut supervisor.lifecycle, ServerPhase::Cooldown).await;
        let failure = *supervisor.lifecycle.borrow();
        let retry_at = failure.retry_at.expect("failure should have a deadline");
        let racing_sender = supervisor.wake_sender.clone();
        let demand = tokio::spawn(async move {
            sleep_until(retry_at).await;
            racing_sender
                .enqueue(WakeRequest::observed(failure))
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
            .enqueue(WakeRequest::observed(*supervisor.lifecycle.borrow()))
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
    async fn stop_precedes_ready_startup_timeout() {
        let mut config = no_rcon_config();
        config.startup_timeout = Duration::ZERO;
        let mut child = process::launch(&config.launch).unwrap();
        let starting = lifecycle_with_generation(ServerPhase::Starting, 1);
        let (mut control, mutations, _wake_sender, _shutdown_sender, lifecycle) =
            test_control_with_mutations(starting);
        let (stop, stop_reply) = test_mutation(
            MutationOperation::Stop,
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            0,
            Arc::new(AdmissionGate::pending()),
        );
        mutations.send(stop).await.unwrap();

        assert!(matches!(
            wait_for_readiness(&mut child, &mut control, &config, &test_backend_endpoint()).await,
            ReadinessOutcome::ExplicitStop
        ));
        assert_eq!(
            stop_reply.await.unwrap().unwrap().disposition,
            MutationDisposition::Accepted
        );
        assert_eq!(lifecycle.borrow().phase, ServerPhase::Stopping);
        assert_eq!(lifecycle.borrow().command_revision, 1);
        force_and_reap(&mut child, &config).await.unwrap();
    }

    #[tokio::test]
    async fn child_exit_precedes_ready_mutation() {
        let mut config = rapid_exit_config(&Endpoint::new("127.0.0.1", 9));
        config.startup_timeout = Duration::ZERO;
        let mut child = process::launch(&config.launch).unwrap();
        let process_id = child.id().unwrap();
        wait_for_test_process_exit(process_id);
        let starting = lifecycle_with_generation(ServerPhase::Starting, 1);
        let (mut control, mutations, _wake_sender, _shutdown_sender, lifecycle) =
            test_control_with_mutations(starting);
        let gate = Arc::new(AdmissionGate::pending());
        let (stop, mut stop_reply) = test_mutation(
            MutationOperation::Stop,
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            0,
            Arc::clone(&gate),
        );
        mutations.send(stop).await.unwrap();

        assert!(matches!(
            wait_for_readiness(&mut child, &mut control, &config, &test_backend_endpoint()).await,
            ReadinessOutcome::Exited(_)
        ));
        assert_eq!(stop_reply.try_recv(), Err(TryRecvError::Empty));
        assert!(!gate.is_admitted());
        assert_eq!(lifecycle.borrow().command_revision, 0);
    }

    #[tokio::test]
    async fn shutdown_precedes_ready_mutation() {
        let config = no_rcon_config();
        let mut child = process::launch(&config.launch).unwrap();
        let starting = lifecycle_with_generation(ServerPhase::Starting, 1);
        let (mut control, mutations, _wake_sender, shutdown_sender, lifecycle) =
            test_control_with_mutations(starting);
        let gate = Arc::new(AdmissionGate::pending());
        let (stop, mut stop_reply) = test_mutation(
            MutationOperation::Stop,
            "cccccccccccccccccccccccccccccccc",
            0,
            Arc::clone(&gate),
        );
        mutations.send(stop).await.unwrap();
        drop(shutdown_sender);

        assert!(matches!(
            wait_for_readiness(&mut child, &mut control, &config, &test_backend_endpoint()).await,
            ReadinessOutcome::Shutdown
        ));
        assert_eq!(stop_reply.try_recv(), Err(TryRecvError::Empty));
        assert!(!gate.is_admitted());
        assert_eq!(lifecycle.borrow().command_revision, 0);
        force_and_reap(&mut child, &config).await.unwrap();
    }

    #[tokio::test]
    async fn successful_exit_before_readiness_is_retryable() {
        let config = rapid_exit_config(&Endpoint::new("127.0.0.1", 9));
        let child = process::launch(&config.launch).expect("short-lived test server should launch");
        let (_wake_root, _wake_sender, wake_receiver) = wake_latch();
        let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
        let (lifecycle_sender, lifecycle) = watch::channel(LifecycleState::reconciling());
        let mut control =
            test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);
        control.record_launch_success(1);

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
        let (_wake_root, wake_sender, wake_receiver) = wake_latch();
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let starting = lifecycle_with_generation(ServerPhase::Starting, 1);
        let (lifecycle_sender, mut lifecycle) = watch::channel(starting);
        let backend_use = test_backend_use();
        let cycle = tokio::spawn(async move {
            let mut control =
                test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);
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
        let (_wake_root, _wake_sender, wake_receiver) = wake_latch();
        let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
        let starting = lifecycle_with_generation(ServerPhase::Starting, 1);
        let (lifecycle_sender, lifecycle) = watch::channel(starting);
        let mut control =
            test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);

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
        let (_wake_root, _wake_sender, wake_receiver) = wake_latch();
        let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
        let running = running_lifecycle(0);
        let (lifecycle_sender, lifecycle) = watch::channel(running);
        let mut control =
            test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);
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
        let (_wake_root, wake_sender, wake_receiver) = wake_latch();
        let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
        let running = running_lifecycle(0);
        let (lifecycle_sender, lifecycle) = watch::channel(running);
        let mut control =
            test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);
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
        let unavailable_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let unavailable_address = endpoint(unavailable_listener.local_addr().unwrap());
        let rcon_server = tokio::spawn(async move {
            let (connection, _) = unavailable_listener.accept().await.unwrap();
            drop(connection);
        });
        config.rcon = Some(RconConfig {
            endpoint: unavailable_address,
            password: RconSecret::new("secret".to_owned()).unwrap(),
        });
        let child = process::launch(&config.launch).unwrap();
        let (_wake_root, _wake_sender, wake_receiver) = wake_latch();
        let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
        let running = running_lifecycle(0);
        let (lifecycle_sender, _lifecycle) = watch::channel(running);
        let mut control =
            test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);

        assert_eq!(
            stop_for_shutdown(child, &mut control, &config, &running_backend_use(running),)
                .await
                .unwrap(),
            CycleOutcome::Shutdown
        );
        rcon_server.await.unwrap();
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
        let (_wake_root, _wake_sender, wake_receiver) = wake_latch();
        let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
        let initial = failed_lifecycle(ServerPhase::Stopped, 1);
        let (lifecycle_sender, lifecycle) = watch::channel(initial);
        let mut control =
            test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);

        for (expected_streak, expected_delay) in [(2, 10), (3, 20)] {
            let (rcon_address, _first_poll, rcon_server) = spawn_player_count_rcon_server(1).await;
            let config = rapid_exit_config(&rcon_address);
            let child =
                process::launch(&config.launch).expect("short-lived test server should launch");
            control.begin_reconciliation();
            control.record_launch_success(1);

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
                .join()
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
        let (_wake_root, _wake_sender, wake_receiver) = wake_latch();
        let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
        let running = failed_lifecycle(ServerPhase::Running, 2);
        let (lifecycle_sender, mut lifecycle) = watch::channel(running);
        let mut control =
            test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);
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
        wait_for_phase(&mut lifecycle, ServerPhase::Cooldown).await;
        assert_eq!(lifecycle.borrow().failure_streak, 3);

        tokio::time::advance(STABILITY_WINDOW).await;
        assert_eq!(lifecycle.borrow().failure_streak, 3);
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
        let (_wake_root, _wake_sender, wake_receiver) = wake_latch();
        let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
        let running = failed_lifecycle(ServerPhase::Running, 3);
        let (lifecycle_sender, mut lifecycle) = watch::channel(running);
        let mut control =
            test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);
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
            .join()
            .await
            .expect("test RCON server should not panic");
    }

    #[tokio::test(start_paused = true)]
    async fn rcon_readiness_poll_and_stop_use_separate_sessions() {
        let (rcon_address, mut events, rcon_server) =
            spawn_scripted_list_rcon_server(vec![player_count_reply(0), player_count_reply(0)])
                .await;

        let mut config = test_config(&rcon_address);
        config.idle_timeout = Duration::from_millis(30);
        config.poll_interval = Duration::from_secs(1);
        let mut child = process::launch(&config.launch).unwrap();
        let (_wake_root, wake_sender, wake_receiver) = wake_latch();
        let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
        let starting = lifecycle_with_generation(ServerPhase::Starting, 1);
        let (lifecycle_sender, lifecycle) = watch::channel(starting);
        let mut control =
            test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);
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

        tokio::time::advance(Duration::from_millis(29)).await;
        assert_eq!(lifecycle.borrow().phase, ServerPhase::Running);
        assert!(Instant::now() < idle_anchor + config.idle_timeout);
        tokio::time::advance(Duration::from_millis(1)).await;

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
        tokio::time::resume();

        let outcome = stop_committed_idle_server(child, &mut control, &config)
            .await
            .unwrap();
        assert_eq!(outcome, CycleOutcome::Stopped);
        assert_eq!(lifecycle.borrow().phase, ServerPhase::Stopping);
        assert_eq!(events.recv().await, Some(TestRconEvent::Stop));
        drop(wake_sender);
        rcon_server.join().await.unwrap();
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
        let (_wake_root, wake_sender, wake_receiver) = wake_latch();
        let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
        let running = running_lifecycle(0);
        let (lifecycle_sender, mut lifecycle) = watch::channel(running);
        let mut control =
            test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);
        let backend_use = running_backend_use(running);
        let cycle = running.owned_running_cycle().unwrap();
        let replay_lease = backend_use.acquire_shared(cycle).await;
        let player_inspection = initially_polled_player_inspection(&config).await;
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
        tokio::time::resume();
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
            .join()
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
        let (_wake_root, wake_sender, wake_receiver) = wake_latch();
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let running = running_lifecycle(0);
        let (lifecycle_sender, lifecycle) = watch::channel(running);
        let mut control =
            test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);
        let backend_use = running_backend_use(running);
        let cycle = running.owned_running_cycle().unwrap();
        let player_inspection = initially_polled_player_inspection(&config).await;
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

        tokio::time::resume();
        shutdown_sender.send(()).unwrap();
        let outcome = monitor.await.unwrap().unwrap();
        assert_eq!(outcome, CycleOutcome::Shutdown);
        assert_eq!(events.recv().await, Some(TestRconEvent::Stop));
        drop(wake_sender);
        rcon_server.join().await.unwrap();
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
        let (_wake_root, wake_sender, wake_receiver) = wake_latch();
        let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
        let running = running_lifecycle(0);
        let (lifecycle_sender, lifecycle) = watch::channel(running);
        let mut control =
            test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);
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
        let outcome = stop_for_shutdown(child, &mut control, &config, &backend_use)
            .await
            .unwrap();
        assert_eq!(outcome, CycleOutcome::Shutdown);
        assert_eq!(events.recv().await, Some(TestRconEvent::Stop));
        drop(wake_sender);
        rcon_server.join().await.unwrap();
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
        let (_wake_root, wake_sender, wake_receiver) = wake_latch();
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let running = running_lifecycle(0);
        let (lifecycle_sender, lifecycle) = watch::channel(running);
        let mut control =
            test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);
        let backend_use = running_backend_use(running);
        let cycle = running.owned_running_cycle().unwrap();
        let player_inspection = initially_polled_player_inspection(&config).await;
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
        shutdown_sender.send(()).unwrap();
        assert_eq!(events.recv().await, Some(TestRconEvent::Closed));
        let outcome = monitor.await.unwrap().unwrap();
        assert_eq!(outcome, CycleOutcome::Shutdown);
        assert_eq!(lifecycle.borrow().phase, ServerPhase::Stopping);
        assert_eq!(events.recv().await, Some(TestRconEvent::Stop));
        drop(wake_sender);
        rcon_server.join().await.unwrap();
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
            assert!(rcon_server.join().await.unwrap_err().is_cancelled());
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
        let (_wake_root, wake_sender, wake_receiver) = wake_latch();
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let running = running_lifecycle(2);
        let (lifecycle_sender, mut lifecycle) = watch::channel(running);
        let mut control =
            test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);
        let backend_use = running_backend_use(running);
        let cycle = running.owned_running_cycle().unwrap();
        let blocking_reader = backend_use.acquire_shared(cycle).await;
        let player_inspection = initially_polled_player_inspection(&config).await;
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

        tokio::time::resume();
        shutdown_sender.send(()).unwrap();
        assert_eq!(events.recv().await, Some(TestRconEvent::Closed));
        assert_eq!(events.recv().await, Some(TestRconEvent::Stop));
        let outcome = monitor.await.unwrap().unwrap();
        assert_eq!(outcome, CycleOutcome::Shutdown);
        drop((blocking_reader, wake_sender));
        rcon_server.join().await.unwrap();
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
        let (_wake_root, wake_sender, wake_receiver) = wake_latch();
        let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
        let running = running_lifecycle(0);
        let (lifecycle_sender, lifecycle) = watch::channel(running);
        let mut control =
            test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);
        let backend_use = running_backend_use(running);
        let cycle = running.owned_running_cycle().unwrap();
        let blocking_reader = backend_use.acquire_shared(cycle).await;
        let player_inspection = initially_polled_player_inspection(&config).await;
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
        assert!(rcon_server.join().await.unwrap_err().is_cancelled());
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
        let (_wake_root, wake_sender, wake_receiver) = wake_latch();
        let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
        let running = running_lifecycle(0);
        let (lifecycle_sender, lifecycle) = watch::channel(running);
        let mut control =
            test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);
        let backend_use = running_backend_use(running);
        let cycle = running.owned_running_cycle().unwrap();
        let player_inspection = initially_polled_player_inspection(&config).await;
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
        assert!(rcon_server.join().await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn idle_stop_resets_failure_streak() {
        let (rcon_address, rcon_ready, rcon_server) = spawn_test_rcon_server().await;
        let config = test_config(&rcon_address);
        let child = process::launch(&config.launch).expect("test server process should launch");
        let (_wake_root, _wake_sender, wake_receiver) = wake_latch();
        let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
        let running = failed_lifecycle(ServerPhase::Running, 3);
        let (lifecycle_sender, lifecycle) = watch::channel(running);
        let mut control =
            test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);

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
        let (_wake_root, _wake_sender, wake_receiver) = wake_latch();
        let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
        let running = lifecycle_with_generation(ServerPhase::Running, 1);
        let (lifecycle_sender, lifecycle) = watch::channel(running);
        let mut control =
            test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);

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
        let (_wake_root, _wake_sender, wake_receiver) = wake_latch();
        let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
        let starting = lifecycle_with_generation(ServerPhase::Starting, 1);
        let (lifecycle_sender, lifecycle) = watch::channel(starting);
        let mut control =
            test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);
        let retry_delay = control.begin_failure_cleanup(FailureCategory::StartupTimedOut);

        let config = test_config(&Endpoint::new("127.0.0.1", 9));
        let error = finish_failed_cleanup(
            async { Err(anyhow::anyhow!("synthetic cleanup failure")) },
            &mut control,
            &config,
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
        let stopped = lifecycle_with_generation(ServerPhase::Stopped, 1);

        for phase in [ServerPhase::Starting, ServerPhase::Running] {
            let request = WakeRequest::observed(lifecycle_with_generation(phase, 1));
            assert!(!request.can_start(stopped));
        }
    }

    #[tokio::test]
    async fn closed_shutdown_source_precedes_queued_startup_wake() {
        let (_wake_root, wake_sender, wake_receiver) = wake_latch();
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let (lifecycle_sender, lifecycle) = watch::channel(LifecycleState::reconciling());
        wake_sender
            .enqueue(WakeRequest::observed(*lifecycle.borrow()))
            .expect("first wake request should fit");
        drop(shutdown_sender);

        let (rcon_address, backend) = refused_endpoints();
        run_test(
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
        let (wake_root, wake_sender, wake_receiver) = wake_latch();
        let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
        let (lifecycle_sender, mut lifecycle) = watch::channel(LifecycleState::reconciling());
        let (rcon_address, backend) = refused_endpoints();
        let supervisor = tokio::spawn(run_test(
            wake_receiver,
            shutdown_receiver,
            lifecycle_sender,
            test_config(&rcon_address),
            backend,
            test_backend_use(),
        ));

        wake_sender
            .enqueue(WakeRequest::observed(*lifecycle.borrow()))
            .expect("supervisor should accept the initial wake request");
        wait_for_phase(&mut lifecycle, ServerPhase::Starting).await;
        drop(wake_sender);
        wake_root.close();

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
        let (_wake_root, wake_sender, wake_receiver) = wake_latch();
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let (lifecycle_sender, mut lifecycle) = watch::channel(LifecycleState::reconciling());
        let initial = *lifecycle.borrow();
        wake_sender
            .enqueue(WakeRequest::observed(initial))
            .expect("first wake request should fit");
        for _ in 0..32 {
            wake_sender
                .enqueue(WakeRequest::observed(initial))
                .expect("equal wake keys should coalesce");
        }
        let (rcon_address, backend) = refused_endpoints();
        let supervisor = tokio::spawn(run_test(
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
                .enqueue(WakeRequest::observed(starting))
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
        let (_wake_root, wake_sender, wake_receiver) = wake_latch();
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let running = lifecycle_with_generation(ServerPhase::Running, 1);
        let (lifecycle_sender, lifecycle) = watch::channel(running);
        wake_sender
            .enqueue(WakeRequest::observed(running))
            .expect("running-phase wake request should fit");
        shutdown_sender
            .send(())
            .expect("running monitor should still await shutdown");
        let mut control =
            test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);
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
        let (_wake_root, wake_sender, wake_receiver) = wake_latch();
        let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
        let active = failed_lifecycle(ServerPhase::Running, 3);
        let (lifecycle_sender, mut lifecycle) = watch::channel(active);
        let stop = tokio::spawn(async move {
            let mut control =
                test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);
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
                    for _ in 0..32 {
                        wake_sender
                            .enqueue(WakeRequest::observed(stopping))
                            .expect("wake latch should remain open");
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
        let (_wake_root, wake_sender, wake_receiver) = wake_latch();
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let active = lifecycle_with_generation(ServerPhase::Running, 1);
        let (lifecycle_sender, mut lifecycle) = watch::channel(active);
        let stop = tokio::spawn(async move {
            let mut control =
                test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);
            control.begin_idle_stop(active.owned_running_cycle().unwrap());
            stop_committed_idle_server(child, &mut control, &config).await
        });

        wait_for_phase(&mut lifecycle, ServerPhase::Stopping).await;
        wake_sender
            .enqueue(WakeRequest::observed(*lifecycle.borrow()))
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
