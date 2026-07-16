use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result, ensure};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::task::JoinSet;
use tokio::time::{Instant, timeout_at};

use crate::runtime::RuntimeControlHandle;
use crate::supervisor::{
    AdmissionGate, FailureCategory, LifecycleStatusSnapshot, MutationEnvelope, MutationError,
    MutationOperation, MutationSuccess, ServerPhase,
};

#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

#[cfg(unix)]
pub(crate) use unix::{BoundEndpoint, InstanceLock};
#[cfg(windows)]
pub(crate) use windows::{BoundEndpoint, InstanceLock};

const PROTOCOL_VERSION: u16 = 2;
const MAX_PAYLOAD_LENGTH: u32 = 65_536;
const MAX_CONTROL_EXCHANGES: usize = 32;
const EXCHANGE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Request {
    List,
    Status {
        server_id: String,
    },
    Start {
        server_id: String,
        request_id: RequestId,
        expected_command_revision: u64,
    },
    Stop {
        server_id: String,
        request_id: RequestId,
        expected_command_revision: u64,
    },
    Restart {
        server_id: String,
        request_id: RequestId,
        expected_command_revision: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
struct RequestId(String);

impl RequestId {
    fn new(value: String) -> Option<Self> {
        (value.len() == 32
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)))
        .then_some(Self(value))
    }

    fn into_string(self) -> String {
        self.0
    }
}

impl<'de> Deserialize<'de> for RequestId {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).ok_or_else(|| serde::de::Error::custom("invalid request ID"))
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestDocument {
    #[serde(rename = "type")]
    operation: RequestDocumentOperation,
    #[serde(default)]
    server_id: Present<String>,
    #[serde(default)]
    request_id: Present<RequestId>,
    #[serde(default)]
    expected_command_revision: Present<u64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum RequestDocumentOperation {
    List,
    Status,
    Start,
    Stop,
    Restart,
}

#[derive(Default)]
enum Present<T> {
    #[default]
    Missing,
    Value(T),
}

impl<'de, T> Deserialize<'de> for Present<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        T::deserialize(deserializer).map(Self::Value)
    }
}

impl<'de> Deserialize<'de> for Request {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let document = RequestDocument::deserialize(deserializer)?;
        match (
            document.operation,
            document.server_id,
            document.request_id,
            document.expected_command_revision,
        ) {
            (
                RequestDocumentOperation::List,
                Present::Missing,
                Present::Missing,
                Present::Missing,
            ) => Ok(Self::List),
            (
                RequestDocumentOperation::Status,
                Present::Value(server_id),
                Present::Missing,
                Present::Missing,
            ) => Ok(Self::Status { server_id }),
            (
                operation @ (RequestDocumentOperation::Start
                | RequestDocumentOperation::Stop
                | RequestDocumentOperation::Restart),
                Present::Value(server_id),
                Present::Value(request_id),
                Present::Value(expected_command_revision),
            ) => Ok(match operation {
                RequestDocumentOperation::Start => Self::Start {
                    server_id,
                    request_id,
                    expected_command_revision,
                },
                RequestDocumentOperation::Stop => Self::Stop {
                    server_id,
                    request_id,
                    expected_command_revision,
                },
                RequestDocumentOperation::Restart => Self::Restart {
                    server_id,
                    request_id,
                    expected_command_revision,
                },
                RequestDocumentOperation::List | RequestDocumentOperation::Status => {
                    unreachable!("matched only mutation operations")
                }
            }),
            _ => Err(serde::de::Error::custom(
                "request fields do not match the operation",
            )),
        }
    }
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum WirePhase {
    Reconciling,
    Stopped,
    Starting,
    Running,
    Stopping,
    Cooldown,
    External,
    Conflict,
}

impl WirePhase {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Reconciling => "reconciling",
            Self::Stopped => "stopped",
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Stopping => "stopping",
            Self::Cooldown => "cooldown",
            Self::External => "external",
            Self::Conflict => "conflict",
        }
    }
}

impl From<ServerPhase> for WirePhase {
    fn from(phase: ServerPhase) -> Self {
        match phase {
            ServerPhase::Reconciling => Self::Reconciling,
            ServerPhase::Stopped => Self::Stopped,
            ServerPhase::Starting => Self::Starting,
            ServerPhase::Running => Self::Running,
            ServerPhase::Stopping => Self::Stopping,
            ServerPhase::Cooldown => Self::Cooldown,
            ServerPhase::External => Self::External,
            ServerPhase::Conflict => Self::Conflict,
        }
    }
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum WireFailureCategory {
    LaunchFailed,
    StartupTimedOut,
    ExitedBeforeReady,
    ExitedUnexpectedly,
}

impl WireFailureCategory {
    const fn as_str(self) -> &'static str {
        match self {
            Self::LaunchFailed => "launch_failed",
            Self::StartupTimedOut => "startup_timed_out",
            Self::ExitedBeforeReady => "exited_before_ready",
            Self::ExitedUnexpectedly => "exited_unexpectedly",
        }
    }
}

impl From<FailureCategory> for WireFailureCategory {
    fn from(category: FailureCategory) -> Self {
        match category {
            FailureCategory::LaunchFailed => Self::LaunchFailed,
            FailureCategory::StartupTimedOut => Self::StartupTimedOut,
            FailureCategory::ExitedBeforeReady => Self::ExitedBeforeReady,
            FailureCategory::ExitedUnexpectedly => Self::ExitedUnexpectedly,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum WireErrorCode {
    MalformedRequest,
    UnsupportedProtocolVersion,
    UnsupportedOperation,
    UnknownServerId,
    OperationRejected,
    ExternalBackendNotOwned,
    ConflictingBackendEvidence,
    OperationInProgress,
    StaleCommandRevision,
    RequestIdConflict,
    CommandQueueFull,
    InternalFailure,
}

impl WireErrorCode {
    const ALL: [Self; 12] = [
        Self::MalformedRequest,
        Self::UnsupportedProtocolVersion,
        Self::UnsupportedOperation,
        Self::UnknownServerId,
        Self::OperationRejected,
        Self::ExternalBackendNotOwned,
        Self::ConflictingBackendEvidence,
        Self::OperationInProgress,
        Self::StaleCommandRevision,
        Self::RequestIdConflict,
        Self::CommandQueueFull,
        Self::InternalFailure,
    ];

    const fn message(self) -> &'static str {
        match self {
            Self::MalformedRequest => "malformed request",
            Self::UnsupportedProtocolVersion => "unsupported control protocol version",
            Self::UnsupportedOperation => "unsupported operation",
            Self::UnknownServerId => "unknown server ID",
            Self::OperationRejected => "operation rejected",
            Self::ExternalBackendNotOwned => "external backend is not owned",
            Self::ConflictingBackendEvidence => "backend evidence conflicts",
            Self::OperationInProgress => "operation in progress",
            Self::StaleCommandRevision => "command revision is stale",
            Self::RequestIdConflict => "request ID conflicts",
            Self::CommandQueueFull => "command queue is full",
            Self::InternalFailure => "internal failure",
        }
    }
}

#[derive(Serialize)]
struct SuccessEnvelope<T> {
    #[serde(rename = "type")]
    response_type: &'static str,
    result: T,
}

#[derive(Serialize)]
struct ListResult<'a> {
    #[serde(rename = "type")]
    result_type: &'static str,
    servers: &'a [String],
}

#[derive(Serialize)]
struct StatusResult<'a> {
    #[serde(rename = "type")]
    result_type: &'static str,
    server: StatusFields<'a>,
}

#[derive(Serialize)]
struct StatusFields<'a> {
    server_id: &'a str,
    phase: WirePhase,
    failure_category: Option<WireFailureCategory>,
    failure_streak: u32,
    retry_after_ms: Option<u64>,
    command_revision: u64,
}

#[derive(Serialize)]
struct OperationResult<'a> {
    #[serde(rename = "type")]
    result_type: &'static str,
    server_id: &'a str,
    operation: &'static str,
    request_id: &'a str,
    command_revision: u64,
    disposition: &'static str,
}

#[derive(Serialize)]
struct ErrorEnvelope {
    #[serde(rename = "type")]
    response_type: &'static str,
    error: ErrorFields,
}

#[derive(Serialize)]
struct ErrorFields {
    code: WireErrorCode,
    message: &'static str,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum Response {
    Success { result: SuccessResult },
    Error { error: ReceivedError },
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum SuccessResult {
    List { servers: Vec<String> },
    Status { server: ReceivedStatus },
    Operation(ReceivedOperation),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReceivedStatus {
    server_id: String,
    phase: WirePhase,
    failure_category: RequiredNullable<WireFailureCategory>,
    failure_streak: u32,
    retry_after_ms: RequiredNullable<u64>,
    command_revision: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReceivedOperation {
    server_id: String,
    operation: String,
    request_id: String,
    command_revision: u64,
    disposition: String,
}

struct RequiredNullable<T>(Option<T>);

impl<'de, T> Deserialize<'de> for RequiredNullable<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Option::deserialize(deserializer).map(Self)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReceivedError {
    code: WireErrorCode,
    #[serde(rename = "message")]
    _message: String,
}

pub struct ServerStatus {
    server_id: String,
    phase: WirePhase,
    failure_category: Option<WireFailureCategory>,
    failure_streak: u32,
    retry_after_ms: Option<u64>,
    command_revision: u64,
}

impl ServerStatus {
    #[must_use]
    pub fn server_id(&self) -> &str {
        &self.server_id
    }

    #[must_use]
    pub const fn phase(&self) -> &'static str {
        self.phase.as_str()
    }

    #[must_use]
    pub const fn failure_category(&self) -> Option<&'static str> {
        match self.failure_category {
            Some(category) => Some(category.as_str()),
            None => None,
        }
    }

    #[must_use]
    pub const fn failure_streak(&self) -> u32 {
        self.failure_streak
    }

    #[must_use]
    pub const fn retry_after_ms(&self) -> Option<u64> {
        self.retry_after_ms
    }

    #[must_use]
    pub const fn command_revision(&self) -> u64 {
        self.command_revision
    }
}

pub struct OperationStatus {
    server_id: String,
    operation: String,
    request_id: String,
    command_revision: u64,
    disposition: String,
}

impl OperationStatus {
    #[must_use]
    pub fn server_id(&self) -> &str {
        &self.server_id
    }
    #[must_use]
    pub fn operation(&self) -> &str {
        &self.operation
    }
    #[must_use]
    pub fn request_id(&self) -> &str {
        &self.request_id
    }
    #[must_use]
    pub const fn command_revision(&self) -> u64 {
        self.command_revision
    }
    #[must_use]
    pub fn disposition(&self) -> &str {
        &self.disposition
    }
}

#[derive(Debug)]
pub enum ClientError {
    DaemonUnavailable,
    ProtocolMismatch,
    UnknownServerId,
    SubmissionUncertain,
    Operation(String),
}

impl fmt::Display for ClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DaemonUnavailable => formatter
                .write_str("the MCServerNap daemon is unavailable for the current OS principal"),
            Self::ProtocolMismatch => {
                formatter.write_str("the client and daemon use incompatible control protocols")
            }
            Self::UnknownServerId => formatter.write_str("unknown server ID"),
            Self::SubmissionUncertain => formatter.write_str(
                "the mutation outcome could not be validated; submission may or may not have been admitted",
            ),
            Self::Operation(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for ClientError {}

pub(crate) struct PreparedRegistry {
    server_ids: Vec<String>,
    list_response: Arc<[u8]>,
}

impl PreparedRegistry {
    pub(crate) fn new<'a>(server_ids: impl Iterator<Item = &'a String>) -> Result<Self> {
        let mut server_ids = server_ids.cloned().collect::<Vec<_>>();
        server_ids.sort();
        let list = SuccessEnvelope {
            response_type: "success",
            result: ListResult {
                result_type: "list",
                servers: &server_ids,
            },
        };
        let list_response = encode_frame(&list).map_err(|error| {
            anyhow::anyhow!(
                "control protocol v2 response limit exceeded by the complete server list: {error}"
            )
        })?;

        let longest_id = server_ids
            .iter()
            .max_by_key(|server_id| server_id.len())
            .context("control registry requires at least one configured server")?;
        let worst_case = SuccessEnvelope {
            response_type: "success",
            result: StatusResult {
                result_type: "status",
                server: StatusFields {
                    server_id: longest_id,
                    phase: WirePhase::Reconciling,
                    failure_category: Some(WireFailureCategory::ExitedUnexpectedly),
                    failure_streak: u32::MAX,
                    retry_after_ms: Some(u64::MAX),
                    command_revision: u64::MAX,
                },
            },
        };
        encode_frame(&worst_case).map_err(|error| {
            anyhow::anyhow!(
                "control protocol v2 response limit exceeded by a worst-case server status: {error}"
            )
        })?;

        let request_id = "ffffffffffffffffffffffffffffffff";
        for operation in ["start", "stop", "restart"] {
            let worst_case = SuccessEnvelope {
                response_type: "success",
                result: OperationResult {
                    result_type: "operation",
                    server_id: longest_id,
                    operation,
                    request_id,
                    command_revision: u64::MAX,
                    disposition: "already_satisfied",
                },
            };
            encode_frame(&worst_case).map_err(|error| {
                anyhow::anyhow!(
                    "control protocol v2 response limit exceeded by a worst-case {operation} success: {error}"
                )
            })?;
        }
        for code in WireErrorCode::ALL {
            encode_frame(&ErrorEnvelope {
                response_type: "error",
                error: ErrorFields {
                    code,
                    message: code.message(),
                },
            })
            .map_err(|error| {
                anyhow::anyhow!(
                    "control protocol v2 response limit exceeded by the {code:?} error: {error}"
                )
            })?;
        }

        Ok(Self {
            server_ids,
            list_response: list_response.into(),
        })
    }

    pub(crate) fn activate(
        self,
        handles: BTreeMap<String, RuntimeControlHandle>,
    ) -> Result<Registry> {
        ensure!(
            handles.len() == self.server_ids.len()
                && handles
                    .keys()
                    .zip(&self.server_ids)
                    .all(|(actual, expected)| actual == expected),
            "control registry activation did not receive every prepared server"
        );
        ensure!(
            handles
                .iter()
                .all(|(server_id, handle)| server_id == handle.server_id()),
            "control registry handle identity does not match its server ID"
        );
        Ok(Registry(Arc::new(RegistryInner {
            handles,
            list_response: self.list_response,
        })))
    }
}

#[derive(Clone)]
pub(crate) struct Registry(Arc<RegistryInner>);

struct RegistryInner {
    handles: BTreeMap<String, RuntimeControlHandle>,
    list_response: Arc<[u8]>,
}

pub(crate) struct ControlServer {
    endpoint: BoundEndpoint,
    registry: Registry,
    permits: Arc<Semaphore>,
}

impl ControlServer {
    pub(crate) fn new(endpoint: BoundEndpoint, registry: Registry) -> Self {
        Self {
            endpoint,
            registry,
            permits: Arc::new(Semaphore::new(MAX_CONTROL_EXCHANGES)),
        }
    }

    pub(crate) async fn run(
        mut self,
        mut shutdown: oneshot::Receiver<()>,
        admission_stopped: oneshot::Sender<()>,
    ) -> Result<()> {
        let mut clients = JoinSet::new();
        let accept_result = loop {
            tokio::select! {
                biased;

                _ = &mut shutdown => break Ok(()),
                completed = clients.join_next(), if !clients.is_empty() => {
                    if let Some(Err(error)) = completed {
                        break Err(anyhow::anyhow!("control-client task panicked: {error}"));
                    }
                }
                accepted = self.endpoint.accept(&self.permits) => {
                    match accepted.context("control endpoint accept failed") {
                        Err(error) => break Err(error),
                        Ok(Some((stream, permit))) => {
                            let registry = self.registry.clone();
                            let deadline = Instant::now() + EXCHANGE_TIMEOUT;
                            clients.spawn(async move {
                                serve_accepted_before(stream, registry, permit, deadline).await;
                            });
                        }
                        Ok(None) => log::debug!("Control connection rejected: exchange limit reached"),
                    }
                }
            }
        };

        let endpoint_result = self
            .endpoint
            .stop()
            .context("control endpoint cleanup failed");
        clients.abort_all();
        let mut client_panics = Vec::new();
        while let Some(completed) = clients.join_next().await {
            if let Err(error) = completed
                && error.is_panic()
            {
                client_panics.push(anyhow::anyhow!("control-client task panicked: {error}"));
            }
        }
        let _ = admission_stopped.send(());
        drop(self.registry);

        combine_control_results(accept_result, endpoint_result, client_panics, |error| {
            log::error!("Additional control failure during shutdown: {error:#}");
        })
    }
}

fn combine_control_results(
    accept_result: Result<()>,
    endpoint_result: Result<()>,
    client_panics: impl IntoIterator<Item = anyhow::Error>,
    mut report_secondary: impl FnMut(&anyhow::Error),
) -> Result<()> {
    let mut primary = None;
    let mut record = |result: Result<()>| {
        if let Err(error) = result {
            if primary.is_none() {
                primary = Some(error);
            } else {
                report_secondary(&error);
            }
        }
    };

    record(accept_result);
    record(endpoint_result);
    for error in client_panics {
        record(Err(error));
    }

    match primary {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

#[cfg(test)]
async fn serve_accepted<S>(stream: S, registry: Registry, permit: OwnedSemaphorePermit)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let deadline = Instant::now() + EXCHANGE_TIMEOUT;
    serve_accepted_before(stream, registry, permit, deadline).await;
}

async fn serve_accepted_before<S>(
    mut stream: S,
    registry: Registry,
    _permit: OwnedSemaphorePermit,
    deadline: Instant,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let result = timeout_at(deadline, serve_exchange(&mut stream, &registry)).await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(ExchangeError::Client)) => {
            log::debug!("Control exchange ended before a complete response");
        }
        Err(_) => log::debug!("Control exchange deadline elapsed"),
    }
}

enum ExchangeError {
    Client,
}

async fn serve_exchange<S>(stream: &mut S, registry: &Registry) -> Result<(), ExchangeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let request = match read_frame(stream).await {
        Ok(ReadFrame::Version { version, .. }) if version != PROTOCOL_VERSION => {
            let response = constant_error_frame(WireErrorCode::UnsupportedProtocolVersion);
            stream
                .write_all(&response)
                .await
                .map_err(|_| ExchangeError::Client)?;
            return Ok(());
        }
        Ok(ReadFrame::Version { body, .. }) => {
            let Ok(request) = serde_json::from_slice::<Request>(&body) else {
                let response = constant_error_frame(WireErrorCode::MalformedRequest);
                stream
                    .write_all(&response)
                    .await
                    .map_err(|_| ExchangeError::Client)?;
                return Ok(());
            };
            request
        }
        Err(error) => {
            log::debug!(
                "Control request framing rejected (declared payload length: {:?})",
                error.declared_length
            );
            let response = constant_error_frame(WireErrorCode::MalformedRequest);
            stream
                .write_all(&response)
                .await
                .map_err(|_| ExchangeError::Client)?;
            return Ok(());
        }
    };

    match request {
        Request::List => stream
            .write_all(&registry.0.list_response)
            .await
            .map_err(|_| ExchangeError::Client)?,
        Request::Status { server_id } => {
            serve_status(stream, registry, &server_id).await?;
        }
        mutation @ (Request::Start { .. } | Request::Stop { .. } | Request::Restart { .. }) => {
            let (server_id, request_id, expected_command_revision, operation) = match mutation {
                Request::Start {
                    server_id,
                    request_id,
                    expected_command_revision,
                } => (
                    server_id,
                    request_id,
                    expected_command_revision,
                    MutationOperation::Start,
                ),
                Request::Stop {
                    server_id,
                    request_id,
                    expected_command_revision,
                } => (
                    server_id,
                    request_id,
                    expected_command_revision,
                    MutationOperation::Stop,
                ),
                Request::Restart {
                    server_id,
                    request_id,
                    expected_command_revision,
                } => (
                    server_id,
                    request_id,
                    expected_command_revision,
                    MutationOperation::Restart,
                ),
                Request::List | Request::Status { .. } => {
                    unreachable!("matched only mutation operations")
                }
            };
            serve_mutation(
                stream,
                registry,
                server_id,
                request_id,
                expected_command_revision,
                operation,
            )
            .await?;
        }
    }
    Ok(())
}

async fn serve_status<S>(
    stream: &mut S,
    registry: &Registry,
    server_id: &str,
) -> Result<(), ExchangeError>
where
    S: AsyncWrite + Unpin,
{
    let Some(handle) = registry.0.handles.get(server_id) else {
        let response = constant_error_frame(WireErrorCode::UnknownServerId);
        stream
            .write_all(&response)
            .await
            .map_err(|_| ExchangeError::Client)?;
        return Ok(());
    };
    let Some(snapshot) = handle.snapshot() else {
        return Err(ExchangeError::Client);
    };
    let response = status_frame(handle.server_id(), snapshot, Instant::now());
    stream
        .write_all(&response)
        .await
        .map_err(|_| ExchangeError::Client)
}

struct AdmissionCancellation {
    gate: Arc<AdmissionGate>,
    active: bool,
}

impl AdmissionCancellation {
    fn disarm(&mut self) {
        self.active = false;
    }
}

impl Drop for AdmissionCancellation {
    fn drop(&mut self) {
        if self.active {
            self.gate.cancel();
        }
    }
}

async fn serve_mutation<S>(
    stream: &mut S,
    registry: &Registry,
    server_id: String,
    request_id: RequestId,
    expected_command_revision: u64,
    operation: MutationOperation,
) -> Result<(), ExchangeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let Some(handle) = registry.0.handles.get(&server_id) else {
        let response = constant_error_frame(WireErrorCode::UnknownServerId);
        stream
            .write_all(&response)
            .await
            .map_err(|_| ExchangeError::Client)?;
        return Ok(());
    };
    let sender = handle.mutation_sender();
    let permit = match sender.try_reserve_owned() {
        Ok(permit) => permit,
        Err(mpsc::error::TrySendError::Full(_)) => {
            let response = constant_error_frame(WireErrorCode::CommandQueueFull);
            stream
                .write_all(&response)
                .await
                .map_err(|_| ExchangeError::Client)?;
            return Ok(());
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            let response = constant_error_frame(WireErrorCode::InternalFailure);
            stream
                .write_all(&response)
                .await
                .map_err(|_| ExchangeError::Client)?;
            return Ok(());
        }
    };
    let gate = Arc::new(AdmissionGate::pending());
    let mut cancellation = AdmissionCancellation {
        gate: Arc::clone(&gate),
        active: true,
    };
    let (reply, reply_receiver) = oneshot::channel();
    permit.send(MutationEnvelope {
        operation,
        request_id: request_id.into_string(),
        expected_command_revision,
        reply,
        admission: gate,
    });

    let mut extra = [0_u8; 1];
    tokio::pin!(reply_receiver);
    let reply = tokio::select! {
        biased;

        result = &mut reply_receiver => result.map_err(|_| ExchangeError::Client)?,
        _ = stream.read(&mut extra) => return Err(ExchangeError::Client),
    };
    cancellation.disarm();
    let response = match reply {
        Ok(success) => operation_frame(&success),
        Err(error) => mutation_error_frame(error),
    };
    stream
        .write_all(&response)
        .await
        .map_err(|_| ExchangeError::Client)?;
    Ok(())
}

fn operation_frame(success: &MutationSuccess) -> Vec<u8> {
    encode_frame(&SuccessEnvelope {
        response_type: "success",
        result: OperationResult {
            result_type: "operation",
            server_id: &success.server_id,
            operation: success.operation.as_str(),
            request_id: &success.request_id,
            command_revision: success.command_revision,
            disposition: success.disposition.as_str(),
        },
    })
    .expect("prepared operation response bound must cover every success")
}

fn mutation_error_frame(error: MutationError) -> Vec<u8> {
    let code = match error {
        MutationError::OperationRejected => WireErrorCode::OperationRejected,
        MutationError::ExternalBackendNotOwned => WireErrorCode::ExternalBackendNotOwned,
        MutationError::ConflictingBackendEvidence => WireErrorCode::ConflictingBackendEvidence,
        MutationError::OperationInProgress => WireErrorCode::OperationInProgress,
        MutationError::StaleCommandRevision => WireErrorCode::StaleCommandRevision,
        MutationError::RequestIdConflict => WireErrorCode::RequestIdConflict,
        MutationError::InternalFailure => WireErrorCode::InternalFailure,
    };
    constant_error_frame(code)
}

fn status_frame(
    server_id: &str,
    snapshot: LifecycleStatusSnapshot,
    snapshot_now: Instant,
) -> Vec<u8> {
    let response = SuccessEnvelope {
        response_type: "success",
        result: StatusResult {
            result_type: "status",
            server: StatusFields {
                server_id,
                phase: snapshot.phase.into(),
                failure_category: snapshot.failure.map(Into::into),
                failure_streak: snapshot.failure_streak,
                retry_after_ms: snapshot
                    .retry_at
                    .map(|retry_at| retry_milliseconds(retry_at, snapshot_now)),
                command_revision: snapshot.command_revision,
            },
        },
    };
    encode_frame(&response).expect("prepared status response bound must cover every snapshot")
}

fn retry_milliseconds(retry_at: Instant, snapshot_now: Instant) -> u64 {
    let remaining = retry_at.saturating_duration_since(snapshot_now);
    let milliseconds = remaining.as_millis();
    let rounded = milliseconds + u128::from(!remaining.subsec_nanos().is_multiple_of(1_000_000));
    u64::try_from(rounded).expect("supervisor retry delays must fit the v2 millisecond field")
}

fn error_frame(code: WireErrorCode, message: &'static str) -> Vec<u8> {
    encode_frame(&ErrorEnvelope {
        response_type: "error",
        error: ErrorFields { code, message },
    })
    .expect("protocol-v2 error responses are bounded constants")
}

fn constant_error_frame(code: WireErrorCode) -> Vec<u8> {
    error_frame(code, code.message())
}

fn encode_frame(value: &impl Serialize) -> Result<Vec<u8>> {
    let maximum_body_length = MAX_PAYLOAD_LENGTH as usize - 2;
    let mut body = BoundedJson::new(maximum_body_length);
    serde_json::to_writer(&mut body, value).context("failed to encode bounded JSON")?;
    let body = body.into_inner();
    let payload_length = body.len() + 2;
    let payload_length = u32::try_from(payload_length).expect("bounded payload length fits u32");
    let mut frame = Vec::with_capacity(payload_length as usize + 4);
    frame.extend_from_slice(&payload_length.to_be_bytes());
    frame.extend_from_slice(&PROTOCOL_VERSION.to_be_bytes());
    frame.extend_from_slice(&body);
    Ok(frame)
}

struct BoundedJson {
    bytes: Vec<u8>,
    maximum: usize,
}

impl BoundedJson {
    fn new(maximum: usize) -> Self {
        Self {
            bytes: Vec::new(),
            maximum,
        }
    }

    fn into_inner(self) -> Vec<u8> {
        self.bytes
    }
}

impl io::Write for BoundedJson {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if buffer.len() > self.maximum - self.bytes.len() {
            return Err(io::Error::other(format!(
                "payload exceeds the {MAX_PAYLOAD_LENGTH}-byte protocol limit"
            )));
        }
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

enum ReadFrame {
    Version { version: u16, body: Vec<u8> },
}

#[derive(Debug)]
struct FrameError {
    declared_length: Option<u32>,
}

async fn read_frame<R>(reader: &mut R) -> std::result::Result<ReadFrame, FrameError>
where
    R: AsyncRead + Unpin,
{
    let mut header = [0; 4];
    reader
        .read_exact(&mut header)
        .await
        .map_err(|_| FrameError {
            declared_length: None,
        })?;
    let payload_length = u32::from_be_bytes(header);
    if !(2..=MAX_PAYLOAD_LENGTH).contains(&payload_length) {
        return Err(FrameError {
            declared_length: Some(payload_length),
        });
    }
    let mut version = [0; 2];
    reader
        .read_exact(&mut version)
        .await
        .map_err(|_| FrameError {
            declared_length: Some(payload_length),
        })?;
    let version = u16::from_be_bytes(version);
    if version != PROTOCOL_VERSION {
        return Ok(ReadFrame::Version {
            version,
            body: Vec::new(),
        });
    }
    let body_length = usize::try_from(payload_length - 2).expect("bounded u32 length fits usize");
    let mut body = vec![0; body_length];
    reader.read_exact(&mut body).await.map_err(|_| FrameError {
        declared_length: Some(payload_length),
    })?;
    Ok(ReadFrame::Version { version, body })
}

pub async fn list() -> std::result::Result<Vec<String>, ClientError> {
    match request(Request::List).await? {
        SuccessResult::List { servers } => validate_server_list(servers),
        SuccessResult::Status { .. } | SuccessResult::Operation(_) => {
            Err(ClientError::DaemonUnavailable)
        }
    }
}

fn validate_server_list(servers: Vec<String>) -> std::result::Result<Vec<String>, ClientError> {
    if servers.is_empty() || !servers.windows(2).all(|pair| pair[0] < pair[1]) {
        return Err(ClientError::DaemonUnavailable);
    }
    Ok(servers)
}

pub async fn status(server_id: String) -> std::result::Result<ServerStatus, ClientError> {
    let expected_server_id = server_id.clone();
    match request(Request::Status { server_id }).await? {
        SuccessResult::Status { server } if server.server_id == expected_server_id => {
            Ok(ServerStatus {
                server_id: server.server_id,
                phase: server.phase,
                failure_category: server.failure_category.0,
                failure_streak: server.failure_streak,
                retry_after_ms: server.retry_after_ms.0,
                command_revision: server.command_revision,
            })
        }
        SuccessResult::Status { .. } | SuccessResult::List { .. } | SuccessResult::Operation(_) => {
            Err(ClientError::DaemonUnavailable)
        }
    }
}

pub async fn start(
    server_id: String,
    request_id: String,
    expected_command_revision: u64,
) -> std::result::Result<OperationStatus, ClientError> {
    let expected_result_revision = expected_command_revision.checked_add(1);
    mutate(
        Request::Start {
            server_id: server_id.clone(),
            request_id: RequestId::new(request_id.clone())
                .ok_or_else(|| ClientError::Operation("invalid request ID".to_owned()))?,
            expected_command_revision,
        },
        server_id,
        request_id,
        "start",
        expected_result_revision,
    )
    .await
}

pub async fn stop(
    server_id: String,
    request_id: String,
    expected_command_revision: u64,
) -> std::result::Result<OperationStatus, ClientError> {
    let expected_result_revision = expected_command_revision.checked_add(1);
    mutate(
        Request::Stop {
            server_id: server_id.clone(),
            request_id: RequestId::new(request_id.clone())
                .ok_or_else(|| ClientError::Operation("invalid request ID".to_owned()))?,
            expected_command_revision,
        },
        server_id,
        request_id,
        "stop",
        expected_result_revision,
    )
    .await
}

pub async fn restart(
    server_id: String,
    request_id: String,
    expected_command_revision: u64,
) -> std::result::Result<OperationStatus, ClientError> {
    let expected_result_revision = expected_command_revision.checked_add(1);
    mutate(
        Request::Restart {
            server_id: server_id.clone(),
            request_id: RequestId::new(request_id.clone())
                .ok_or_else(|| ClientError::Operation("invalid request ID".to_owned()))?,
            expected_command_revision,
        },
        server_id,
        request_id,
        "restart",
        expected_result_revision,
    )
    .await
}

async fn mutate(
    request: Request,
    expected_server_id: String,
    expected_request_id: String,
    expected_operation: &'static str,
    expected_result_revision: Option<u64>,
) -> std::result::Result<OperationStatus, ClientError> {
    let deadline = Instant::now() + EXCHANGE_TIMEOUT;
    let frame =
        encode_frame(&request).map_err(|error| ClientError::Operation(error.to_string()))?;
    let mut stream = platform_connect(deadline)
        .await
        .map_err(|_| ClientError::DaemonUnavailable)?;
    let result = match timeout_at(deadline, client_exchange(&mut stream, &frame)).await {
        Ok(result) => result.map_err(|error| match error {
            ClientError::DaemonUnavailable => ClientError::SubmissionUncertain,
            other => other,
        })?,
        Err(_) => return Err(ClientError::SubmissionUncertain),
    };
    match result {
        SuccessResult::Operation(operation) => validate_mutation_success(
            operation,
            &expected_server_id,
            &expected_request_id,
            expected_operation,
            expected_result_revision,
        ),
        _ => Err(ClientError::SubmissionUncertain),
    }
}

fn validate_mutation_success(
    operation: ReceivedOperation,
    expected_server_id: &str,
    expected_request_id: &str,
    expected_operation: &str,
    expected_result_revision: Option<u64>,
) -> std::result::Result<OperationStatus, ClientError> {
    if operation.server_id != expected_server_id
        || operation.request_id != expected_request_id
        || operation.operation != expected_operation
        || Some(operation.command_revision) != expected_result_revision
        || match expected_operation {
            "restart" => operation.disposition != "accepted",
            _ => !matches!(
                operation.disposition.as_str(),
                "accepted" | "already_satisfied"
            ),
        }
    {
        return Err(ClientError::SubmissionUncertain);
    }

    Ok(OperationStatus {
        server_id: operation.server_id,
        operation: operation.operation,
        request_id: operation.request_id,
        command_revision: operation.command_revision,
        disposition: operation.disposition,
    })
}

async fn request(request: Request) -> std::result::Result<SuccessResult, ClientError> {
    let deadline = Instant::now() + EXCHANGE_TIMEOUT;
    match timeout_at(deadline, request_before(deadline, request)).await {
        Ok(result) => result,
        Err(_) => Err(ClientError::DaemonUnavailable),
    }
}

async fn request_before(
    deadline: Instant,
    request: Request,
) -> std::result::Result<SuccessResult, ClientError> {
    let frame =
        encode_frame(&request).map_err(|error| ClientError::Operation(error.to_string()))?;
    let mut stream = platform_connect(deadline)
        .await
        .map_err(|_| ClientError::DaemonUnavailable)?;
    client_exchange(&mut stream, &frame).await
}

async fn client_exchange<S>(
    stream: &mut S,
    frame: &[u8],
) -> std::result::Result<SuccessResult, ClientError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    stream
        .write_all(frame)
        .await
        .map_err(|_| ClientError::DaemonUnavailable)?;
    let response = match read_frame(stream).await {
        Ok(ReadFrame::Version { version, .. }) if version != PROTOCOL_VERSION => {
            return Err(ClientError::ProtocolMismatch);
        }
        Ok(ReadFrame::Version { body, .. }) => {
            serde_json::from_slice::<Response>(&body).map_err(|_| ClientError::DaemonUnavailable)?
        }
        Err(_) => return Err(ClientError::DaemonUnavailable),
    };
    match response {
        Response::Success { result } => Ok(result),
        Response::Error { error } => match error.code {
            WireErrorCode::UnknownServerId => Err(ClientError::UnknownServerId),
            WireErrorCode::UnsupportedProtocolVersion => Err(ClientError::ProtocolMismatch),
            WireErrorCode::MalformedRequest => Err(ClientError::DaemonUnavailable),
            WireErrorCode::UnsupportedOperation => {
                Err(ClientError::Operation("unsupported operation".to_owned()))
            }
            WireErrorCode::OperationRejected => {
                Err(ClientError::Operation("operation rejected".to_owned()))
            }
            WireErrorCode::ExternalBackendNotOwned => Err(ClientError::Operation(
                "external backend is not owned".to_owned(),
            )),
            WireErrorCode::ConflictingBackendEvidence => Err(ClientError::Operation(
                "backend evidence conflicts".to_owned(),
            )),
            WireErrorCode::OperationInProgress => {
                Err(ClientError::Operation("operation in progress".to_owned()))
            }
            WireErrorCode::StaleCommandRevision => Err(ClientError::Operation(
                "command revision is stale".to_owned(),
            )),
            WireErrorCode::RequestIdConflict => {
                Err(ClientError::Operation("request ID conflicts".to_owned()))
            }
            WireErrorCode::CommandQueueFull => {
                Err(ClientError::Operation("command queue is full".to_owned()))
            }
            WireErrorCode::InternalFailure => {
                Err(ClientError::Operation("daemon internal failure".to_owned()))
            }
        },
    }
}

#[cfg(unix)]
async fn platform_connect(deadline: Instant) -> std::io::Result<unix::ControlStream> {
    unix::connect(deadline).await
}

#[cfg(windows)]
async fn platform_connect(deadline: Instant) -> std::io::Result<windows::ControlStream> {
    windows::connect(deadline).await
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use serde_json::Value;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _, DuplexStream};
    use tokio::sync::{Semaphore, watch};
    use tokio::time::{Duration, Instant, advance};

    use super::*;
    use crate::runtime::RuntimeStatusHandle;
    use crate::supervisor::{FailureCategory, LifecycleState};

    fn raw_frame(version: u16, body: &[u8]) -> Vec<u8> {
        let payload_length = u32::try_from(body.len() + 2).unwrap();
        let mut frame = Vec::with_capacity(body.len() + 6);
        frame.extend_from_slice(&payload_length.to_be_bytes());
        frame.extend_from_slice(&version.to_be_bytes());
        frame.extend_from_slice(body);
        frame
    }

    fn one_server_registry(lifecycle: LifecycleState) -> (Registry, watch::Sender<LifecycleState>) {
        let prepared = PreparedRegistry::new([&"survival".to_owned()].into_iter()).unwrap();
        let (handle, sender) = RuntimeStatusHandle::test("survival", lifecycle);
        let handles = BTreeMap::from([("survival".to_owned(), handle)]);
        (prepared.activate(handles).unwrap(), sender)
    }

    async fn exchange(request: &[u8], registry: Registry) -> Vec<u8> {
        let permits = Arc::new(Semaphore::new(1));
        let permit = Arc::clone(&permits).try_acquire_owned().unwrap();
        let (mut client, server) = tokio::io::duplex(70_000);
        let task = tokio::spawn(serve_accepted(server, registry, permit));
        client.write_all(request).await.unwrap();
        client.shutdown().await.unwrap();
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        task.await.unwrap();
        assert_eq!(permits.available_permits(), 1);
        response
    }

    async fn client_receives(response: Vec<u8>) -> std::result::Result<SuccessResult, ClientError> {
        let (mut client, mut daemon) = tokio::io::duplex(70_000);
        let daemon_task = tokio::spawn(async move {
            let _ = read_frame(&mut daemon).await;
            daemon.write_all(&response).await.unwrap();
        });
        let request = encode_frame(&Request::List).unwrap();
        let result = client_exchange(&mut client, &request).await;
        daemon_task.await.unwrap();
        result
    }

    fn frame_body(frame: &[u8]) -> &[u8] {
        let payload_length = u32::from_be_bytes(frame[..4].try_into().unwrap()) as usize;
        assert_eq!(frame.len(), payload_length + 4);
        assert_eq!(
            u16::from_be_bytes(frame[4..6].try_into().unwrap()),
            PROTOCOL_VERSION
        );
        &frame[6..]
    }

    fn response_code(frame: &[u8]) -> WireErrorCode {
        let response: Response = serde_json::from_slice(frame_body(frame)).unwrap();
        let Response::Error { error } = response else {
            panic!("expected an error response");
        };
        error.code
    }

    fn received_operation(revision: u64, disposition: &str) -> ReceivedOperation {
        ReceivedOperation {
            server_id: "survival".to_owned(),
            operation: "start".to_owned(),
            request_id: "11111111111111111111111111111111".to_owned(),
            command_revision: revision,
            disposition: disposition.to_owned(),
        }
    }

    fn validate_test_operation(
        operation: ReceivedOperation,
        expected_revision: Option<u64>,
    ) -> std::result::Result<OperationStatus, ClientError> {
        validate_mutation_success(
            operation,
            "survival",
            "11111111111111111111111111111111",
            "start",
            expected_revision,
        )
    }

    #[test]
    fn mutation_success_requires_the_exact_next_revision() {
        for disposition in ["accepted", "already_satisfied"] {
            let status = validate_test_operation(received_operation(13, disposition), Some(13))
                .expect("the exact next revision should be accepted");
            assert_eq!(status.command_revision(), 13);
            assert_eq!(status.disposition(), disposition);
        }

        // An exact server-side replay returns the same originally expected next revision.
        assert_eq!(
            validate_test_operation(received_operation(13, "accepted"), Some(13))
                .unwrap()
                .command_revision(),
            13
        );

        for revision in [11, 12, 14, u64::MAX] {
            assert!(matches!(
                validate_test_operation(received_operation(revision, "accepted"), Some(13)),
                Err(ClientError::SubmissionUncertain)
            ));
        }
        assert!(matches!(
            validate_test_operation(received_operation(u64::MAX, "accepted"), None),
            Err(ClientError::SubmissionUncertain)
        ));
    }

    #[test]
    fn mutation_success_identity_and_disposition_remain_strict() {
        let mut cases = Vec::new();

        let mut wrong_server = received_operation(13, "accepted");
        wrong_server.server_id = "creative".to_owned();
        cases.push(wrong_server);

        let mut wrong_request = received_operation(13, "accepted");
        wrong_request.request_id = "22222222222222222222222222222222".to_owned();
        cases.push(wrong_request);

        let mut wrong_operation = received_operation(13, "accepted");
        wrong_operation.operation = "stop".to_owned();
        cases.push(wrong_operation);

        cases.push(received_operation(13, "unknown"));

        for operation in cases {
            assert!(matches!(
                validate_test_operation(operation, Some(13)),
                Err(ClientError::SubmissionUncertain)
            ));
        }
    }

    fn combine_test_results(
        accept_failure: Option<&str>,
        endpoint_failure: Option<&str>,
        client_panics: &[&str],
    ) -> (Result<()>, Vec<String>) {
        let result = |failure: Option<&str>| match failure {
            Some(message) => Err(anyhow::anyhow!(message.to_owned())),
            None => Ok(()),
        };
        let mut secondary = Vec::new();
        let combined = combine_control_results(
            result(accept_failure),
            result(endpoint_failure),
            client_panics
                .iter()
                .map(|message| anyhow::anyhow!((*message).to_owned())),
            |error| secondary.push(error.to_string()),
        );
        (combined, secondary)
    }

    #[test]
    fn control_result_combination_succeeds_when_every_outcome_succeeds() {
        let (result, secondary) = combine_test_results(None, None, &[]);
        assert!(result.is_ok());
        assert!(secondary.is_empty());
    }

    #[test]
    fn each_individual_control_failure_is_returned_without_secondary_reporting() {
        for (accept, endpoint, panics, expected) in [
            (Some("accept"), None, &[][..], "accept"),
            (None, Some("endpoint"), &[][..], "endpoint"),
            (None, None, &["client"][..], "client"),
        ] {
            let (result, secondary) = combine_test_results(accept, endpoint, panics);
            assert_eq!(result.unwrap_err().to_string(), expected);
            assert!(secondary.is_empty());
        }
    }

    #[test]
    fn control_result_combination_preserves_precedence_and_reports_later_failures() {
        let (result, secondary) = combine_test_results(
            Some("accept"),
            Some("endpoint"),
            &["client one", "client two"],
        );
        assert_eq!(result.unwrap_err().to_string(), "accept");
        assert_eq!(secondary, ["endpoint", "client one", "client two"]);

        let (result, secondary) = combine_test_results(None, Some("endpoint"), &["client"]);
        assert_eq!(result.unwrap_err().to_string(), "endpoint");
        assert_eq!(secondary, ["client"]);
    }

    #[test]
    fn client_accepts_nonempty_strictly_sorted_server_lists() {
        assert_eq!(
            validate_server_list(vec!["survival".to_owned()]).unwrap(),
            ["survival"]
        );
        assert_eq!(
            validate_server_list(vec!["creative".to_owned(), "survival".to_owned()]).unwrap(),
            ["creative", "survival"]
        );
    }

    #[test]
    fn client_rejects_empty_duplicate_and_unsorted_server_lists() {
        for servers in [
            Vec::new(),
            vec!["survival".to_owned(), "survival".to_owned()],
            vec!["survival".to_owned(), "creative".to_owned()],
        ] {
            assert!(matches!(
                validate_server_list(servers),
                Err(ClientError::DaemonUnavailable)
            ));
        }
    }

    #[test]
    fn frame_bytes_are_exact_big_endian_protocol_v2() {
        let frame = encode_frame(&Request::List).unwrap();
        let mut expected = vec![0, 0, 0, 17, 0, 2];
        expected.extend_from_slice(br#"{"type":"list"}"#);
        assert_eq!(frame, expected);

        let restart = Request::Restart {
            server_id: "survival".to_owned(),
            request_id: RequestId::new("11111111111111111111111111111111".to_owned()).unwrap(),
            expected_command_revision: 12,
        };
        assert_eq!(
            encode_frame(&restart).unwrap(),
            raw_frame(
                PROTOCOL_VERSION,
                br#"{"type":"restart","server_id":"survival","request_id":"11111111111111111111111111111111","expected_command_revision":12}"#,
            )
        );
        assert!(WireErrorCode::ALL.contains(&WireErrorCode::UnsupportedOperation));
        assert_eq!(
            response_code(&constant_error_frame(WireErrorCode::UnsupportedOperation)),
            WireErrorCode::UnsupportedOperation
        );
    }

    #[test]
    fn bounded_json_rejects_before_copying_an_oversized_chunk() {
        let mut writer = BoundedJson::new(8);
        assert!(std::io::Write::write_all(&mut writer, &[0; 9]).is_err());
        assert!(writer.bytes.is_empty());
    }

    #[tokio::test]
    async fn fragmented_header_version_and_body_decode_successfully() {
        let (registry, _authority) =
            one_server_registry(LifecycleState::test_phase(ServerPhase::Stopped));
        let request = encode_frame(&Request::List).unwrap();
        let permits = Arc::new(Semaphore::new(1));
        let permit = Arc::clone(&permits).try_acquire_owned().unwrap();
        let (mut client, server) = tokio::io::duplex(128);
        let task = tokio::spawn(serve_accepted(server, registry, permit));
        for byte in request {
            client.write_all(&[byte]).await.unwrap();
            tokio::task::yield_now().await;
        }
        client.shutdown().await.unwrap();
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        task.await.unwrap();
        assert!(matches!(
            serde_json::from_slice::<Response>(frame_body(&response)).unwrap(),
            Response::Success {
                result: SuccessResult::List { .. }
            }
        ));
    }

    #[tokio::test]
    async fn coalesced_second_request_is_ignored_after_one_response() {
        let (registry, _authority) =
            one_server_registry(LifecycleState::test_phase(ServerPhase::Stopped));
        let first = encode_frame(&Request::List).unwrap();
        let second = encode_frame(&Request::Status {
            server_id: "survival".to_owned(),
        })
        .unwrap();
        let response = exchange(&[first, second].concat(), registry).await;
        assert!(matches!(
            serde_json::from_slice::<Response>(frame_body(&response)).unwrap(),
            Response::Success {
                result: SuccessResult::List { .. }
            }
        ));
    }

    #[tokio::test]
    async fn payload_length_boundaries_are_enforced_before_body_read() {
        for length in [0_u32, 1, 65_537] {
            let (registry, _authority) =
                one_server_registry(LifecycleState::test_phase(ServerPhase::Stopped));
            let response = exchange(&length.to_be_bytes(), registry).await;
            assert!(matches!(
                response_code(&response),
                WireErrorCode::MalformedRequest
            ));
        }

        let (registry, _authority) =
            one_server_registry(LifecycleState::test_phase(ServerPhase::Stopped));
        let mut maximum = Vec::with_capacity(65_540);
        maximum.extend_from_slice(&65_536_u32.to_be_bytes());
        maximum.extend_from_slice(&PROTOCOL_VERSION.to_be_bytes());
        maximum.resize(65_540, b'x');
        let response = exchange(&maximum, registry).await;
        assert!(matches!(
            response_code(&response),
            WireErrorCode::MalformedRequest
        ));
    }

    #[tokio::test]
    async fn strict_request_schema_rejects_every_malformed_form() {
        let bodies: &[&[u8]] = &[
            &[0xff],
            br#"{"type":}"#,
            br#"{"type":"list","type":"list"}"#,
            br#"{"type":"list","extra":1}"#,
            br"{}",
            br#"{"type":"unknown"}"#,
            br#"{"type":"status"}"#,
            br#"{"type":"list"}{"type":"list"}"#,
        ];
        for body in bodies {
            let (registry, _authority) =
                one_server_registry(LifecycleState::test_phase(ServerPhase::Stopped));
            let response = exchange(&raw_frame(PROTOCOL_VERSION, body), registry).await;
            assert!(
                matches!(
                    serde_json::from_slice::<Response>(frame_body(&response)).unwrap(),
                    Response::Error {
                        error: ReceivedError {
                            code: WireErrorCode::MalformedRequest,
                            ..
                        }
                    }
                ),
                "accepted malformed body: {:?}",
                String::from_utf8_lossy(body)
            );
        }
    }

    #[test]
    fn mutation_request_schema_and_request_ids_are_strict() {
        for (body, expected_operation) in [
            (
                br#"{"type":"start","server_id":"survival","request_id":"11111111111111111111111111111111","expected_command_revision":12}"#.as_slice(),
                "start",
            ),
            (
                br#"{"type":"stop","server_id":"survival","request_id":"22222222222222222222222222222222","expected_command_revision":12}"#.as_slice(),
                "stop",
            ),
            (
                br#"{"type":"restart","server_id":"survival","request_id":"abcdefabcdefabcdefabcdefabcdefab","expected_command_revision":12}"#.as_slice(),
                "restart",
            ),
        ] {
            let request = serde_json::from_slice::<Request>(body).unwrap();
            let operation = match request {
                Request::Start { .. } => "start",
                Request::Stop { .. } => "stop",
                Request::Restart { .. } => "restart",
                Request::List | Request::Status { .. } => "read_only",
            };
            assert_eq!(operation, expected_operation);
        }

        for request_id in [
            "1111111111111111111111111111111",
            "111111111111111111111111111111111",
            "ABCDEFABCDEFABCDEFABCDEFABCDEFAB",
            "gggggggggggggggggggggggggggggggg",
            "11111111-1111-1111-1111-111111111111",
        ] {
            let body = format!(
                r#"{{"type":"start","server_id":"survival","request_id":"{request_id}","expected_command_revision":0}}"#
            );
            assert!(
                serde_json::from_str::<Request>(&body).is_err(),
                "accepted {request_id}"
            );
        }
        for body in [
            br#"{"type":"start","server_id":"survival","request_id":"11111111111111111111111111111111"}"#.as_slice(),
            br#"{"type":"stop","server_id":"survival","request_id":"11111111111111111111111111111111","expected_command_revision":0,"extra":1}"#.as_slice(),
            br#"{"type":"restart","server_id":"survival","request_id":"11111111111111111111111111111111","request_id":"11111111111111111111111111111111","expected_command_revision":0}"#.as_slice(),
        ] {
            assert!(serde_json::from_slice::<Request>(body).is_err());
        }
    }

    #[tokio::test]
    async fn valid_restart_is_dispatched_through_supervisor_admission() {
        let prepared = PreparedRegistry::new([&"survival".to_owned()].into_iter()).unwrap();
        let (handle, _authority, mut mutations) = RuntimeStatusHandle::test_with_mutations(
            "survival",
            LifecycleState::test_phase(ServerPhase::Running),
        );
        let registry = prepared
            .activate(BTreeMap::from([("survival".to_owned(), handle)]))
            .unwrap();
        let request = Request::Restart {
            server_id: "survival".to_owned(),
            request_id: RequestId::new("11111111111111111111111111111111".to_owned()).unwrap(),
            expected_command_revision: 0,
        };
        let permits = Arc::new(Semaphore::new(1));
        let permit = Arc::clone(&permits).try_acquire_owned().unwrap();
        let (mut client, server) = tokio::io::duplex(70_000);
        let exchange = tokio::spawn(serve_accepted(server, registry, permit));
        client
            .write_all(&encode_frame(&request).unwrap())
            .await
            .unwrap();
        let envelope = mutations.recv().await.unwrap();
        assert_eq!(envelope.operation, MutationOperation::Restart);
        assert_eq!(envelope.expected_command_revision, 0);
        assert_eq!(envelope.request_id, "11111111111111111111111111111111");
        envelope
            .reply
            .send(Ok(MutationSuccess {
                server_id: "survival".to_owned(),
                operation: MutationOperation::Restart,
                request_id: "11111111111111111111111111111111".to_owned(),
                command_revision: 1,
                disposition: crate::supervisor::MutationDisposition::Accepted,
            }))
            .unwrap();
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        exchange.await.unwrap();
        assert_eq!(permits.available_permits(), 1);
        let Response::Success {
            result: SuccessResult::Operation(operation),
        } = serde_json::from_slice(frame_body(&response)).unwrap()
        else {
            panic!("restart should return an operation response");
        };
        assert_eq!(operation.operation, "restart");
        assert_eq!(operation.command_revision, 1);
    }

    #[test]
    fn restart_client_validation_requires_exact_identity_disposition_and_next_revision() {
        let valid = ReceivedOperation {
            server_id: "survival".to_owned(),
            operation: "restart".to_owned(),
            request_id: "11111111111111111111111111111111".to_owned(),
            command_revision: 13,
            disposition: "accepted".to_owned(),
        };
        assert_eq!(
            validate_mutation_success(
                valid,
                "survival",
                "11111111111111111111111111111111",
                "restart",
                Some(13),
            )
            .unwrap()
            .operation(),
            "restart"
        );

        for corrupt in [
            "server_id",
            "operation",
            "request_id",
            "revision",
            "disposition",
        ] {
            let mut invalid = ReceivedOperation {
                server_id: "survival".to_owned(),
                operation: "restart".to_owned(),
                request_id: "11111111111111111111111111111111".to_owned(),
                command_revision: 13,
                disposition: "accepted".to_owned(),
            };
            match corrupt {
                "server_id" => invalid.server_id = "creative".to_owned(),
                "operation" => invalid.operation = "stop".to_owned(),
                "request_id" => invalid.request_id = "22222222222222222222222222222222".to_owned(),
                "revision" => invalid.command_revision = 12,
                "disposition" => invalid.disposition = "already_satisfied".to_owned(),
                _ => unreachable!(),
            }
            assert!(matches!(
                validate_mutation_success(
                    invalid,
                    "survival",
                    "11111111111111111111111111111111",
                    "restart",
                    Some(13),
                ),
                Err(ClientError::SubmissionUncertain)
            ));
        }
    }

    #[tokio::test]
    async fn peer_eof_cancels_an_enqueued_pending_mutation() {
        let prepared = PreparedRegistry::new([&"survival".to_owned()].into_iter()).unwrap();
        let (handle, _authority, mut mutations) = RuntimeStatusHandle::test_with_mutations(
            "survival",
            LifecycleState::test_phase(ServerPhase::Stopped),
        );
        let registry = prepared
            .activate(BTreeMap::from([("survival".to_owned(), handle)]))
            .unwrap();
        let permits = Arc::new(Semaphore::new(1));
        let permit = Arc::clone(&permits).try_acquire_owned().unwrap();
        let (mut client, server) = tokio::io::duplex(1_024);
        let task = tokio::spawn(serve_accepted(server, registry, permit));
        let request = Request::Start {
            server_id: "survival".to_owned(),
            request_id: RequestId::new("11111111111111111111111111111111".to_owned()).unwrap(),
            expected_command_revision: 0,
        };
        client
            .write_all(&encode_frame(&request).unwrap())
            .await
            .unwrap();
        client.shutdown().await.unwrap();
        let envelope = mutations.recv().await.unwrap();
        task.await.unwrap();
        assert!(envelope.admission.is_cancelled());
        assert!(!envelope.admission.is_admitted());
    }

    #[tokio::test]
    async fn thirty_third_mutation_is_rejected_without_waiting() {
        let prepared = PreparedRegistry::new([&"survival".to_owned()].into_iter()).unwrap();
        let (handle, _authority, _mutations) = RuntimeStatusHandle::test_with_mutations(
            "survival",
            LifecycleState::test_phase(ServerPhase::Stopped),
        );
        let sender = handle.mutation_sender();
        for index in 0..32 {
            let permit = sender.clone().try_reserve_owned().unwrap();
            let (reply, _receiver) = oneshot::channel();
            permit.send(MutationEnvelope {
                operation: MutationOperation::Start,
                request_id: format!("{index:032x}"),
                expected_command_revision: 0,
                reply,
                admission: Arc::new(AdmissionGate::pending()),
            });
        }
        let registry = prepared
            .activate(BTreeMap::from([("survival".to_owned(), handle)]))
            .unwrap();
        let request = Request::Stop {
            server_id: "survival".to_owned(),
            request_id: RequestId::new("ffffffffffffffffffffffffffffffff".to_owned()).unwrap(),
            expected_command_revision: 0,
        };
        let response = exchange(&encode_frame(&request).unwrap(), registry).await;
        assert_eq!(response_code(&response), WireErrorCode::CommandQueueFull);
    }

    #[tokio::test]
    async fn truncated_headers_and_bodies_are_malformed() {
        for request in [
            vec![0, 0, 0],
            [10_u32.to_be_bytes().as_slice(), &[0, 2], b"{}"].concat(),
        ] {
            let (registry, _authority) =
                one_server_registry(LifecycleState::test_phase(ServerPhase::Stopped));
            let response = exchange(&request, registry).await;
            assert!(matches!(
                response_code(&response),
                WireErrorCode::MalformedRequest
            ));
        }
    }

    #[tokio::test]
    async fn version_two_succeeds_and_other_versions_skip_json() {
        let (registry, _authority) =
            one_server_registry(LifecycleState::test_phase(ServerPhase::Stopped));
        let response = exchange(&raw_frame(2, br#"{"type":"list"}"#), registry).await;
        assert!(matches!(
            serde_json::from_slice::<Response>(frame_body(&response)).unwrap(),
            Response::Success { .. }
        ));

        for version in [0, 1, 3] {
            let (registry, _authority) =
                one_server_registry(LifecycleState::test_phase(ServerPhase::Stopped));
            let response = exchange(&raw_frame(version, &[0xff, 0xff]), registry).await;
            assert!(matches!(
                response_code(&response),
                WireErrorCode::UnsupportedProtocolVersion
            ));
        }
    }

    #[tokio::test]
    async fn incompatible_response_version_is_observed_without_reading_json() {
        let (mut writer, mut reader) = tokio::io::duplex(16);
        writer.write_all(&65_536_u32.to_be_bytes()).await.unwrap();
        writer.write_all(&1_u16.to_be_bytes()).await.unwrap();
        let ReadFrame::Version { version, body } = read_frame(&mut reader).await.unwrap();
        assert_eq!(version, 1);
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn client_classifies_protocol_unknown_and_malformed_daemon_responses() {
        assert!(matches!(
            client_receives(raw_frame(1, &[0xff])).await,
            Err(ClientError::ProtocolMismatch)
        ));
        assert!(matches!(
            client_receives(error_frame(
                WireErrorCode::UnknownServerId,
                "diagnostic text is not contractual"
            ))
            .await,
            Err(ClientError::UnknownServerId)
        ));
        for malformed in [
            Vec::new(),
            raw_frame(2, b"not json"),
            raw_frame(2, br#"{"type":"success","result":{"type":"list"}}"#),
            raw_frame(
                2,
                br#"{"type":"success","result":{"type":"list","servers":[],"extra":1}}"#,
            ),
        ] {
            assert!(matches!(
                client_receives(malformed).await,
                Err(ClientError::DaemonUnavailable)
            ));
        }
    }

    #[tokio::test]
    async fn client_maps_every_protocol_v2_error_code() {
        for code in WireErrorCode::ALL {
            let Err(error) = client_receives(constant_error_frame(code)).await else {
                panic!("wire errors must not be successful client results");
            };
            match code {
                WireErrorCode::MalformedRequest => {
                    assert!(matches!(error, ClientError::DaemonUnavailable));
                }
                WireErrorCode::UnsupportedProtocolVersion => {
                    assert!(matches!(error, ClientError::ProtocolMismatch));
                }
                WireErrorCode::UnknownServerId => {
                    assert!(matches!(error, ClientError::UnknownServerId));
                }
                WireErrorCode::UnsupportedOperation
                | WireErrorCode::OperationRejected
                | WireErrorCode::ExternalBackendNotOwned
                | WireErrorCode::ConflictingBackendEvidence
                | WireErrorCode::OperationInProgress
                | WireErrorCode::StaleCommandRevision
                | WireErrorCode::RequestIdConflict
                | WireErrorCode::CommandQueueFull
                | WireErrorCode::InternalFailure => {
                    assert!(matches!(error, ClientError::Operation(_)));
                }
            }
        }
    }

    #[test]
    fn registry_list_is_sorted_and_preencoded_bytes_are_shared() {
        let ids = ["survival".to_owned(), "creative".to_owned()];
        let prepared = PreparedRegistry::new(ids.iter()).unwrap();
        let (creative, _creative_authority) =
            RuntimeStatusHandle::test("creative", LifecycleState::test_phase(ServerPhase::Stopped));
        let (survival, _survival_authority) =
            RuntimeStatusHandle::test("survival", LifecycleState::test_phase(ServerPhase::Stopped));
        let registry = prepared
            .activate(BTreeMap::from([
                ("creative".to_owned(), creative),
                ("survival".to_owned(), survival),
            ]))
            .unwrap();
        let clone = registry.clone();
        assert!(Arc::ptr_eq(
            &registry.0.list_response,
            &clone.0.list_response
        ));
        let Response::Success {
            result: SuccessResult::List { servers },
        } = serde_json::from_slice(frame_body(&registry.0.list_response)).unwrap()
        else {
            panic!("expected list response");
        };
        assert_eq!(servers, ["creative", "survival"]);
    }

    #[test]
    fn preflight_rejects_oversized_list_and_worst_case_status() {
        let list_ids = [
            format!("a{}", "x".repeat(33_000)),
            format!("b{}", "x".repeat(33_000)),
        ];
        let list_error = PreparedRegistry::new(list_ids.iter())
            .err()
            .unwrap()
            .to_string();
        assert!(list_error.contains("control protocol v2 response limit"));
        assert!(list_error.contains("complete server list"));

        let status_id = format!("a{}", "x".repeat(65_350));
        let status_error = PreparedRegistry::new([&status_id].into_iter())
            .err()
            .unwrap()
            .to_string();
        assert!(status_error.contains("control protocol v2 response limit"));
        assert!(status_error.contains("worst-case server status"));
    }

    #[test]
    fn lifecycle_phase_and_failure_mappings_are_exact() {
        let phases = [
            (ServerPhase::Reconciling, "reconciling"),
            (ServerPhase::Stopped, "stopped"),
            (ServerPhase::Starting, "starting"),
            (ServerPhase::Running, "running"),
            (ServerPhase::Stopping, "stopping"),
            (ServerPhase::Cooldown, "cooldown"),
            (ServerPhase::External, "external"),
            (ServerPhase::Conflict, "conflict"),
        ];
        for (internal, wire) in phases {
            assert_eq!(WirePhase::from(internal).as_str(), wire);
        }

        let failures = [
            (FailureCategory::LaunchFailed, "launch_failed"),
            (FailureCategory::StartupTimedOut, "startup_timed_out"),
            (FailureCategory::ExitedBeforeReady, "exited_before_ready"),
            (FailureCategory::ExitedUnexpectedly, "exited_unexpectedly"),
        ];
        for (internal, wire) in failures {
            assert_eq!(WireFailureCategory::from(internal).as_str(), wire);
        }
    }

    #[test]
    fn status_always_contains_exact_allowlisted_fields_and_explicit_nulls() {
        let snapshot = LifecycleState::test_phase(ServerPhase::Stopped).status_snapshot();
        let frame = status_frame("survival", snapshot, Instant::now());
        let value: Value = serde_json::from_slice(frame_body(&frame)).unwrap();
        let server = value["result"]["server"].as_object().unwrap();
        let keys = server.keys().map(String::as_str).collect::<Vec<_>>();
        assert_eq!(
            keys,
            [
                "command_revision",
                "failure_category",
                "failure_streak",
                "phase",
                "retry_after_ms",
                "server_id"
            ]
        );
        assert!(server["failure_category"].is_null());
        assert!(server["retry_after_ms"].is_null());
    }

    #[test]
    fn retry_milliseconds_ceil_and_expire_at_zero() {
        let now = Instant::now();
        assert_eq!(retry_milliseconds(now, now), 0);
        assert_eq!(retry_milliseconds(now - Duration::from_secs(1), now), 0);
        assert_eq!(retry_milliseconds(now + Duration::from_nanos(1), now), 1);
        assert_eq!(retry_milliseconds(now + Duration::from_millis(1), now), 1);
        assert_eq!(
            retry_milliseconds(
                now + Duration::from_millis(1) + Duration::from_nanos(1),
                now
            ),
            2
        );
    }

    #[tokio::test]
    async fn known_and_unknown_server_ids_have_contractual_results() {
        let request = encode_frame(&Request::Status {
            server_id: "survival".to_owned(),
        })
        .unwrap();
        let (registry, _authority) =
            one_server_registry(LifecycleState::test_phase(ServerPhase::Running));
        let response = exchange(&request, registry).await;
        assert!(matches!(
            serde_json::from_slice::<Response>(frame_body(&response)).unwrap(),
            Response::Success {
                result: SuccessResult::Status { .. }
            }
        ));

        let request = encode_frame(&Request::Status {
            server_id: "missing".to_owned(),
        })
        .unwrap();
        let (registry, _authority) =
            one_server_registry(LifecycleState::test_phase(ServerPhase::Running));
        let response = exchange(&request, registry).await;
        assert!(matches!(
            response_code(&response),
            WireErrorCode::UnknownServerId
        ));
    }

    #[tokio::test]
    async fn closed_lifecycle_authority_closes_without_a_mixed_response() {
        let (registry, authority) =
            one_server_registry(LifecycleState::test_phase(ServerPhase::Running));
        drop(authority);
        let request = encode_frame(&Request::Status {
            server_id: "survival".to_owned(),
        })
        .unwrap();
        assert!(exchange(&request, registry).await.is_empty());
    }

    #[test]
    fn snapshot_races_never_mix_fields_from_two_watch_values() {
        let first = LifecycleState::test_status(ServerPhase::Stopped, None, 0, None);
        let second = LifecycleState::test_status(
            ServerPhase::Cooldown,
            Some(FailureCategory::LaunchFailed),
            2,
            Some(Instant::now() + Duration::from_secs(10)),
        );
        let (handle, authority) = RuntimeStatusHandle::test("survival", first);
        for index in 0..1_000 {
            authority.send_replace(if index % 2 == 0 { second } else { first });
            let snapshot = handle.snapshot().unwrap();
            assert!(
                (snapshot.phase == ServerPhase::Stopped
                    && snapshot.failure.is_none()
                    && snapshot.failure_streak == 0
                    && snapshot.retry_at.is_none())
                    || (snapshot.phase == ServerPhase::Cooldown
                        && snapshot.failure == Some(FailureCategory::LaunchFailed)
                        && snapshot.failure_streak == 2
                        && snapshot.retry_at.is_some())
            );
        }
    }

    #[test]
    fn encoded_responses_exclude_unallowlisted_runtime_details() {
        let sentinels = [
            "sentinel-password",
            "sentinel-argument",
            "sentinel-working-directory",
            "127.0.0.1:25566",
            "127.0.0.1:25575",
        ];
        let snapshot = LifecycleState::test_status(
            ServerPhase::Cooldown,
            Some(FailureCategory::LaunchFailed),
            2,
            Some(Instant::now() + Duration::from_secs(10)),
        )
        .status_snapshot();
        let encoded = status_frame("survival", snapshot, Instant::now());
        let text = String::from_utf8(frame_body(&encoded).to_vec()).unwrap();
        for sentinel in sentinels {
            assert!(!text.contains(sentinel));
        }
        for forbidden_field in [
            "ownership",
            "backend_authority",
            "player_count",
            "process_id",
            "launch_generation",
            "phase_started_at",
        ] {
            assert!(!text.contains(forbidden_field));
        }
    }

    async fn deadline_case(
        registry: Registry,
        initial_write: &[u8],
        capacity: usize,
    ) -> Arc<Semaphore> {
        let permits = Arc::new(Semaphore::new(1));
        let permit = Arc::clone(&permits).try_acquire_owned().unwrap();
        let (mut client, server) = tokio::io::duplex(capacity);
        let task = tokio::spawn(serve_accepted(server, registry, permit));
        if !initial_write.is_empty() {
            client.write_all(initial_write).await.unwrap();
        }
        tokio::task::yield_now().await;
        advance(EXCHANGE_TIMEOUT).await;
        task.await.unwrap();
        permits
    }

    #[tokio::test(start_paused = true)]
    async fn one_absolute_deadline_covers_slow_header_body_and_write() {
        let (registry, _authority) =
            one_server_registry(LifecycleState::test_phase(ServerPhase::Stopped));
        assert_eq!(
            deadline_case(registry, &[], 64).await.available_permits(),
            1
        );

        let (registry, _authority) =
            one_server_registry(LifecycleState::test_phase(ServerPhase::Stopped));
        let partial_body = [100_u32.to_be_bytes().as_slice(), &[0, 2], b"{"].concat();
        assert_eq!(
            deadline_case(registry, &partial_body, 64)
                .await
                .available_permits(),
            1
        );

        let ids = (0..100)
            .map(|index| format!("server-{index:03}"))
            .collect::<Vec<_>>();
        let prepared = PreparedRegistry::new(ids.iter()).unwrap();
        let mut handles = BTreeMap::new();
        let mut authorities = Vec::new();
        for id in &ids {
            let (handle, authority) =
                RuntimeStatusHandle::test(id, LifecycleState::test_phase(ServerPhase::Stopped));
            handles.insert(id.clone(), handle);
            authorities.push(authority);
        }
        let registry = prepared.activate(handles).unwrap();
        let request = encode_frame(&Request::List).unwrap();
        assert_eq!(
            deadline_case(registry, &request, 64)
                .await
                .available_permits(),
            1
        );
        drop(authorities);
    }

    #[tokio::test]
    async fn cancellation_restores_the_control_permit() {
        let permits = Arc::new(Semaphore::new(1));
        let permit = Arc::clone(&permits).try_acquire_owned().unwrap();
        let (_client, server): (DuplexStream, DuplexStream) = tokio::io::duplex(64);
        let (registry, _authority) =
            one_server_registry(LifecycleState::test_phase(ServerPhase::Stopped));
        let task = tokio::spawn(serve_accepted(server, registry, permit));
        tokio::task::yield_now().await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(permits.available_permits(), 1);
    }

    #[test]
    fn thirty_two_permits_are_the_only_control_exchange_capacity() {
        let permits = Arc::new(Semaphore::new(MAX_CONTROL_EXCHANGES));
        let held = (0..MAX_CONTROL_EXCHANGES)
            .map(|_| Arc::clone(&permits).try_acquire_owned().unwrap())
            .collect::<Vec<_>>();
        assert!(Arc::clone(&permits).try_acquire_owned().is_err());
        drop(held);
        assert_eq!(permits.available_permits(), MAX_CONTROL_EXCHANGES);
    }
}
