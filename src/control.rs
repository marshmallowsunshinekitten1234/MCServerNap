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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClientMutationOperation {
    Start,
    Stop,
    Restart,
}

impl ClientMutationOperation {
    const ALL: [Self; 3] = [Self::Start, Self::Stop, Self::Restart];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::Stop => "stop",
            Self::Restart => "restart",
        }
    }

    fn request(
        self,
        server_id: String,
        request_id: RequestId,
        expected_command_revision: u64,
    ) -> Request {
        match self {
            Self::Start => Request::Start {
                server_id,
                request_id,
                expected_command_revision,
            },
            Self::Stop => Request::Stop {
                server_id,
                request_id,
                expected_command_revision,
            },
            Self::Restart => Request::Restart {
                server_id,
                request_id,
                expected_command_revision,
            },
        }
    }

    fn accepts_disposition(self, disposition: &str) -> bool {
        match self {
            Self::Start | Self::Stop => matches!(disposition, "accepted" | "already_satisfied"),
            Self::Restart => disposition == "accepted",
        }
    }

    fn from_wire_name(value: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|operation| operation.as_str() == value)
    }

    pub async fn submit(
        self,
        server_id: String,
        request_id: String,
        expected_command_revision: u64,
    ) -> std::result::Result<OperationStatus, ClientError> {
        let request_id = RequestId::new(request_id)
            .ok_or_else(|| ClientError::Operation("invalid request ID".to_owned()))?;
        let request = self.request(
            server_id.clone(),
            request_id.clone(),
            expected_command_revision,
        );
        mutate(
            request,
            server_id,
            request_id.into_string(),
            self,
            expected_command_revision.checked_add(1),
        )
        .await
    }
}

impl<'de> Deserialize<'de> for ClientMutationOperation {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::from_wire_name(&value)
            .ok_or_else(|| serde::de::Error::custom("unknown mutation operation"))
    }
}

impl fmt::Display for ClientMutationOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
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
    operation: ClientMutationOperation,
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
    operation: ClientMutationOperation,
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
        self.operation.as_str()
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
        for operation in ClientMutationOperation::ALL {
            let worst_case = SuccessEnvelope {
                response_type: "success",
                result: OperationResult {
                    result_type: "operation",
                    server_id: longest_id,
                    operation: operation.as_str(),
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

async fn mutate(
    request: Request,
    expected_server_id: String,
    expected_request_id: String,
    expected_operation: ClientMutationOperation,
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
    expected_operation: ClientMutationOperation,
    expected_result_revision: Option<u64>,
) -> std::result::Result<OperationStatus, ClientError> {
    if operation.server_id != expected_server_id
        || operation.request_id != expected_request_id
        || operation.operation != expected_operation
        || Some(operation.command_revision) != expected_result_revision
        || !expected_operation.accepts_disposition(&operation.disposition)
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
mod tests;
