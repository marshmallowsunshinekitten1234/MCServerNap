use std::collections::BTreeMap;
use std::sync::Arc;

use serde_json::Value;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _, DuplexStream};
use tokio::sync::{Semaphore, watch};
use tokio::time::{Duration, Instant, advance};

use super::*;
use crate::runtime::RuntimeControlHandle;
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
    let (handle, sender) = RuntimeControlHandle::test("survival", lifecycle);
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

fn received_operation(
    operation: ClientMutationOperation,
    revision: u64,
    disposition: &str,
) -> ReceivedOperation {
    ReceivedOperation {
        server_id: "survival".to_owned(),
        operation,
        request_id: "11111111111111111111111111111111".to_owned(),
        command_revision: revision,
        disposition: disposition.to_owned(),
    }
}

fn validate_test_operation(
    operation: ReceivedOperation,
    expected_operation: ClientMutationOperation,
    expected_revision: Option<u64>,
) -> std::result::Result<OperationStatus, ClientError> {
    validate_mutation_success(
        operation,
        "survival",
        "11111111111111111111111111111111",
        expected_operation,
        expected_revision,
    )
}

#[test]
fn start_and_stop_accept_only_their_two_valid_dispositions() {
    for operation in [
        ClientMutationOperation::Start,
        ClientMutationOperation::Stop,
    ] {
        for disposition in ["accepted", "already_satisfied"] {
            let status = validate_test_operation(
                received_operation(operation, 13, disposition),
                operation,
                Some(13),
            )
            .expect("start and stop should accept both contractual dispositions");
            assert_eq!(status.operation(), operation.as_str());
            assert_eq!(status.command_revision(), 13);
            assert_eq!(status.disposition(), disposition);
        }
        assert!(matches!(
            validate_test_operation(
                received_operation(operation, 13, "unknown"),
                operation,
                Some(13),
            ),
            Err(ClientError::SubmissionUncertain)
        ));
    }
}

#[test]
fn mutation_success_requires_the_exact_next_revision() {
    for revision in [11, 12, 14, u64::MAX] {
        assert!(matches!(
            validate_test_operation(
                received_operation(ClientMutationOperation::Start, revision, "accepted"),
                ClientMutationOperation::Start,
                Some(13),
            ),
            Err(ClientError::SubmissionUncertain)
        ));
    }
    assert!(matches!(
        validate_test_operation(
            received_operation(ClientMutationOperation::Start, u64::MAX, "accepted"),
            ClientMutationOperation::Start,
            None,
        ),
        Err(ClientError::SubmissionUncertain)
    ));
}

#[test]
fn mismatched_returned_operation_is_submission_uncertain() {
    let returned = received_operation(ClientMutationOperation::Stop, 13, "accepted");
    assert!(matches!(
        validate_test_operation(returned, ClientMutationOperation::Start, Some(13),),
        Err(ClientError::SubmissionUncertain)
    ));
}

#[test]
fn mutation_success_server_and_request_identity_remain_strict() {
    let mut cases = Vec::new();

    let mut wrong_server = received_operation(ClientMutationOperation::Start, 13, "accepted");
    wrong_server.server_id = "creative".to_owned();
    cases.push(wrong_server);

    let mut wrong_request = received_operation(ClientMutationOperation::Start, 13, "accepted");
    wrong_request.request_id = "22222222222222222222222222222222".to_owned();
    cases.push(wrong_request);

    for operation in cases {
        assert!(matches!(
            validate_test_operation(operation, ClientMutationOperation::Start, Some(13),),
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

    for (operation, body) in [
            (
                ClientMutationOperation::Start,
                br#"{"type":"start","server_id":"survival","request_id":"11111111111111111111111111111111","expected_command_revision":12}"#.as_slice(),
            ),
            (
                ClientMutationOperation::Stop,
                br#"{"type":"stop","server_id":"survival","request_id":"11111111111111111111111111111111","expected_command_revision":12}"#.as_slice(),
            ),
            (
                ClientMutationOperation::Restart,
                br#"{"type":"restart","server_id":"survival","request_id":"11111111111111111111111111111111","expected_command_revision":12}"#.as_slice(),
            ),
        ] {
            let request = operation.request(
                "survival".to_owned(),
                RequestId::new("11111111111111111111111111111111".to_owned()).unwrap(),
                12,
            );
            assert_eq!(
                encode_frame(&request).unwrap(),
                raw_frame(PROTOCOL_VERSION, body)
            );
        }
    assert!(WireErrorCode::ALL.contains(&WireErrorCode::UnsupportedOperation));
    assert_eq!(
        response_code(&constant_error_frame(WireErrorCode::UnsupportedOperation)),
        WireErrorCode::UnsupportedOperation
    );
}

#[tokio::test]
async fn exact_success_responses_decode_typed_mutation_operations() {
    for (name, expected) in [
        ("start", ClientMutationOperation::Start),
        ("stop", ClientMutationOperation::Stop),
        ("restart", ClientMutationOperation::Restart),
    ] {
        let body = format!(
            r#"{{"type":"success","result":{{"type":"operation","server_id":"survival","operation":"{name}","request_id":"11111111111111111111111111111111","command_revision":13,"disposition":"accepted"}}}}"#
        );
        let result = client_receives(raw_frame(PROTOCOL_VERSION, body.as_bytes()))
            .await
            .expect("exact operation success should decode");
        let SuccessResult::Operation(operation) = result else {
            panic!("expected an operation success");
        };
        assert_eq!(operation.operation, expected);
    }
}

#[tokio::test]
async fn unknown_success_operation_is_not_accepted_as_typed() {
    let response = raw_frame(
            PROTOCOL_VERSION,
            br#"{"type":"success","result":{"type":"operation","server_id":"survival","operation":"unknown","request_id":"11111111111111111111111111111111","command_revision":13,"disposition":"accepted"}}"#,
        );
    assert!(matches!(
        client_receives(response).await,
        Err(ClientError::DaemonUnavailable)
    ));
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
    let (handle, _authority, mut mutations) = RuntimeControlHandle::test_with_mutations(
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
    assert_eq!(operation.operation, ClientMutationOperation::Restart);
    assert_eq!(operation.command_revision, 1);
}

#[test]
fn restart_client_validation_requires_exact_identity_disposition_and_next_revision() {
    let valid = ReceivedOperation {
        server_id: "survival".to_owned(),
        operation: ClientMutationOperation::Restart,
        request_id: "11111111111111111111111111111111".to_owned(),
        command_revision: 13,
        disposition: "accepted".to_owned(),
    };
    assert_eq!(
        validate_mutation_success(
            valid,
            "survival",
            "11111111111111111111111111111111",
            ClientMutationOperation::Restart,
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
            operation: ClientMutationOperation::Restart,
            request_id: "11111111111111111111111111111111".to_owned(),
            command_revision: 13,
            disposition: "accepted".to_owned(),
        };
        match corrupt {
            "server_id" => invalid.server_id = "creative".to_owned(),
            "operation" => invalid.operation = ClientMutationOperation::Stop,
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
                ClientMutationOperation::Restart,
                Some(13),
            ),
            Err(ClientError::SubmissionUncertain)
        ));
    }
}

#[tokio::test]
async fn peer_eof_cancels_an_enqueued_pending_mutation() {
    let prepared = PreparedRegistry::new([&"survival".to_owned()].into_iter()).unwrap();
    let (handle, _authority, mut mutations) = RuntimeControlHandle::test_with_mutations(
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
    let (handle, _authority, _mutations) = RuntimeControlHandle::test_with_mutations(
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
        RuntimeControlHandle::test("creative", LifecycleState::test_phase(ServerPhase::Stopped));
    let (survival, _survival_authority) =
        RuntimeControlHandle::test("survival", LifecycleState::test_phase(ServerPhase::Stopped));
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
    let (handle, authority) = RuntimeControlHandle::test("survival", first);
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
            RuntimeControlHandle::test(id, LifecycleState::test_phase(ServerPhase::Stopped));
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
