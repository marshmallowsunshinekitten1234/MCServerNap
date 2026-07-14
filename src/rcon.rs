use std::io::ErrorKind;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail, ensure};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Instant, timeout_at};

use crate::endpoint::Endpoint;

const AUTH: i32 = 3;
const AUTH_RESPONSE: i32 = 2;
const EXEC_COMMAND: i32 = 2;
const RESPONSE_VALUE: i32 = 0;
const MAX_COMMAND_BYTES: usize = 1_413;
const MAX_PACKET_BYTES: usize = 4 * 1024 * 1024;
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;

/// Minimal asynchronous client for Minecraft's Source RCON transport.
pub struct RconClient {
    stream: TcpStream,
    next_request_id: i32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RconProbeClassification {
    Authenticated,
    Refused,
    AcceptedFailure,
    Inconclusive,
}

#[derive(Debug)]
pub(crate) struct RconProbeResult {
    pub(crate) classification: RconProbeClassification,
    pub(crate) detail: Option<anyhow::Error>,
}

impl RconClient {
    pub async fn connect(endpoint: &Endpoint, password: &str) -> Result<Self> {
        validate_body(password, "RCON password")?;
        let stream = TcpStream::connect((endpoint.host(), endpoint.port()))
            .await
            .with_context(|| format!("failed to connect to RCON at {endpoint}"))?;
        Self::authenticate(stream, password).await
    }

    async fn authenticate(stream: TcpStream, password: &str) -> Result<Self> {
        stream
            .set_nodelay(true)
            .context("failed to enable TCP_NODELAY for RCON")?;

        let mut client = Self {
            stream,
            next_request_id: 1,
        };
        let authentication_id = client.send_packet(AUTH, password).await?;

        loop {
            let packet = client.read_packet().await?;
            if packet.kind != AUTH_RESPONSE {
                continue;
            }
            if packet.id == -1 {
                bail!("RCON authentication failed");
            }
            ensure!(
                packet.id == authentication_id,
                "RCON authentication response used an unexpected request ID"
            );
            break;
        }

        Ok(client)
    }

    /// Execute a command and collect all Minecraft response packets.
    pub async fn command(&mut self, command: &str) -> Result<String> {
        validate_body(command, "RCON command")?;
        let command_id = self.send_packet(EXEC_COMMAND, command).await?;

        // Minecraft can mishandle immediately adjacent RCON requests (MC-72390).
        tokio::time::sleep(std::time::Duration::from_millis(3)).await;
        let end_marker_id = self.send_packet(EXEC_COMMAND, "").await?;

        let mut response = Vec::new();
        loop {
            let packet = self.read_packet().await?;
            if packet.id == end_marker_id {
                return String::from_utf8(response).context("RCON response is not valid UTF-8");
            }
            ensure!(
                packet.id == command_id && packet.kind == RESPONSE_VALUE,
                "RCON returned an unexpected response packet"
            );
            ensure!(
                response.len() + packet.body.len() <= MAX_RESPONSE_BYTES,
                "RCON response exceeds the {MAX_RESPONSE_BYTES}-byte limit"
            );
            response.extend_from_slice(&packet.body);
        }
    }

    /// Send Minecraft's `stop` command without waiting for a response that may never arrive.
    pub async fn stop(&mut self) -> Result<()> {
        self.send_packet(EXEC_COMMAND, "stop").await?;
        Ok(())
    }

    async fn send_packet(&mut self, kind: i32, body: &str) -> Result<i32> {
        let id = self.next_request_id;
        self.next_request_id = if id == i32::MAX { 1 } else { id + 1 };

        let packet = encode_packet(id, kind, body.as_bytes())?;
        self.stream
            .write_all(&packet)
            .await
            .context("failed to write an RCON packet")?;
        Ok(id)
    }

    async fn read_packet(&mut self) -> Result<Packet> {
        let length = self
            .stream
            .read_i32_le()
            .await
            .context("failed to read the RCON packet length")?;
        ensure!(
            length >= 10,
            "RCON packet length is below the protocol minimum"
        );
        let length = usize::try_from(length).context("RCON packet length is negative")?;
        ensure!(
            length <= MAX_PACKET_BYTES,
            "RCON packet exceeds the {MAX_PACKET_BYTES}-byte limit"
        );

        let mut payload = vec![0; length];
        self.stream
            .read_exact(&mut payload)
            .await
            .context("RCON connection closed during a packet")?;
        decode_payload(&payload)
    }
}

pub(crate) async fn probe(
    endpoint: &Endpoint,
    password: &str,
    command_timeout: Duration,
) -> RconProbeResult {
    if let Err(error) = validate_body(password, "RCON password") {
        return RconProbeResult {
            classification: RconProbeClassification::Inconclusive,
            detail: Some(error),
        };
    }

    let deadline = Instant::now() + command_timeout;
    let stream = match timeout_at(
        deadline,
        TcpStream::connect((endpoint.host(), endpoint.port())),
    )
    .await
    {
        Ok(Ok(stream)) => stream,
        Ok(Err(error)) if error.kind() == ErrorKind::ConnectionRefused => {
            return RconProbeResult {
                classification: RconProbeClassification::Refused,
                detail: Some(error.into()),
            };
        }
        Ok(Err(error)) => {
            return RconProbeResult {
                classification: RconProbeClassification::Inconclusive,
                detail: Some(error.into()),
            };
        }
        Err(_) => {
            return RconProbeResult {
                classification: RconProbeClassification::Inconclusive,
                detail: Some(anyhow!("RCON TCP connection timed out")),
            };
        }
    };

    match timeout_at(deadline, RconClient::authenticate(stream, password)).await {
        Ok(Ok(_)) => RconProbeResult {
            classification: RconProbeClassification::Authenticated,
            detail: None,
        },
        Ok(Err(error)) => RconProbeResult {
            classification: RconProbeClassification::AcceptedFailure,
            detail: Some(error),
        },
        Err(_) => RconProbeResult {
            classification: RconProbeClassification::AcceptedFailure,
            detail: Some(anyhow!(
                "RCON authentication did not finish before the command deadline"
            )),
        },
    }
}

struct Packet {
    id: i32,
    kind: i32,
    body: Vec<u8>,
}

fn validate_body(value: &str, description: &str) -> Result<()> {
    ensure!(
        value.len() <= MAX_COMMAND_BYTES,
        "{description} exceeds the {MAX_COMMAND_BYTES}-byte Minecraft limit"
    );
    ensure!(
        !value.as_bytes().contains(&0),
        "{description} contains a NUL byte"
    );
    Ok(())
}

fn encode_packet(id: i32, kind: i32, body: &[u8]) -> Result<Vec<u8>> {
    let payload_length = body
        .len()
        .checked_add(10)
        .context("RCON packet length overflow")?;
    let payload_length_i32 = i32::try_from(payload_length).context("RCON packet is too long")?;
    let mut packet = Vec::with_capacity(payload_length + 4);
    packet.extend_from_slice(&payload_length_i32.to_le_bytes());
    packet.extend_from_slice(&id.to_le_bytes());
    packet.extend_from_slice(&kind.to_le_bytes());
    packet.extend_from_slice(body);
    packet.extend_from_slice(&[0, 0]);
    Ok(packet)
}

fn decode_payload(payload: &[u8]) -> Result<Packet> {
    ensure!(payload.len() >= 10, "RCON packet is truncated");
    ensure!(
        payload[payload.len() - 2..] == [0, 0],
        "RCON packet has invalid terminators"
    );
    let id = i32::from_le_bytes(payload[0..4].try_into().expect("packet length was checked"));
    let kind = i32::from_le_bytes(payload[4..8].try_into().expect("packet length was checked"));
    Ok(Packet {
        id,
        kind,
        body: payload[8..payload.len() - 2].to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn endpoint(address: std::net::SocketAddr) -> Endpoint {
        Endpoint::new(address.ip().to_string(), address.port())
    }

    async fn receive_test_packet(stream: &mut TcpStream) -> Packet {
        let length = stream
            .read_i32_le()
            .await
            .expect("test packet length should arrive");
        let mut payload = vec![0; usize::try_from(length).expect("positive test packet length")];
        stream
            .read_exact(&mut payload)
            .await
            .expect("test packet payload should arrive");
        decode_payload(&payload).expect("test packet should decode")
    }

    async fn send_test_packet(stream: &mut TcpStream, id: i32, kind: i32, body: &[u8]) {
        let packet = encode_packet(id, kind, body).expect("test packet should encode");
        stream
            .write_all(&packet)
            .await
            .expect("test packet should write");
    }

    #[test]
    fn packet_encoding_matches_source_rcon_layout() {
        let encoded = encode_packet(42, EXEC_COMMAND, b"list").expect("packet should encode");
        assert_eq!(i32::from_le_bytes(encoded[0..4].try_into().unwrap()), 14);

        let decoded = decode_payload(&encoded[4..]).expect("payload should decode");
        assert_eq!(decoded.id, 42);
        assert_eq!(decoded.kind, EXEC_COMMAND);
        assert_eq!(decoded.body, b"list");
    }

    #[test]
    fn rejects_invalid_packet_terminators() {
        let mut encoded = encode_packet(1, RESPONSE_VALUE, b"ok").expect("packet should encode");
        *encoded.last_mut().expect("packet is nonempty") = 1;
        assert!(decode_payload(&encoded[4..]).is_err());
    }

    #[test]
    fn rejects_embedded_nul_commands() {
        assert!(validate_body("say hi\0stop", "command").is_err());
    }

    #[tokio::test]
    async fn authenticates_collects_multipart_responses_and_sends_stop() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test RCON listener should bind");
        let address = listener.local_addr().expect("listener has an address");

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("RCON client should connect");
            let auth = receive_test_packet(&mut stream).await;
            assert_eq!(auth.kind, AUTH);
            assert_eq!(auth.body, b"secret");

            // Minecraft may emit a response-value packet before its auth response.
            send_test_packet(&mut stream, auth.id, RESPONSE_VALUE, b"").await;
            send_test_packet(&mut stream, auth.id, AUTH_RESPONSE, b"").await;

            let command = receive_test_packet(&mut stream).await;
            assert_eq!(command.kind, EXEC_COMMAND);
            assert_eq!(command.body, b"list");
            let end_marker = receive_test_packet(&mut stream).await;
            assert_eq!(end_marker.kind, EXEC_COMMAND);
            assert!(end_marker.body.is_empty());

            send_test_packet(&mut stream, command.id, RESPONSE_VALUE, b"There are ").await;
            send_test_packet(
                &mut stream,
                command.id,
                RESPONSE_VALUE,
                b"2 of a max of 20 players online: a, b",
            )
            .await;
            send_test_packet(&mut stream, end_marker.id, RESPONSE_VALUE, b"").await;

            let stop = receive_test_packet(&mut stream).await;
            assert_eq!(stop.kind, EXEC_COMMAND);
            assert_eq!(stop.body, b"stop");
        });

        let mut client = RconClient::connect(&endpoint(address), "secret")
            .await
            .expect("RCON authentication should succeed");
        let response = client
            .command("list")
            .await
            .expect("multipart response should be collected");
        assert_eq!(response, "There are 2 of a max of 20 players online: a, b");
        client.stop().await.expect("stop command should be sent");
        server.await.expect("test RCON server should not panic");
    }

    #[tokio::test(start_paused = true)]
    async fn probe_retains_tcp_acceptance_when_authentication_reaches_the_deadline() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test RCON listener should bind");
        let address = listener.local_addr().expect("listener has an address");
        let (accepted_sender, accepted_receiver) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("probe should connect");
            let auth = receive_test_packet(&mut stream).await;
            assert_eq!(auth.kind, AUTH);
            accepted_sender
                .send(())
                .expect("test should await TCP acceptance");
            std::future::pending::<()>().await;
        });
        let endpoint = endpoint(address);
        let probing =
            tokio::spawn(async move { probe(&endpoint, "secret", Duration::from_secs(5)).await });

        accepted_receiver
            .await
            .expect("probe should reach authentication");
        tokio::time::advance(Duration::from_secs(5)).await;
        let result = probing.await.expect("probe task should not panic");
        assert_eq!(
            result.classification,
            RconProbeClassification::AcceptedFailure
        );
        assert!(
            result
                .detail
                .expect("timeout detail should be retained")
                .to_string()
                .contains("authentication")
        );
        server.abort();
    }

    #[tokio::test]
    async fn probe_classifies_authentication_failure_after_tcp_acceptance() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test RCON listener should bind");
        let address = listener.local_addr().expect("listener has an address");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("probe should connect");
            let _auth = receive_test_packet(&mut stream).await;
            send_test_packet(&mut stream, -1, AUTH_RESPONSE, b"").await;
        });

        let result = probe(&endpoint(address), "secret", Duration::from_secs(1)).await;
        assert_eq!(
            result.classification,
            RconProbeClassification::AcceptedFailure
        );
        assert!(
            result
                .detail
                .expect("authentication failure detail should be retained")
                .to_string()
                .contains("authentication failed")
        );
        server.await.expect("test RCON server should not panic");
    }

    #[tokio::test]
    async fn probe_classifies_protocol_failure_and_eof_after_tcp_acceptance() {
        for response in [Some(5_i32.to_le_bytes()), None] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("test RCON listener should bind");
            let address = listener.local_addr().expect("listener has an address");
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.expect("probe should connect");
                let _auth = receive_test_packet(&mut stream).await;
                if let Some(bytes) = response {
                    stream
                        .write_all(&bytes)
                        .await
                        .expect("malformed packet length should write");
                }
            });

            let result = probe(&endpoint(address), "secret", Duration::from_secs(1)).await;
            assert_eq!(
                result.classification,
                RconProbeClassification::AcceptedFailure
            );
            assert!(result.detail.is_some());
            server.await.expect("test RCON server should not panic");
        }
    }
}
