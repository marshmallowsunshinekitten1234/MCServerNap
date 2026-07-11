use std::io::ErrorKind;
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use serde::Serialize;
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

use crate::config::Config;

const MAX_PACKET_LENGTH: usize = (1 << 21) - 1;
const MAX_HANDSHAKE_PACKET_LENGTH: usize = 1_024;
const MAX_LOGIN_START_PACKET_LENGTH: usize = 128;
const MAX_STATUS_PACKET_LENGTH: usize = 32;

pub const MINECRAFT_PROTOCOL_VERSION: i32 = 776;
pub const MINECRAFT_VERSION_NAME: &str = "26.2";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LoginIntent {
    Login,
    Transfer,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClientRequest {
    Status,
    Login { intent: LoginIntent },
    UnsupportedProtocol { protocol_version: i32 },
}

#[derive(Debug)]
pub struct MinecraftResponder {
    status_response: Vec<u8>,
    login_disconnect: Vec<u8>,
    incompatible_disconnect: Vec<u8>,
}

#[derive(Serialize)]
struct StatusResponse<'a> {
    version: StatusVersion,
    players: StatusPlayers,
    description: StatusDescription<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    favicon: Option<&'a str>,
}

#[derive(Serialize)]
struct StatusVersion {
    name: &'static str,
    protocol: i32,
}

#[derive(Serialize)]
struct StatusPlayers {
    max: u32,
    online: u32,
    sample: Vec<StatusPlayer>,
}

#[derive(Serialize)]
struct StatusPlayer {
    name: String,
    id: String,
}

#[derive(Serialize)]
struct StatusDescription<'a> {
    text: &'a str,
    color: &'a str,
    bold: bool,
}

impl MinecraftResponder {
    pub fn new(config: &Config, favicon: Option<&str>) -> Result<Self> {
        let status = StatusResponse {
            version: StatusVersion {
                name: MINECRAFT_VERSION_NAME,
                protocol: MINECRAFT_PROTOCOL_VERSION,
            },
            players: StatusPlayers {
                max: 0,
                online: 0,
                sample: Vec::new(),
            },
            description: StatusDescription {
                text: &config.motd_text,
                color: &config.motd_color,
                bold: config.motd_bold,
            },
            favicon,
        };
        let status_json =
            serde_json::to_string(&status).context("failed to serialize status response")?;
        let status_response = encode_string_packet(0, &status_json)?;

        let disconnect_component = json!({
            "text": config.connection_msg_text,
            "color": config.connection_msg_color,
            "bold": config.connection_msg_bold,
        });
        let disconnect_json = serde_json::to_string(&disconnect_component)
            .context("failed to serialize the login disconnect message")?;
        let login_disconnect = encode_string_packet(0, &disconnect_json)?;
        let incompatible_component = json!({
            "text": format!(
                "This server requires Minecraft Java {MINECRAFT_VERSION_NAME} (protocol {MINECRAFT_PROTOCOL_VERSION})."
            ),
            "color": "red",
            "bold": true,
        });
        let incompatible_json = serde_json::to_string(&incompatible_component)
            .context("failed to serialize the incompatible-version message")?;
        let incompatible_disconnect = encode_string_packet(0, &incompatible_json)?;

        Ok(Self {
            status_response,
            login_disconnect,
            incompatible_disconnect,
        })
    }

    pub async fn send_login_disconnect(
        &self,
        socket: &mut TcpStream,
        operation_timeout: Duration,
    ) -> Result<()> {
        write_all_with_timeout(socket, &self.login_disconnect, operation_timeout)
            .await
            .context("failed to send login disconnect")?;
        shutdown_with_timeout(socket, operation_timeout).await
    }

    pub async fn serve_status(
        &self,
        socket: &mut TcpStream,
        operation_timeout: Duration,
    ) -> Result<()> {
        write_all_with_timeout(socket, &self.status_response, operation_timeout)
            .await
            .context("failed to send status response")?;

        // Clients may close after the status response without sending the optional ping.
        let packet = match timeout(
            operation_timeout,
            read_packet(socket, MAX_STATUS_PACKET_LENGTH),
        )
        .await
        {
            Ok(Ok(packet)) => Some(packet),
            Ok(Err(error)) if is_normal_client_close(&error) => None,
            Ok(Err(error)) => return Err(error).context("failed to read status ping"),
            Err(_) => None,
        };

        if let Some(packet) = packet {
            let mut cursor = PacketCursor::new(&packet);
            ensure!(
                cursor.read_varint()? == 1,
                "expected status ping packet ID 1"
            );
            let timestamp = cursor.read_i64()?;
            cursor.ensure_finished()?;

            let pong = encode_packet(1, &timestamp.to_be_bytes())?;
            write_all_with_timeout(socket, &pong, operation_timeout)
                .await
                .context("failed to send status pong")?;
        }

        shutdown_with_timeout(socket, operation_timeout).await
    }

    pub async fn send_incompatible_disconnect(
        &self,
        socket: &mut TcpStream,
        operation_timeout: Duration,
    ) -> Result<()> {
        write_all_with_timeout(socket, &self.incompatible_disconnect, operation_timeout)
            .await
            .context("failed to send incompatible-version disconnect")?;
        shutdown_with_timeout(socket, operation_timeout).await
    }
}

fn is_normal_client_close(error: &anyhow::Error) -> bool {
    error.downcast_ref::<std::io::Error>().is_some_and(|error| {
        matches!(
            error.kind(),
            ErrorKind::UnexpectedEof
                | ErrorKind::BrokenPipe
                | ErrorKind::ConnectionAborted
                | ErrorKind::ConnectionReset
        )
    })
}

/// Read and validate the initial protocol exchange while the backend is asleep.
/// Login requests include a validated Login Start packet so a bare handshake does not wake the server.
pub async fn read_client_request(socket: &mut TcpStream) -> Result<ClientRequest> {
    let first = socket
        .read_u8()
        .await
        .context("failed to read the first client byte")?;

    let handshake_packet =
        read_packet_after_first_byte(socket, first, MAX_HANDSHAKE_PACKET_LENGTH).await?;
    let handshake = parse_handshake(&handshake_packet)?;

    let intent = match handshake.intent {
        HandshakeIntent::Status => {
            let request = read_packet(socket, MAX_STATUS_PACKET_LENGTH).await?;
            let mut cursor = PacketCursor::new(&request);
            ensure!(
                cursor.read_varint()? == 0,
                "expected status request packet ID 0"
            );
            cursor.ensure_finished()?;
            return Ok(ClientRequest::Status);
        }
        HandshakeIntent::Login => LoginIntent::Login,
        HandshakeIntent::Transfer => LoginIntent::Transfer,
    };

    if handshake.protocol_version != MINECRAFT_PROTOCOL_VERSION {
        return Ok(ClientRequest::UnsupportedProtocol {
            protocol_version: handshake.protocol_version,
        });
    }

    let login_start = read_packet(socket, MAX_LOGIN_START_PACKET_LENGTH).await?;
    parse_login_start(&login_start)?;
    Ok(ClientRequest::Login { intent })
}

fn parse_login_start(packet: &[u8]) -> Result<()> {
    let mut cursor = PacketCursor::new(packet);
    ensure!(
        cursor.read_varint()? == 0,
        "expected login start packet ID 0"
    );
    let username = cursor.read_string(16)?;
    ensure!(!username.is_empty(), "login username must not be empty");
    cursor.read_bytes(16)?;
    cursor.ensure_finished()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HandshakeIntent {
    Status,
    Login,
    Transfer,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Handshake {
    protocol_version: i32,
    intent: HandshakeIntent,
}

fn parse_handshake(packet: &[u8]) -> Result<Handshake> {
    let mut cursor = PacketCursor::new(packet);
    ensure!(cursor.read_varint()? == 0, "expected handshake packet ID 0");
    let protocol_version = cursor.read_varint()?;
    cursor.read_string(255)?;
    cursor.read_u16()?;
    let intent = match cursor.read_varint()? {
        1 => HandshakeIntent::Status,
        2 => HandshakeIntent::Login,
        3 => HandshakeIntent::Transfer,
        value => bail!("unsupported handshake intent {value}"),
    };
    cursor.ensure_finished()?;
    Ok(Handshake {
        protocol_version,
        intent,
    })
}

async fn read_packet(socket: &mut TcpStream, application_limit: usize) -> Result<Vec<u8>> {
    let first = socket
        .read_u8()
        .await
        .context("failed to read packet length")?;
    read_packet_after_first_byte(socket, first, application_limit).await
}

async fn read_packet_after_first_byte(
    socket: &mut TcpStream,
    first: u8,
    application_limit: usize,
) -> Result<Vec<u8>> {
    let mut byte = first;
    let mut length = 0usize;

    for position in 0..3 {
        length |= usize::from(byte & 0x7f) << (position * 7);
        if byte & 0x80 == 0 {
            ensure!(length <= MAX_PACKET_LENGTH, "packet exceeds protocol limit");
            ensure!(
                length <= application_limit,
                "packet length {length} exceeds the {application_limit}-byte limit"
            );
            let mut packet = vec![0; length];
            socket
                .read_exact(&mut packet)
                .await
                .context("client closed a partial packet")?;
            return Ok(packet);
        }
        if position < 2 {
            byte = socket
                .read_u8()
                .await
                .context("client closed a partial packet length")?;
        }
    }

    bail!("packet length VarInt exceeds three bytes")
}

fn encode_string_packet(packet_id: i32, value: &str) -> Result<Vec<u8>> {
    ensure!(
        value.encode_utf16().count() <= 32_767,
        "packet string exceeds 32767 UTF-16 code units"
    );
    let length = i32::try_from(value.len()).context("packet string is too long")?;
    let mut payload = Vec::with_capacity(varint_size(length) + value.len());
    write_varint(length, &mut payload);
    payload.extend_from_slice(value.as_bytes());
    encode_packet(packet_id, &payload)
}

fn encode_packet(packet_id: i32, payload: &[u8]) -> Result<Vec<u8>> {
    let body_length = varint_size(packet_id)
        .checked_add(payload.len())
        .context("packet length overflow")?;
    ensure!(
        body_length <= MAX_PACKET_LENGTH,
        "packet exceeds protocol limit"
    );
    let body_length_i32 = i32::try_from(body_length).context("packet is too long")?;

    let mut packet = Vec::with_capacity(varint_size(body_length_i32) + body_length);
    write_varint(body_length_i32, &mut packet);
    write_varint(packet_id, &mut packet);
    packet.extend_from_slice(payload);
    Ok(packet)
}

fn write_varint(value: i32, output: &mut Vec<u8>) {
    let mut remaining = value.cast_unsigned();
    loop {
        let mut byte = u8::try_from(remaining & 0x7f).expect("value is masked to seven bits");
        remaining >>= 7;
        if remaining != 0 {
            byte |= 0x80;
        }
        output.push(byte);
        if remaining == 0 {
            return;
        }
    }
}

const fn varint_size(value: i32) -> usize {
    let bits = value.cast_unsigned();
    match bits {
        0..=0x7f => 1,
        0x80..=0x3fff => 2,
        0x4000..=0x1f_ffff => 3,
        0x20_0000..=0x0fff_ffff => 4,
        _ => 5,
    }
}

async fn write_all_with_timeout(
    socket: &mut TcpStream,
    bytes: &[u8],
    operation_timeout: Duration,
) -> Result<()> {
    timeout(operation_timeout, socket.write_all(bytes))
        .await
        .context("socket write timed out")??;
    Ok(())
}

async fn shutdown_with_timeout(socket: &mut TcpStream, operation_timeout: Duration) -> Result<()> {
    match timeout(operation_timeout, socket.shutdown()).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) if is_normal_disconnect(&error) => Ok(()),
        Ok(Err(error)) => Err(error).context("socket shutdown failed"),
        Err(_) => bail!("socket shutdown timed out"),
    }
}

fn is_normal_disconnect(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        ErrorKind::BrokenPipe | ErrorKind::ConnectionAborted | ErrorKind::ConnectionReset
    )
}

struct PacketCursor<'a> {
    packet: &'a [u8],
    offset: usize,
}

impl<'a> PacketCursor<'a> {
    const fn new(packet: &'a [u8]) -> Self {
        Self { packet, offset: 0 }
    }

    fn read_varint(&mut self) -> Result<i32> {
        let (value, length) = decode_varint(&self.packet[self.offset..])?;
        self.offset += length;
        Ok(value)
    }

    fn read_string(&mut self, max_utf16_units: usize) -> Result<&'a str> {
        let byte_length = self.read_varint()?;
        ensure!(byte_length >= 0, "string has a negative byte length");
        let byte_length = usize::try_from(byte_length).context("string byte length is negative")?;
        ensure!(
            byte_length <= max_utf16_units * 3,
            "string exceeds its encoded byte limit"
        );
        let bytes = self.read_bytes(byte_length)?;
        let value = std::str::from_utf8(bytes).context("string is not valid UTF-8")?;
        ensure!(
            value.encode_utf16().count() <= max_utf16_units,
            "string exceeds its UTF-16 length limit"
        );
        Ok(value)
    }

    fn read_u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(self.read_array()?))
    }

    fn read_i64(&mut self) -> Result<i64> {
        Ok(i64::from_be_bytes(self.read_array()?))
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N]> {
        Ok(self
            .read_bytes(N)?
            .try_into()
            .expect("read_bytes returns the requested length"))
    }

    fn read_bytes(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(length)
            .context("packet offset overflow")?;
        ensure!(end <= self.packet.len(), "packet is truncated");
        let bytes = &self.packet[self.offset..end];
        self.offset = end;
        Ok(bytes)
    }

    const fn remaining(&self) -> usize {
        self.packet.len() - self.offset
    }

    fn ensure_finished(&self) -> Result<()> {
        ensure!(self.remaining() == 0, "packet contains trailing data");
        Ok(())
    }
}

fn decode_varint(input: &[u8]) -> Result<(i32, usize)> {
    let mut result = 0u32;
    for position in 0..5 {
        let byte = *input.get(position).context("truncated VarInt")?;
        if position == 4 {
            ensure!(byte & 0xf0 == 0, "VarInt exceeds 32 bits");
        }
        result |= u32::from(byte & 0x7f) << (position * 7);
        if byte & 0x80 == 0 {
            return Ok((result.cast_signed(), position + 1));
        }
    }
    bail!("VarInt exceeds five bytes")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    async fn socket_pair() -> (TcpStream, TcpStream) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener should bind");
        let address = listener.local_addr().expect("listener has an address");
        let client = TcpStream::connect(address);
        let server = listener.accept();
        let (client, server) = tokio::join!(client, server);
        (
            client.expect("client should connect"),
            server.expect("server should accept").0,
        )
    }

    fn frame_raw_packet(body: &[u8]) -> Vec<u8> {
        let mut packet = Vec::new();
        write_varint(
            i32::try_from(body.len()).expect("test packet length fits in i32"),
            &mut packet,
        );
        packet.extend_from_slice(body);
        packet
    }

    fn encode_handshake(protocol: i32, address: &str, port: u16, intent: i32) -> Vec<u8> {
        let mut packet = Vec::new();
        write_varint(0, &mut packet);
        write_varint(protocol, &mut packet);
        write_varint(
            i32::try_from(address.len()).expect("test address length fits in i32"),
            &mut packet,
        );
        packet.extend_from_slice(address.as_bytes());
        packet.extend_from_slice(&port.to_be_bytes());
        write_varint(intent, &mut packet);
        packet
    }

    fn encode_login_start(username: &str, uuid: [u8; 16]) -> Vec<u8> {
        let mut packet = Vec::new();
        write_varint(0, &mut packet);
        write_varint(
            i32::try_from(username.len()).expect("test username length fits in i32"),
            &mut packet,
        );
        packet.extend_from_slice(username.as_bytes());
        packet.extend_from_slice(&uuid);
        packet
    }

    #[test]
    fn varint_matches_protocol_examples() {
        let examples: &[(i32, &[u8])] = &[
            (0, &[0x00]),
            (1, &[0x01]),
            (127, &[0x7f]),
            (128, &[0x80, 0x01]),
            (25565, &[0xdd, 0xc7, 0x01]),
            (i32::MAX, &[0xff, 0xff, 0xff, 0xff, 0x07]),
            (-1, &[0xff, 0xff, 0xff, 0xff, 0x0f]),
            (i32::MIN, &[0x80, 0x80, 0x80, 0x80, 0x08]),
        ];

        for &(value, expected) in examples {
            let mut encoded = Vec::new();
            write_varint(value, &mut encoded);
            assert_eq!(encoded, expected);
            assert_eq!(decode_varint(expected).expect("valid VarInt").0, value);
        }
    }

    #[test]
    fn parses_current_login_and_transfer_handshakes() {
        assert_eq!(
            parse_handshake(&encode_handshake(
                MINECRAFT_PROTOCOL_VERSION,
                "localhost",
                25565,
                2,
            ))
            .expect("valid login handshake"),
            Handshake {
                protocol_version: MINECRAFT_PROTOCOL_VERSION,
                intent: HandshakeIntent::Login,
            }
        );
        assert_eq!(
            parse_handshake(&encode_handshake(
                MINECRAFT_PROTOCOL_VERSION,
                "localhost",
                25565,
                3,
            ))
            .expect("valid transfer handshake")
            .intent,
            HandshakeIntent::Transfer
        );
    }

    #[test]
    fn rejects_truncated_and_trailing_handshakes() {
        let mut truncated = encode_handshake(MINECRAFT_PROTOCOL_VERSION, "localhost", 25565, 2);
        truncated.pop();
        assert!(parse_handshake(&truncated).is_err());

        let mut trailing = encode_handshake(MINECRAFT_PROTOCOL_VERSION, "localhost", 25565, 2);
        trailing.push(0);
        assert!(parse_handshake(&trailing).is_err());
    }

    #[test]
    fn status_response_advertises_only_the_current_protocol() {
        let responder = MinecraftResponder::new(&Config::default(), None)
            .expect("responder should be constructed");
        let framed = &responder.status_response;
        let (frame_length, length_bytes) = decode_varint(framed).expect("frame length");
        assert_eq!(
            usize::try_from(frame_length).expect("positive frame length"),
            framed.len() - length_bytes
        );

        let mut cursor = PacketCursor::new(&framed[length_bytes..]);
        assert_eq!(cursor.read_varint().expect("packet ID"), 0);
        let json = cursor.read_string(32_767).expect("status JSON");
        let value: Value = serde_json::from_str(json).expect("valid status JSON");
        assert_eq!(value["version"]["protocol"], MINECRAFT_PROTOCOL_VERSION);
        assert_eq!(value["version"]["name"], MINECRAFT_VERSION_NAME);
    }

    #[tokio::test]
    async fn reads_coalesced_handshake_and_status_packets() {
        let (mut client, mut server) = socket_pair().await;
        let handshake = encode_handshake(1234, "localhost", 25565, 1);
        let mut exchange = frame_raw_packet(&handshake);
        exchange.extend_from_slice(&frame_raw_packet(&[0]));
        client
            .write_all(&exchange)
            .await
            .expect("client exchange should write");

        assert_eq!(
            read_client_request(&mut server)
                .await
                .expect("coalesced exchange should parse"),
            ClientRequest::Status
        );
    }

    #[tokio::test]
    async fn login_request_requires_and_consumes_login_start() {
        let (mut client, mut server) = socket_pair().await;
        let handshake = encode_handshake(MINECRAFT_PROTOCOL_VERSION, "localhost", 25565, 2);
        let mut exchange = frame_raw_packet(&handshake);
        exchange.extend_from_slice(&frame_raw_packet(&encode_login_start("player", [7; 16])));
        client
            .write_all(&exchange)
            .await
            .expect("client exchange should write");

        assert_eq!(
            read_client_request(&mut server)
                .await
                .expect("login exchange should parse"),
            ClientRequest::Login {
                intent: LoginIntent::Login
            }
        );
    }

    #[tokio::test]
    async fn rejects_non_current_login_protocol_without_waking() {
        let (mut client, mut server) = socket_pair().await;
        let handshake = encode_handshake(MINECRAFT_PROTOCOL_VERSION - 1, "localhost", 25565, 2);
        client
            .write_all(&frame_raw_packet(&handshake))
            .await
            .expect("client handshake should write");

        assert_eq!(
            read_client_request(&mut server)
                .await
                .expect("handshake should parse"),
            ClientRequest::UnsupportedProtocol {
                protocol_version: MINECRAFT_PROTOCOL_VERSION - 1
            }
        );
    }

    #[tokio::test]
    async fn rejects_legacy_unframed_ping() {
        let (mut client, mut server) = socket_pair().await;
        client
            .write_all(&[0xfe, 0x01])
            .await
            .expect("legacy bytes should write");
        client
            .shutdown()
            .await
            .expect("legacy client write half should close");

        assert!(read_client_request(&mut server).await.is_err());
    }

    #[test]
    fn login_start_requires_current_uuid_layout_and_no_trailing_data() {
        let valid = encode_login_start("player", [9; 16]);
        assert!(parse_login_start(&valid).is_ok());

        let mut missing_uuid = valid.clone();
        missing_uuid.pop();
        assert!(parse_login_start(&missing_uuid).is_err());

        let mut trailing = valid;
        trailing.push(0);
        assert!(parse_login_start(&trailing).is_err());
    }

    #[tokio::test]
    async fn status_service_echoes_ping_timestamp() {
        let (mut client, mut server) = socket_pair().await;
        let responder = MinecraftResponder::new(&Config::default(), None)
            .expect("responder should be constructed");
        let server_task = tokio::spawn(async move {
            responder
                .serve_status(&mut server, Duration::from_secs(2))
                .await
        });

        let status = read_packet(&mut client, MAX_PACKET_LENGTH)
            .await
            .expect("status response should arrive");
        let mut status_cursor = PacketCursor::new(&status);
        assert_eq!(status_cursor.read_varint().expect("status packet ID"), 0);
        let status_json = status_cursor.read_string(32_767).expect("status JSON");
        let status_value: Value = serde_json::from_str(status_json).expect("valid status JSON");
        assert_eq!(
            status_value["version"]["protocol"],
            MINECRAFT_PROTOCOL_VERSION
        );

        let timestamp = 1_234_567_890_i64;
        let outbound = encode_packet(1, &timestamp.to_be_bytes()).expect("ping should encode");
        client
            .write_all(&outbound)
            .await
            .expect("ping should write");

        let inbound = read_packet(&mut client, MAX_STATUS_PACKET_LENGTH)
            .await
            .expect("pong should arrive");
        let mut pong_cursor = PacketCursor::new(&inbound);
        assert_eq!(pong_cursor.read_varint().expect("pong packet ID"), 1);
        assert_eq!(pong_cursor.read_i64().expect("pong timestamp"), timestamp);
        pong_cursor
            .ensure_finished()
            .expect("pong has no trailing data");
        server_task
            .await
            .expect("status task should not panic")
            .expect("status service should succeed");
    }

    #[tokio::test]
    async fn status_service_rejects_malformed_ping() {
        let (mut client, mut server) = socket_pair().await;
        let responder = MinecraftResponder::new(&Config::default(), None)
            .expect("responder should be constructed");
        let server_task = tokio::spawn(async move {
            responder
                .serve_status(&mut server, Duration::from_secs(2))
                .await
        });

        read_packet(&mut client, MAX_PACKET_LENGTH)
            .await
            .expect("status response should arrive");
        let malformed = encode_packet(0, &[]).expect("malformed ping should encode");
        client
            .write_all(&malformed)
            .await
            .expect("malformed ping should write");

        assert!(
            server_task
                .await
                .expect("status task should not panic")
                .is_err()
        );
    }
}
