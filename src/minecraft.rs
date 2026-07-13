use std::io::ErrorKind;
use std::ops::Range;
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use serde::Serialize;
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Instant, timeout, timeout_at};

use crate::config::Config;

mod version;

use version::InitialProtocol;
pub use version::MinecraftVersion;

const MAX_PACKET_LENGTH: usize = (1 << 21) - 1;
const MAX_HANDSHAKE_PACKET_LENGTH: usize = 1_024;
const MAX_LOGIN_START_PACKET_LENGTH: usize = 128;
const MAX_STATUS_PACKET_LENGTH: usize = 32;
const CONFLICT_DISCONNECT_MESSAGE: &str = "MCServerNap cannot safely start the server because the backend state is uncertain. Try again later or contact the server administrator.";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LoginIntent {
    Login,
    Transfer,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HandshakeIntent {
    Status,
    Login,
    Transfer,
    Unknown(i32),
}

// Deliberately no Debug: forwarding addresses may contain profiles or authentication tokens.
#[allow(
    dead_code,
    reason = "the single-server routing insertion point retains address and port without selecting on them"
)]
pub(crate) struct ConnectionEnvelope {
    socket: TcpStream,
    framed_handshake: Vec<u8>,
    protocol_version: i32,
    requested_address_range: Range<usize>,
    requested_port: u16,
    intent: HandshakeIntent,
}

impl ConnectionEnvelope {
    pub(crate) async fn read(socket: TcpStream, deadline: Instant) -> Result<Self> {
        timeout_at(deadline, Self::read_inner(socket))
            .await
            .context("initial Minecraft handshake timed out")?
    }

    async fn read_inner(mut socket: TcpStream) -> Result<Self> {
        let mut framed_handshake = Vec::with_capacity(3);
        let mut declared_length = 0usize;

        for position in 0..3 {
            let byte = socket
                .read_u8()
                .await
                .context("client closed a partial handshake length")?;
            framed_handshake.push(byte);
            declared_length |= usize::from(byte & 0x7f) << (position * 7);

            if byte & 0x80 == 0 {
                ensure!(
                    declared_length <= MAX_PACKET_LENGTH,
                    "handshake exceeds protocol limit"
                );
                ensure!(
                    declared_length <= MAX_HANDSHAKE_PACKET_LENGTH,
                    "handshake length {declared_length} exceeds the {MAX_HANDSHAKE_PACKET_LENGTH}-byte limit"
                );

                let body_offset = framed_handshake.len();
                framed_handshake.resize(body_offset + declared_length, 0);
                socket
                    .read_exact(&mut framed_handshake[body_offset..])
                    .await
                    .context("client closed a partial handshake body")?;

                let handshake = parse_handshake(&framed_handshake[body_offset..])?;
                let requested_address_range = (body_offset
                    + handshake.requested_address_range.start)
                    ..(body_offset + handshake.requested_address_range.end);
                return Ok(Self {
                    socket,
                    framed_handshake,
                    protocol_version: handshake.protocol_version,
                    requested_address_range,
                    requested_port: handshake.requested_port,
                    intent: handshake.intent,
                });
            }
        }

        bail!("handshake length VarInt exceeds three bytes")
    }

    pub(crate) const fn protocol_version(&self) -> i32 {
        self.protocol_version
    }

    pub(crate) const fn intent(&self) -> HandshakeIntent {
        self.intent
    }

    pub(crate) fn socket_mut(&mut self) -> &mut TcpStream {
        &mut self.socket
    }

    pub(crate) fn into_proxy_parts(self) -> (TcpStream, Vec<u8>) {
        (self.socket, self.framed_handshake)
    }
}

#[allow(
    dead_code,
    reason = "the single-server routing insertion point does not consume its endpoint yet"
)]
impl ConnectionEnvelope {
    pub(crate) fn requested_address(&self) -> &str {
        std::str::from_utf8(&self.framed_handshake[self.requested_address_range.clone()])
            .expect("requested address was validated while constructing the envelope")
    }

    pub(crate) const fn requested_port(&self) -> u16 {
        self.requested_port
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClientRequest {
    Status,
    Login { intent: LoginIntent },
    UnsupportedProtocol { protocol_version: i32 },
}

#[derive(Debug)]
pub struct MinecraftResponder {
    minecraft_version: MinecraftVersion,
    status_response: Vec<u8>,
    login_disconnect: Vec<u8>,
    conflict_disconnect: Vec<u8>,
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
        let minecraft_version = config.minecraft_version;
        let status = StatusResponse {
            version: StatusVersion {
                name: minecraft_version.name(),
                protocol: minecraft_version.protocol(),
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
        let conflict_component = json!({
            "text": CONFLICT_DISCONNECT_MESSAGE,
            "color": "red",
        });
        let conflict_json = serde_json::to_string(&conflict_component)
            .context("failed to serialize the conflict disconnect message")?;
        let conflict_disconnect = encode_string_packet(0, &conflict_json)?;
        let incompatible_component = json!({
            "text": format!(
                "This server requires Minecraft Java {} (protocol {}).",
                minecraft_version.name(),
                minecraft_version.protocol(),
            ),
            "color": "red",
            "bold": true,
        });
        let incompatible_json = serde_json::to_string(&incompatible_component)
            .context("failed to serialize the incompatible-version message")?;
        let incompatible_disconnect = encode_string_packet(0, &incompatible_json)?;

        Ok(Self {
            minecraft_version,
            status_response,
            login_disconnect,
            conflict_disconnect,
            incompatible_disconnect,
        })
    }

    pub(crate) async fn read_sleeping_request(
        &self,
        connection: &mut ConnectionEnvelope,
        deadline: Instant,
    ) -> Result<Option<ClientRequest>> {
        let login_intent = match connection.intent() {
            HandshakeIntent::Status => {
                let request = read_initial_packet(
                    connection.socket_mut(),
                    MAX_STATUS_PACKET_LENGTH,
                    deadline,
                )
                .await?;
                let mut cursor = PacketCursor::new(&request);
                ensure!(
                    cursor.read_varint()? == 0,
                    "expected status request packet ID 0"
                );
                cursor.ensure_finished()?;
                return Ok(Some(ClientRequest::Status));
            }
            HandshakeIntent::Login => LoginIntent::Login,
            HandshakeIntent::Transfer => LoginIntent::Transfer,
            HandshakeIntent::Unknown(_) => return Ok(None),
        };

        if connection.protocol_version() != self.minecraft_version.protocol() {
            return Ok(Some(ClientRequest::UnsupportedProtocol {
                protocol_version: connection.protocol_version(),
            }));
        }

        validate_login_intent(self.minecraft_version, login_intent)?;
        let login_start = read_initial_packet(
            connection.socket_mut(),
            MAX_LOGIN_START_PACKET_LENGTH,
            deadline,
        )
        .await?;
        parse_login_start(&login_start, self.minecraft_version)?;
        Ok(Some(ClientRequest::Login {
            intent: login_intent,
        }))
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

    pub async fn send_conflict_disconnect(
        &self,
        socket: &mut TcpStream,
        operation_timeout: Duration,
    ) -> Result<()> {
        write_all_with_timeout(socket, &self.conflict_disconnect, operation_timeout)
            .await
            .context("failed to send conflict disconnect")?;
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

fn validate_login_intent(minecraft_version: MinecraftVersion, intent: LoginIntent) -> Result<()> {
    ensure!(
        intent != LoginIntent::Transfer
            || minecraft_version.initial_protocol() == InitialProtocol::RequiredUuidAndTransfer,
        "transfer login is not supported by Minecraft Java {}",
        minecraft_version.name()
    );
    Ok(())
}

fn parse_login_start(packet: &[u8], minecraft_version: MinecraftVersion) -> Result<()> {
    let mut cursor = PacketCursor::new(packet);
    ensure!(
        cursor.read_varint()? == 0,
        "expected login start packet ID 0"
    );
    let username = cursor.read_string(16)?;
    ensure!(!username.is_empty(), "login username must not be empty");
    match minecraft_version.initial_protocol() {
        InitialProtocol::OptionalUuid => {
            if cursor.read_bool()? {
                cursor.read_bytes(16)?;
            }
        }
        InitialProtocol::RequiredUuid | InitialProtocol::RequiredUuidAndTransfer => {
            cursor.read_bytes(16)?;
        }
    }
    cursor.ensure_finished()
}

struct Handshake {
    protocol_version: i32,
    requested_address_range: Range<usize>,
    requested_port: u16,
    intent: HandshakeIntent,
}

fn parse_handshake(packet: &[u8]) -> Result<Handshake> {
    let mut cursor = PacketCursor::new(packet);
    ensure!(cursor.read_varint()? == 0, "expected handshake packet ID 0");
    let protocol_version = cursor.read_varint()?;
    let (_, requested_address_range) = cursor.read_string_with_range(255)?;
    let requested_port = cursor.read_u16()?;
    let intent = match cursor.read_varint()? {
        1 => HandshakeIntent::Status,
        2 => HandshakeIntent::Login,
        3 => HandshakeIntent::Transfer,
        value => HandshakeIntent::Unknown(value),
    };
    cursor.ensure_finished()?;
    Ok(Handshake {
        protocol_version,
        requested_address_range,
        requested_port,
        intent,
    })
}

async fn read_initial_packet(
    socket: &mut TcpStream,
    application_limit: usize,
    deadline: Instant,
) -> Result<Vec<u8>> {
    timeout_at(deadline, read_packet(socket, application_limit))
        .await
        .context("initial Minecraft exchange timed out")?
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
        Ok(self.read_string_with_range(max_utf16_units)?.0)
    }

    fn read_string_with_range(
        &mut self,
        max_utf16_units: usize,
    ) -> Result<(&'a str, Range<usize>)> {
        let byte_length = self.read_varint()?;
        ensure!(byte_length >= 0, "string has a negative byte length");
        let byte_length = usize::try_from(byte_length).context("string byte length is negative")?;
        ensure!(
            byte_length <= max_utf16_units * 3,
            "string exceeds its encoded byte limit"
        );
        let start = self.offset;
        let bytes = self.read_bytes(byte_length)?;
        let value = std::str::from_utf8(bytes).context("string is not valid UTF-8")?;
        ensure!(
            value.encode_utf16().count() <= max_utf16_units,
            "string exceeds its UTF-16 length limit"
        );
        Ok((value, start..self.offset))
    }

    fn read_u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(self.read_array()?))
    }

    fn read_bool(&mut self) -> Result<bool> {
        match self.read_array::<1>()?[0] {
            0 => Ok(false),
            1 => Ok(true),
            value => bail!("invalid Boolean value {value}"),
        }
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

    fn version_named(name: &str) -> MinecraftVersion {
        MinecraftVersion::supported()
            .find(|version| version.name() == name)
            .expect("test version should be in the catalogue")
    }

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

    fn responder_for_version(minecraft_version: MinecraftVersion) -> MinecraftResponder {
        MinecraftResponder::new(
            &Config {
                minecraft_version,
                ..Config::default()
            },
            None,
        )
        .expect("test responder should be constructed")
    }

    async fn read_connection(framed_handshake: &[u8]) -> Result<ConnectionEnvelope> {
        let (mut client, server) = socket_pair().await;
        client
            .write_all(framed_handshake)
            .await
            .expect("test handshake should write");
        client
            .shutdown()
            .await
            .expect("test client write half should close");
        ConnectionEnvelope::read(server, Instant::now() + Duration::from_secs(2)).await
    }

    async fn read_sleeping_exchange(
        exchange: &[u8],
        minecraft_version: MinecraftVersion,
    ) -> Result<Option<ClientRequest>> {
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut connection = read_connection(exchange).await?;
        responder_for_version(minecraft_version)
            .read_sleeping_request(&mut connection, deadline)
            .await
    }

    fn encode_login_start_prefix(username: &str) -> Vec<u8> {
        let mut packet = Vec::new();
        write_varint(0, &mut packet);
        write_varint(
            i32::try_from(username.len()).expect("test username length fits in i32"),
            &mut packet,
        );
        packet.extend_from_slice(username.as_bytes());
        packet
    }

    fn encode_required_uuid_login_start(username: &str, uuid: [u8; 16]) -> Vec<u8> {
        let mut packet = encode_login_start_prefix(username);
        packet.extend_from_slice(&uuid);
        packet
    }

    fn encode_optional_uuid_login_start(username: &str, uuid: Option<[u8; 16]>) -> Vec<u8> {
        let mut packet = encode_login_start_prefix(username);
        packet.push(u8::from(uuid.is_some()));
        if let Some(uuid) = uuid {
            packet.extend_from_slice(&uuid);
        }
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

    #[tokio::test]
    async fn connection_envelope_extracts_routing_metadata_and_all_intents() {
        let protocol_version = 1_234;
        let address = "play.example.test";
        let requested_port = 25_565;
        for (value, expected) in [
            (1, HandshakeIntent::Status),
            (2, HandshakeIntent::Login),
            (3, HandshakeIntent::Transfer),
            (77, HandshakeIntent::Unknown(77)),
        ] {
            let body = encode_handshake(protocol_version, address, requested_port, value);
            let framed = frame_raw_packet(&body);
            let connection = read_connection(&framed)
                .await
                .expect("structurally valid handshake should parse");

            assert_eq!(connection.protocol_version(), protocol_version);
            assert_eq!(connection.requested_address(), address);
            assert_eq!(connection.requested_port(), requested_port);
            assert_eq!(connection.intent(), expected);
            assert_eq!(connection.framed_handshake, framed);
            let address_pointer = connection.requested_address().as_ptr() as usize;
            let frame_start = connection.framed_handshake.as_ptr() as usize;
            assert!((frame_start..frame_start + framed.len()).contains(&address_pointer));
        }
    }

    #[tokio::test]
    async fn connection_envelope_preserves_embedded_nul_address_data() {
        let address = "localhost\0FORGE\0profile-token";
        let body = encode_handshake(763, address, 25_565, 2);
        let connection = read_connection(&frame_raw_packet(&body))
            .await
            .expect("Forge-style handshake should parse");

        assert_eq!(connection.requested_address(), address);
        assert_eq!(connection.intent(), HandshakeIntent::Login);
    }

    #[tokio::test]
    async fn connection_envelope_accepts_non_minimal_frame_and_field_varints() {
        let mut body = vec![0x80, 0x00, 0xfb, 0x00, 0x89, 0x00];
        body.extend_from_slice(b"localhost");
        body.extend_from_slice(&25_565_u16.to_be_bytes());
        body.extend_from_slice(&[0x82, 0x00]);
        let body_length = u8::try_from(body.len()).expect("test body length fits in one byte");
        let mut framed = vec![body_length | 0x80, 0x80, 0x00];
        framed.extend_from_slice(&body);

        let connection = read_connection(&framed)
            .await
            .expect("non-minimal VarInts should remain valid");
        assert_eq!(connection.framed_handshake, framed);
        assert_eq!(connection.protocol_version(), 123);
        assert_eq!(connection.requested_address(), "localhost");
        assert_eq!(connection.requested_port(), 25_565);
        assert_eq!(connection.intent(), HandshakeIntent::Login);
    }

    #[tokio::test]
    async fn connection_envelope_reads_one_byte_fragmentation() {
        let body = encode_handshake(763, "fragmented.example", 25_565, 3);
        let framed = frame_raw_packet(&body);
        let (mut client, server) = socket_pair().await;
        let expected = framed.clone();
        let writer = tokio::spawn(async move {
            for byte in framed {
                client
                    .write_all(&[byte])
                    .await
                    .expect("fragment should write");
                tokio::task::yield_now().await;
            }
        });

        let connection = ConnectionEnvelope::read(server, Instant::now() + Duration::from_secs(2))
            .await
            .expect("fragmented handshake should parse");
        writer.await.expect("writer should not panic");
        assert_eq!(connection.framed_handshake, expected);
        assert_eq!(connection.requested_address(), "fragmented.example");
        assert_eq!(connection.intent(), HandshakeIntent::Transfer);
    }

    #[tokio::test]
    async fn connection_envelope_enforces_utf16_address_limit() {
        let accepted = format!("{}a", "😀".repeat(127));
        assert_eq!(accepted.encode_utf16().count(), 255);
        let accepted_body = encode_handshake(763, &accepted, 25_565, 2);
        let connection = read_connection(&frame_raw_packet(&accepted_body))
            .await
            .expect("255 UTF-16 code units should be accepted");
        assert_eq!(connection.requested_address(), accepted);

        let rejected = "😀".repeat(128);
        assert_eq!(rejected.encode_utf16().count(), 256);
        let rejected_body = encode_handshake(763, &rejected, 25_565, 2);
        assert!(
            read_connection(&frame_raw_packet(&rejected_body))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn oversized_declared_handshake_is_rejected_without_waiting_for_a_body() {
        let (mut client, server) = socket_pair().await;
        let mut prefix = Vec::new();
        write_varint(
            i32::try_from(MAX_HANDSHAKE_PACKET_LENGTH + 1).expect("test length fits in i32"),
            &mut prefix,
        );
        client
            .write_all(&prefix)
            .await
            .expect("oversized prefix should write");

        let result = timeout(
            Duration::from_millis(100),
            ConnectionEnvelope::read(server, Instant::now() + Duration::from_secs(1)),
        )
        .await
        .expect("declared length should be rejected before reading a body");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn connection_envelope_rejects_malformed_handshakes_and_frames() {
        let valid = encode_handshake(763, "localhost", 25_565, 2);

        let mut invalid_utf8 = encode_handshake(763, "x", 25_565, 2);
        let address_offset = 1 + varint_size(763) + 1;
        invalid_utf8[address_offset] = 0xff;

        let mut wrong_packet_id = valid.clone();
        wrong_packet_id[0] = 1;

        let mut trailing = valid.clone();
        trailing.push(0);

        for body in [invalid_utf8, wrong_packet_id, trailing] {
            assert!(read_connection(&frame_raw_packet(&body)).await.is_err());
        }

        for incomplete_frame in [Vec::new(), vec![0x80], vec![5, 0, 1]] {
            assert!(read_connection(&incomplete_frame).await.is_err());
        }

        assert!(read_connection(&[0x80, 0x80, 0x80]).await.is_err());

        let mut structurally_truncated = valid;
        structurally_truncated.pop();
        assert!(
            read_connection(&frame_raw_packet(&structurally_truncated))
                .await
                .is_err()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn initial_deadline_is_absolute_across_handshake_and_sleeping_request() {
        let body = encode_handshake(763, "localhost", 25_565, 1);
        let framed = frame_raw_packet(&body);
        let (mut client, server) = socket_pair().await;
        let deadline = Instant::now() + Duration::from_secs(5);
        let reader = tokio::spawn(async move {
            let mut connection = ConnectionEnvelope::read(server, deadline).await?;
            responder_for_version(MinecraftVersion::latest())
                .read_sleeping_request(&mut connection, deadline)
                .await
        });

        for byte in framed {
            client
                .write_all(&[byte])
                .await
                .expect("trickle byte should write");
            tokio::time::advance(Duration::from_millis(100)).await;
        }
        client
            .write_all(&[1])
            .await
            .expect("status frame prefix should write");
        let past_original_deadline =
            deadline.saturating_duration_since(Instant::now()) + Duration::from_millis(1);
        tokio::time::advance(past_original_deadline).await;
        tokio::task::yield_now().await;

        assert!(
            reader.is_finished(),
            "reader should expire at the original absolute deadline"
        );

        let error = reader
            .await
            .expect("reader should not panic")
            .expect_err("the original deadline should expire");
        assert_eq!(error.to_string(), "initial Minecraft exchange timed out");
    }

    #[test]
    fn status_response_advertises_the_configured_version() {
        for minecraft_version in MinecraftVersion::supported() {
            let config = Config {
                minecraft_version,
                ..Config::default()
            };
            let responder =
                MinecraftResponder::new(&config, None).expect("responder should be constructed");
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
            assert_eq!(value["version"]["protocol"], minecraft_version.protocol());
            assert_eq!(value["version"]["name"], minecraft_version.name());
        }
    }

    #[test]
    fn conflict_disconnect_is_fixed_and_red() {
        let responder = MinecraftResponder::new(&Config::default(), None)
            .expect("responder should be constructed");
        let (frame_length, frame_prefix) =
            decode_varint(&responder.conflict_disconnect).expect("frame length");
        assert_eq!(
            usize::try_from(frame_length).expect("positive frame length"),
            responder.conflict_disconnect.len() - frame_prefix
        );
        let mut cursor = PacketCursor::new(&responder.conflict_disconnect[frame_prefix..]);
        assert_eq!(cursor.read_varint().expect("disconnect packet ID"), 0);
        let component: Value = serde_json::from_str(
            cursor
                .read_string(32_767)
                .expect("disconnect JSON should be present"),
        )
        .expect("disconnect component should be valid JSON");
        cursor
            .ensure_finished()
            .expect("disconnect packet should have no trailing data");
        assert_eq!(component["text"], CONFLICT_DISCONNECT_MESSAGE);
        assert_eq!(component["color"], "red");
        assert_eq!(component.as_object().expect("JSON object").len(), 2);
    }

    #[tokio::test]
    async fn reads_coalesced_handshake_and_status_packets() {
        let handshake = encode_handshake(1234, "localhost", 25565, 1);
        let mut exchange = frame_raw_packet(&handshake);
        exchange.extend_from_slice(&frame_raw_packet(&[0]));

        assert_eq!(
            read_sleeping_exchange(&exchange, MinecraftVersion::latest())
                .await
                .expect("coalesced exchange should parse"),
            Some(ClientRequest::Status)
        );
    }

    #[tokio::test]
    async fn login_request_requires_and_consumes_login_start() {
        let minecraft_version = MinecraftVersion::latest();
        let handshake = encode_handshake(minecraft_version.protocol(), "localhost", 25565, 2);
        let mut exchange = frame_raw_packet(&handshake);
        exchange.extend_from_slice(&frame_raw_packet(&encode_required_uuid_login_start(
            "player", [7; 16],
        )));
        assert_eq!(
            read_sleeping_exchange(&exchange, minecraft_version)
                .await
                .expect("login exchange should parse"),
            Some(ClientRequest::Login {
                intent: LoginIntent::Login
            })
        );
    }

    #[tokio::test]
    async fn minecraft_1_20_1_login_request_accepts_absent_uuid() {
        let minecraft_version = version_named("1.20.1");
        let handshake = encode_handshake(minecraft_version.protocol(), "localhost", 25565, 2);
        let mut exchange = frame_raw_packet(&handshake);
        exchange.extend_from_slice(&frame_raw_packet(&encode_optional_uuid_login_start(
            "player", None,
        )));
        assert_eq!(
            read_sleeping_exchange(&exchange, minecraft_version)
                .await
                .expect("1.20.1 login exchange should parse"),
            Some(ClientRequest::Login {
                intent: LoginIntent::Login
            })
        );
    }

    #[test]
    fn transfer_intent_follows_the_initial_protocol() {
        for minecraft_version in MinecraftVersion::supported() {
            let result = validate_login_intent(minecraft_version, LoginIntent::Transfer);
            assert_eq!(
                result.is_ok(),
                minecraft_version.initial_protocol() == InitialProtocol::RequiredUuidAndTransfer,
                "unexpected transfer support for {}",
                minecraft_version.name()
            );
        }
    }

    #[tokio::test]
    async fn rejects_unconfigured_login_protocol_without_waking() {
        let minecraft_version = MinecraftVersion::latest();
        let unsupported_protocol = minecraft_version.protocol() - 1;
        let handshake = encode_handshake(unsupported_protocol, "localhost", 25565, 2);

        assert_eq!(
            read_sleeping_exchange(&frame_raw_packet(&handshake), minecraft_version)
                .await
                .expect("handshake should parse"),
            Some(ClientRequest::UnsupportedProtocol {
                protocol_version: unsupported_protocol
            })
        );
    }

    #[tokio::test]
    async fn rejects_legacy_unframed_ping() {
        let (mut client, server) = socket_pair().await;
        client
            .write_all(&[0xfe, 0x01])
            .await
            .expect("legacy bytes should write");
        client
            .shutdown()
            .await
            .expect("legacy client write half should close");

        assert!(
            ConnectionEnvelope::read(server, Instant::now() + Duration::from_secs(2))
                .await
                .is_err()
        );
    }

    #[test]
    fn required_uuid_login_start_rejects_missing_or_trailing_data() {
        let valid = encode_required_uuid_login_start("player", [9; 16]);
        for minecraft_version in MinecraftVersion::supported()
            .filter(|version| version.initial_protocol() != InitialProtocol::OptionalUuid)
        {
            assert!(parse_login_start(&valid, minecraft_version).is_ok());

            let mut missing_uuid = valid.clone();
            missing_uuid.pop();
            assert!(parse_login_start(&missing_uuid, minecraft_version).is_err());

            let mut trailing = valid.clone();
            trailing.push(0);
            assert!(parse_login_start(&trailing, minecraft_version).is_err());
        }
    }

    #[test]
    fn optional_uuid_login_start_accepts_present_or_absent_uuid() {
        let minecraft_version = version_named("1.20.1");
        let without_uuid = encode_optional_uuid_login_start("player", None);
        let with_uuid = encode_optional_uuid_login_start("player", Some([9; 16]));
        assert!(parse_login_start(&without_uuid, minecraft_version).is_ok());
        assert!(parse_login_start(&with_uuid, minecraft_version).is_ok());

        let mut invalid_boolean = without_uuid;
        *invalid_boolean.last_mut().expect("packet contains Boolean") = 2;
        assert!(parse_login_start(&invalid_boolean, minecraft_version).is_err());

        let missing_optional_flag = encode_login_start_prefix("player");
        assert!(parse_login_start(&missing_optional_flag, minecraft_version).is_err());
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
            MinecraftVersion::latest().protocol()
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
