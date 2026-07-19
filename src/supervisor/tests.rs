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
            scripted_probes: Some(Arc::new(tokio::sync::Mutex::new(probe_receiver))),
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
            scripted_probes: Some(Arc::new(tokio::sync::Mutex::new(probe_receiver))),
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

fn assert_retained_test_failure(
    lifecycle: LifecycleState,
    phase: ServerPhase,
    command_revision: u64,
    launch_generation: u64,
) {
    assert_eq!(lifecycle.phase, phase);
    assert_eq!(lifecycle.failure, Some(FailureCategory::ExitedUnexpectedly));
    assert_eq!(lifecycle.failure_streak, 3);
    assert_eq!(lifecycle.command_revision, command_revision);
    assert_eq!(lifecycle.launch_generation, launch_generation);
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

struct HeldRestartSupervisor {
    wake_root: WakeLatchRoot,
    wake_sender: WakeLatchSender,
    mutations: mpsc::Sender<MutationEnvelope>,
    shutdown_sender: Option<oneshot::Sender<()>>,
    lifecycle: watch::Receiver<LifecycleState>,
    task: JoinHandle<Result<()>>,
    probes: mpsc::UnboundedSender<ReconciliationResult>,
    probe_gate: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<ReconciliationResult>>>,
    launches: TcpListener,
    old_signal: Option<TcpStream>,
    _runtime_drained: watch::Sender<bool>,
}

impl HeldRestartSupervisor {
    async fn spawn() -> Self {
        let launches = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test launch listener should bind");
        let launch_address = launches
            .local_addr()
            .expect("test launch listener should have an address");
        let mut config = no_rcon_config();
        config.launch.arguments[2] = "supervisor::tests::signaled_exit_fixture".into();
        config
            .launch
            .arguments
            .insert(3, format!("exit-signal={launch_address}").into());
        config.shutdown_timeout = Duration::from_secs(5);

        let (probes, backend) = scripted_backend();
        for _ in 0..2 {
            probes
                .send(test_reconciliation(
                    RconProbeClassification::Refused,
                    BackendProbeClassification::Refused,
                ))
                .expect("initial clean probe result should queue");
        }
        let probe_gate = Arc::clone(
            backend
                .scripted_probes
                .as_ref()
                .expect("test backend should expose its scripted probe gate"),
        );
        let (wake_root, wake_sender, wake_receiver) = wake_latch();
        let (mutations, mutation_receiver) = mpsc::channel(MUTATION_QUEUE_CAPACITY);
        let (quiescence, mut quiescence_receiver) = mpsc::channel(1);
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let (lifecycle_sender, mut lifecycle) = watch::channel(LifecycleState::reconciling());
        let (fatal, _fatal_receiver) = FatalSignal::new();
        let (runtime_drained_sender, runtime_drained) = watch::channel(false);
        wake_sender
            .enqueue(WakeRequest::observed(*lifecycle.borrow()))
            .expect("initial wake should queue");
        let task = tokio::spawn(run(
            wake_receiver,
            mutation_receiver,
            quiescence,
            fatal,
            runtime_drained,
            shutdown_receiver,
            lifecycle_sender,
            config,
            backend,
            test_backend_use(),
        ));

        let (old_signal, _) = timeout(Duration::from_secs(5), launches.accept())
            .await
            .expect("old process launch should be bounded")
            .expect("old process should report its launch");
        wait_for_phase(&mut lifecycle, ServerPhase::Starting).await;
        let (restart, restart_reply) = test_mutation(
            MutationOperation::Restart,
            "96969696969696969696969696969696",
            0,
            Arc::new(AdmissionGate::pending()),
        );
        mutations
            .send(restart)
            .await
            .expect("restart should reach the supervisor");
        let admitted = restart_reply
            .await
            .expect("restart should receive a reply")
            .expect("restart should be admitted");
        assert_eq!(admitted.command_revision, 1);
        assert_eq!(admitted.operation, MutationOperation::Restart);
        wait_for_phase(&mut lifecycle, ServerPhase::Stopping).await;

        let request = timeout(Duration::from_secs(5), quiescence_receiver.recv())
            .await
            .expect("quiescence request should be bounded")
            .expect("restart should request runtime quiescence");
        request
            .acknowledged
            .send(Ok(()))
            .expect("supervisor should await quiescence acknowledgement");
        tokio::task::yield_now().await;

        Self {
            wake_root,
            wake_sender,
            mutations,
            shutdown_sender: Some(shutdown_sender),
            lifecycle,
            task,
            probes,
            probe_gate,
            launches,
            old_signal: Some(old_signal),
            _runtime_drained: runtime_drained_sender,
        }
    }

    async fn release_old_and_wait_for_final_gate(&mut self) {
        release_signaled_process(
            self.old_signal
                .take()
                .expect("old process should still be held"),
        )
        .await;
        wait_for_phase(&mut self.lifecycle, ServerPhase::Reconciling).await;
    }

    fn request_shutdown(&mut self) {
        self.shutdown_sender
            .take()
            .expect("shutdown should be requested once")
            .send(())
            .expect("supervisor should still await shutdown");
    }

    async fn join(self) -> (watch::Receiver<LifecycleState>, TcpListener) {
        timeout(Duration::from_secs(10), self.task)
            .await
            .expect("supervisor shutdown should be bounded")
            .expect("supervisor task should not panic")
            .expect("supervisor should finish cleanly");
        (self.lifecycle, self.launches)
    }
}

async fn release_signaled_process(mut signal: TcpStream) {
    signal
        .write_all(&[1])
        .await
        .expect("test process should receive its exit signal");
    let result = timeout(Duration::from_secs(5), signal.read_to_end(&mut Vec::new()))
        .await
        .expect("test process exit should be bounded");
    if let Err(error) = result {
        assert_eq!(
            error.kind(),
            ErrorKind::ConnectionReset,
            "test process should close its exit signal"
        );
    }
}

fn assert_no_pending_test_launch(listener: &TcpListener) {
    let mut accept = Box::pin(listener.accept());
    let waker = std::task::Waker::noop();
    let mut context = std::task::Context::from_waker(waker);
    assert!(matches!(
        std::future::Future::poll(accept.as_mut(), &mut context),
        std::task::Poll::Pending
    ));
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
async fn exact_last_admitted_tuple_replays_without_revision_or_action() {
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
    assert_eq!(
        control.handle_mutation(replay, &config),
        MutationEffect::None
    );
    assert_eq!(replay_reply.await.unwrap().unwrap(), success);
    assert_eq!(lifecycle.borrow().command_revision, 1);
    assert!(!replay_gate.is_admitted());
}

#[tokio::test]
async fn conflicting_reuse_of_last_admitted_request_id_is_rejected() {
    let (mut control, _wake_sender, _shutdown_sender, lifecycle) =
        test_control(LifecycleState::stopped());
    let config = test_config(&Endpoint::new("127.0.0.1", 9));
    let (first, first_reply) = test_mutation(
        MutationOperation::Start,
        "11111111111111111111111111111111",
        0,
        Arc::new(AdmissionGate::pending()),
    );
    control.handle_mutation(first, &config);
    first_reply.await.unwrap().unwrap();

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
async fn older_exact_tuple_is_stale_after_an_intervening_admission() {
    let (mut control, _wake_sender, _shutdown_sender, lifecycle) =
        test_control(LifecycleState::stopped());
    let config = test_config(&Endpoint::new("127.0.0.1", 9));
    let (first, first_reply) = test_mutation(
        MutationOperation::Start,
        "11111111111111111111111111111111",
        0,
        Arc::new(AdmissionGate::pending()),
    );
    control.handle_mutation(first, &config);
    first_reply.await.unwrap().unwrap();

    let (intervening, intervening_reply) = test_mutation(
        MutationOperation::Stop,
        "22222222222222222222222222222222",
        1,
        Arc::new(AdmissionGate::pending()),
    );
    control.handle_mutation(intervening, &config);
    assert_eq!(
        intervening_reply.await.unwrap().unwrap().command_revision,
        2
    );
    assert!(!control.launch_demand);

    let (old_retry, old_retry_reply) = test_mutation(
        MutationOperation::Start,
        "11111111111111111111111111111111",
        0,
        Arc::new(AdmissionGate::pending()),
    );
    assert_eq!(
        control.handle_mutation(old_retry, &config),
        MutationEffect::None
    );
    assert_eq!(
        old_retry_reply.await.unwrap(),
        Err(MutationError::StaleCommandRevision)
    );
    assert_eq!(lifecycle.borrow().command_revision, 2);
    assert!(!control.launch_demand);
}

#[tokio::test]
async fn running_and_starting_restart_admit_once_and_publish_explicit_restart() {
    for phase in [ServerPhase::Starting, ServerPhase::Running] {
        let initial = failed_lifecycle(phase, 2);
        let (mut control, _, _, lifecycle) = test_control(initial);
        let config = test_config(&Endpoint::new("127.0.0.1", 9));
        let (restart, admission_reply) = test_mutation(
            MutationOperation::Restart,
            "abababababababababababababababab",
            0,
            Arc::new(AdmissionGate::pending()),
        );
        assert_eq!(
            control.handle_mutation(restart, &config),
            MutationEffect::StopOwned
        );
        let success = admission_reply.await.unwrap().unwrap();
        assert_eq!(success.operation, MutationOperation::Restart);
        assert_eq!(success.command_revision, 1);
        assert_eq!(success.disposition, MutationDisposition::Accepted);
        let stopping = *lifecycle.borrow();
        assert_eq!(stopping.phase, ServerPhase::Stopping);
        assert_eq!(stopping.failure, initial.failure);
        assert_eq!(stopping.failure_streak, initial.failure_streak);
        assert_eq!(
            control.stopping_reason,
            Some(StoppingReason::ExplicitRestart)
        );
        assert!(control.launch_demand);

        let (duplicate, duplicate_reply) = test_mutation(
            MutationOperation::Restart,
            "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd",
            1,
            Arc::new(AdmissionGate::pending()),
        );
        control.handle_mutation(duplicate, &config);
        assert_eq!(
            duplicate_reply.await.unwrap(),
            Err(MutationError::OperationInProgress)
        );
        assert_eq!(lifecycle.borrow().command_revision, 1);

        let (replay, replay_reply) = test_mutation(
            MutationOperation::Restart,
            "abababababababababababababababab",
            0,
            Arc::new(AdmissionGate::pending()),
        );
        control.handle_mutation(replay, &config);
        assert_eq!(replay_reply.await.unwrap().unwrap(), success);
        assert_eq!(lifecycle.borrow().command_revision, 1);
    }
}

#[tokio::test]
async fn cancelled_restart_never_publishes_but_post_admission_cancellation_cannot_undo_it() {
    let (mut control, _, _, lifecycle) =
        test_control(LifecycleState::test_phase(ServerPhase::Running));
    let config = test_config(&Endpoint::new("127.0.0.1", 9));
    let cancelled_gate = Arc::new(AdmissionGate::pending());
    assert!(cancelled_gate.cancel());
    let (cancelled, cancelled_reply) = test_mutation(
        MutationOperation::Restart,
        "dededededededededededededededede",
        0,
        cancelled_gate,
    );
    control.handle_mutation(cancelled, &config);
    assert!(cancelled_reply.await.is_err());
    assert_eq!(lifecycle.borrow().command_revision, 0);

    let admitted_gate = Arc::new(AdmissionGate::pending());
    let (admitted, admitted_reply) = test_mutation(
        MutationOperation::Restart,
        "efefefefefefefefefefefefefefefef",
        0,
        Arc::clone(&admitted_gate),
    );
    assert_eq!(
        control.handle_mutation(admitted, &config),
        MutationEffect::StopOwned
    );
    assert!(!admitted_gate.cancel());
    assert_eq!(admitted_reply.await.unwrap().unwrap().command_revision, 1);
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
            StoppingReason::ExplicitStop,
            Err(MutationError::OperationInProgress),
        ),
        (
            StoppingReason::ExplicitRestart,
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
            Some(StoppingReason::ExplicitStop),
            Ok(MutationDisposition::Accepted),
        ),
        (
            StoppingReason::FailureCleanup,
            Some(StoppingReason::FailureCleanup),
            Ok(MutationDisposition::AlreadySatisfied),
        ),
        (
            StoppingReason::ExplicitStop,
            Some(StoppingReason::ExplicitStop),
            Err(MutationError::OperationInProgress),
        ),
        (
            StoppingReason::ExplicitRestart,
            Some(StoppingReason::ExplicitStop),
            Ok(MutationDisposition::Accepted),
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
        let result = control.plan_mutation(MutationOperation::Stop, control.current_lifecycle(), 1);
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
async fn restart_phase_table_covers_every_phase_and_stopping_reason() {
    for (phase, expected) in [
        (
            ServerPhase::Reconciling,
            Err(MutationError::OperationRejected),
        ),
        (ServerPhase::Stopped, Err(MutationError::OperationRejected)),
        (ServerPhase::Starting, Ok(MutationDisposition::Accepted)),
        (ServerPhase::Running, Ok(MutationDisposition::Accepted)),
        (ServerPhase::Stopping, Ok(MutationDisposition::Accepted)),
        (ServerPhase::Cooldown, Err(MutationError::OperationRejected)),
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
            .plan_mutation(MutationOperation::Restart, control.current_lifecycle(), 1)
            .map(|plan| plan.disposition);
        assert_eq!(result, expected, "unexpected restart policy for {phase:?}");
    }

    let (mut final_gate, _, _, _) =
        test_control(LifecycleState::test_phase(ServerPhase::Reconciling));
    final_gate.reconciliation_mode = ReconciliationMode::FinalGate;
    assert!(matches!(
        final_gate.plan_mutation(
            MutationOperation::Restart,
            final_gate.current_lifecycle(),
            1,
        ),
        Err(MutationError::OperationInProgress)
    ));

    for (reason, launch_pending, expected) in [
        (
            StoppingReason::AutomaticIdle,
            false,
            Ok(MutationDisposition::Accepted),
        ),
        (
            StoppingReason::AutomaticIdle,
            true,
            Err(MutationError::OperationInProgress),
        ),
        (
            StoppingReason::FailureCleanup,
            false,
            Err(MutationError::OperationRejected),
        ),
        (
            StoppingReason::ExplicitStop,
            false,
            Err(MutationError::OperationInProgress),
        ),
        (
            StoppingReason::ExplicitRestart,
            true,
            Err(MutationError::OperationInProgress),
        ),
        (
            StoppingReason::DaemonShutdown,
            false,
            Err(MutationError::OperationRejected),
        ),
    ] {
        let (mut control, _, _, _) =
            test_control(LifecycleState::test_phase(ServerPhase::Stopping));
        control.stopping_reason = Some(reason);
        control.launch_demand = launch_pending;
        let result = control
            .plan_mutation(MutationOperation::Restart, control.current_lifecycle(), 1)
            .map(|plan| plan.disposition);
        assert_eq!(result, expected, "unexpected restart policy for {reason:?}");
    }

    let (mut control, _, _, _) = test_control(LifecycleState::test_phase(ServerPhase::Stopping));
    control.stopping_reason = None;
    assert!(matches!(
        control.plan_mutation(MutationOperation::Restart, control.current_lifecycle(), 1,),
        Err(MutationError::OperationRejected)
    ));
}

#[tokio::test]
async fn restart_during_explicit_stop_is_in_progress_without_side_effects() {
    let initial = lifecycle_with_generation(ServerPhase::Stopping, 3);
    let (mut control, _, _, lifecycle) = test_control(initial);
    control.stopping_reason = Some(StoppingReason::ExplicitStop);
    control.launch_demand = false;
    let config = test_config(&Endpoint::new("127.0.0.1", 9));
    let gate = Arc::new(AdmissionGate::pending());
    let (restart, reply) = test_mutation(
        MutationOperation::Restart,
        "95959595959595959595959595959595",
        initial.command_revision,
        Arc::clone(&gate),
    );

    assert_eq!(
        control.handle_mutation(restart, &config),
        MutationEffect::None
    );
    assert_eq!(
        reply.await.unwrap(),
        Err(MutationError::OperationInProgress)
    );
    assert!(!gate.is_admitted());
    assert_eq!(*lifecycle.borrow(), initial);
    assert_eq!(control.stopping_reason, Some(StoppingReason::ExplicitStop));
    assert!(!control.launch_demand);
    assert!(control.last_admission.is_none());
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
    control.stopping_reason = Some(StoppingReason::ExplicitStop);
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
    control.stopping_reason = Some(StoppingReason::ExplicitStop);
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
    control.stopping_reason = Some(StoppingReason::ExplicitStop);
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
    let (_rcon_address, events, rcon_server) = spawn_scripted_list_rcon_server(Vec::new()).await;

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
    let mut control = test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);
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

async fn wait_for_phase(lifecycle: &mut watch::Receiver<LifecycleState>, expected: ServerPhase) {
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
        AcceptedFailure, Authenticated, Inconclusive as RconInconclusive, Refused as RconRefused,
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
        control.stopping_reason = Some(StoppingReason::ExplicitStop);
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
            wait_for_explicit_cleanup(ready_cleanup(Ok(())), &mut control, &config, false).await;
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
    control.stopping_reason = Some(StoppingReason::ExplicitStop);
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

#[tokio::test]
async fn stop_cancels_restart_only_while_explicit_cleanup_is_active() {
    let initial = failed_lifecycle(ServerPhase::Stopping, 2);
    let (mut control, mutations, _wake_sender, _shutdown_sender, lifecycle) =
        test_control_with_mutations(initial);
    control.stopping_reason = Some(StoppingReason::ExplicitRestart);
    control.launch_demand = true;
    let config = test_config(&Endpoint::new("127.0.0.1", 9));
    let (release_cleanup, cleanup_released) = oneshot::channel();
    let cleanup = tokio::spawn(async move {
        let (result, shutdown) = wait_for_explicit_cleanup(
            async {
                cleanup_released.await.unwrap();
                Ok(())
            },
            &mut control,
            &config,
            false,
        )
        .await;
        result.unwrap();
        assert!(!shutdown);
        assert_eq!(control.stopping_reason, Some(StoppingReason::ExplicitStop));
        assert!(!control.launch_demand);
        control.publish_clean_stopped_after_reap();
        control
    });

    let (stop, stop_reply) = test_mutation(
        MutationOperation::Stop,
        "91919191919191919191919191919191",
        0,
        Arc::new(AdmissionGate::pending()),
    );
    mutations.send(stop).await.unwrap();
    let success = stop_reply.await.unwrap().unwrap();
    assert_eq!(success.disposition, MutationDisposition::Accepted);
    assert_eq!(success.command_revision, 1);
    assert_eq!(lifecycle.borrow().phase, ServerPhase::Stopping);
    release_cleanup.send(()).unwrap();
    let control = cleanup.await.unwrap();
    let stopped = *lifecycle.borrow();
    assert_eq!(stopped.phase, ServerPhase::Stopped);
    assert_eq!(stopped.command_revision, 1);
    assert_eq!(stopped.failure, None);
    assert_eq!(stopped.failure_streak, 0);
    assert!(!control.launch_demand);
}

#[tokio::test]
async fn cleanup_completion_precedes_stop_and_final_gate_prevents_retroactive_cancellation() {
    let initial = failed_lifecycle(ServerPhase::Stopping, 2);
    let (mut control, mutations, _wake_sender, _shutdown_sender, lifecycle) =
        test_control_with_mutations(initial);
    control.stopping_reason = Some(StoppingReason::ExplicitRestart);
    control.launch_demand = true;
    let config = test_config(&Endpoint::new("127.0.0.1", 9));
    let (stop, mut stop_reply) = test_mutation(
        MutationOperation::Stop,
        "92929292929292929292929292929292",
        0,
        Arc::new(AdmissionGate::pending()),
    );
    mutations.send(stop).await.unwrap();

    let (result, shutdown) =
        wait_for_explicit_cleanup(ready_cleanup(Ok(())), &mut control, &config, false).await;
    result.unwrap();
    assert!(!shutdown);
    assert!(matches!(
        stop_reply.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    control.launch_demand = false;
    control.publish_reaped_for_restart();
    control.begin_reconciliation();
    process_queued_mutation(&mut control, &config).await;
    assert_eq!(
        stop_reply.await.unwrap(),
        Err(MutationError::OperationInProgress)
    );
    assert_eq!(lifecycle.borrow().phase, ServerPhase::Reconciling);
    assert_eq!(lifecycle.borrow().command_revision, 0);
    assert_eq!(lifecycle.borrow().failure, initial.failure);
    assert_eq!(lifecycle.borrow().failure_streak, initial.failure_streak);
}

#[tokio::test]
async fn restart_reap_reconciliation_starting_and_running_preserve_failure_history() {
    let mut initial = failed_lifecycle(ServerPhase::Stopping, 3);
    initial.command_revision = 7;
    initial.retry_at = Some(Instant::now() + Duration::from_secs(5));
    let (mut control, _, _, lifecycle) = test_control(initial);
    control.stopping_reason = Some(StoppingReason::ExplicitRestart);
    control.publish_reaped_for_restart();
    let reaped = *lifecycle.borrow();
    assert_eq!(reaped.phase, ServerPhase::Stopped);
    assert_eq!(reaped.command_revision, 7);
    assert_eq!(reaped.launch_generation, initial.launch_generation);
    assert_eq!(reaped.failure, initial.failure);
    assert_eq!(reaped.failure_streak, 3);
    assert!(reaped.retry_at.is_none());

    control.begin_reconciliation();
    assert_eq!(lifecycle.borrow().failure_streak, 3);
    control.record_launch_success(initial.launch_generation + 1);
    assert_eq!(lifecycle.borrow().phase, ServerPhase::Starting);
    assert_eq!(lifecycle.borrow().failure_streak, 3);
    let backend_use = test_backend_use();
    control.publish_running(&backend_use);
    assert_eq!(lifecycle.borrow().phase, ServerPhase::Running);
    assert_eq!(lifecycle.borrow().failure, initial.failure);
    assert_eq!(lifecycle.borrow().failure_streak, 3);
    control.reset_failure_streak();
    assert_eq!(lifecycle.borrow().failure, None);
    assert_eq!(lifecycle.borrow().failure_streak, 0);
}

#[tokio::test]
async fn replacement_failures_escalate_the_retained_restart_streak() {
    let retained_streak = 3;

    let reconciling = failed_lifecycle(ServerPhase::Reconciling, retained_streak);
    let (control, _, _, lifecycle) = test_control(reconciling);
    control.record_launch_failure(reconciling.launch_generation + 1);
    assert_eq!(lifecycle.borrow().phase, ServerPhase::Cooldown);
    assert_eq!(
        lifecycle.borrow().failure,
        Some(FailureCategory::LaunchFailed)
    );
    assert_eq!(lifecycle.borrow().failure_streak, retained_streak + 1);

    let starting = failed_lifecycle(ServerPhase::Starting, retained_streak);
    let (mut control, _, _, lifecycle) = test_control(starting);
    control.begin_failure_cleanup(FailureCategory::StartupTimedOut);
    assert_eq!(lifecycle.borrow().phase, ServerPhase::Stopping);
    assert_eq!(
        lifecycle.borrow().failure,
        Some(FailureCategory::StartupTimedOut)
    );
    assert_eq!(lifecycle.borrow().failure_streak, retained_streak + 1);

    let starting = failed_lifecycle(ServerPhase::Starting, retained_streak);
    let (control, _, _, lifecycle) = test_control(starting);
    control.record_reaped_failure(FailureCategory::ExitedBeforeReady);
    assert_eq!(lifecycle.borrow().phase, ServerPhase::Cooldown);
    assert_eq!(
        lifecycle.borrow().failure,
        Some(FailureCategory::ExitedBeforeReady)
    );
    assert_eq!(lifecycle.borrow().failure_streak, retained_streak + 1);
}

#[tokio::test]
async fn composed_restart_reaps_before_final_reconciliation_and_launches_once() {
    let mut supervisor = HeldRestartSupervisor::spawn().await;
    assert_eq!(supervisor.lifecycle.borrow().phase, ServerPhase::Stopping);
    assert_eq!(supervisor.lifecycle.borrow().launch_generation, 1);
    assert_no_pending_test_launch(&supervisor.launches);

    supervisor.release_old_and_wait_for_final_gate().await;
    assert_eq!(supervisor.lifecycle.borrow().launch_generation, 1);
    assert_no_pending_test_launch(&supervisor.launches);
    supervisor
        .probes
        .send(test_reconciliation(
            RconProbeClassification::Refused,
            BackendProbeClassification::Refused,
        ))
        .expect("fresh final clean result should reach the supervisor");

    let (replacement_signal, _) = timeout(Duration::from_secs(5), supervisor.launches.accept())
        .await
        .expect("replacement launch should be bounded")
        .expect("clean final reconciliation should launch one replacement");
    wait_for_phase(&mut supervisor.lifecycle, ServerPhase::Starting).await;
    let replacement = *supervisor.lifecycle.borrow();
    assert_eq!(replacement.command_revision, 1);
    assert_eq!(replacement.launch_generation, 2);

    for _ in 0..32 {
        supervisor
            .wake_sender
            .enqueue(WakeRequest::observed(replacement))
            .expect("duplicate wake should reach the supervisor");
    }
    supervisor
        .wake_sender
        .wait_until_empty()
        .await
        .expect("supervisor should drain duplicate wakes");
    let (replay, replay_reply) = test_mutation(
        MutationOperation::Restart,
        "96969696969696969696969696969696",
        0,
        Arc::new(AdmissionGate::pending()),
    );
    supervisor
        .mutations
        .send(replay)
        .await
        .expect("exact retry should reach the supervisor");
    assert_eq!(
        replay_reply
            .await
            .expect("exact retry should receive a reply")
            .expect("exact retry should replay admission")
            .command_revision,
        1
    );
    assert_eq!(supervisor.lifecycle.borrow().launch_generation, 2);
    assert_no_pending_test_launch(&supervisor.launches);

    supervisor.request_shutdown();
    wait_for_phase(&mut supervisor.lifecycle, ServerPhase::Stopping).await;
    release_signaled_process(replacement_signal).await;
    supervisor.wake_root.close();
    let (lifecycle, launches) = supervisor.join().await;
    assert_eq!(lifecycle.borrow().phase, ServerPhase::Stopped);
    assert_eq!(lifecycle.borrow().launch_generation, 2);
    assert_no_pending_test_launch(&launches);
}

#[tokio::test]
async fn composed_restart_final_evidence_blocks_replacement_launch() {
    for (result, expected_phase) in [
        (
            test_reconciliation(
                RconProbeClassification::Authenticated,
                BackendProbeClassification::Connected,
            ),
            ServerPhase::External,
        ),
        (
            test_reconciliation(
                RconProbeClassification::AcceptedFailure,
                BackendProbeClassification::Refused,
            ),
            ServerPhase::Conflict,
        ),
        (
            test_reconciliation(
                RconProbeClassification::Refused,
                BackendProbeClassification::Inconclusive,
            ),
            ServerPhase::Conflict,
        ),
    ] {
        let mut supervisor = HeldRestartSupervisor::spawn().await;
        tokio::task::yield_now().await;
        assert!(supervisor.probe_gate.try_lock().is_ok());
        supervisor
            .probes
            .send(result)
            .expect("final evidence should reach the supervisor");
        tokio::task::yield_now().await;
        assert_eq!(supervisor.lifecycle.borrow().phase, ServerPhase::Stopping);
        assert_no_pending_test_launch(&supervisor.launches);
        release_signaled_process(
            supervisor
                .old_signal
                .take()
                .expect("old process should still be held"),
        )
        .await;
        wait_for_phase(&mut supervisor.lifecycle, expected_phase).await;
        assert_no_pending_test_launch(&supervisor.launches);

        supervisor.request_shutdown();
        supervisor.wake_root.close();
        let (lifecycle, launches) = supervisor.join().await;
        assert_eq!(lifecycle.borrow().phase, expected_phase);
        assert_no_pending_test_launch(&launches);
    }
}

#[tokio::test]
async fn composed_restart_shutdown_at_cleanup_or_final_gate_suppresses_relaunch() {
    for shutdown_during_cleanup in [true, false] {
        let mut supervisor = HeldRestartSupervisor::spawn().await;
        if shutdown_during_cleanup {
            supervisor.request_shutdown();
            release_signaled_process(
                supervisor
                    .old_signal
                    .take()
                    .expect("old process should still be held"),
            )
            .await;
        } else {
            supervisor.release_old_and_wait_for_final_gate().await;
            supervisor.request_shutdown();
        }
        let (lifecycle, launches) = supervisor.join().await;
        assert_eq!(lifecycle.borrow().launch_generation, 1);
        assert_no_pending_test_launch(&launches);
    }
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
async fn automatic_idle_restart_is_admitted_before_but_rejected_after_cleanup_completion() {
    let running = lifecycle_with_generation(ServerPhase::Running, 1);
    let (mut control, mutations, _wake_sender, _shutdown_sender, lifecycle) =
        test_control_with_mutations(running);
    control.begin_idle_stop(running.owned_running_cycle().unwrap());
    let config = test_config(&Endpoint::new("127.0.0.1", 9));
    let (release_cleanup, cleanup_released) = oneshot::channel();
    let cleanup = tokio::spawn(async move {
        let outcome = wait_for_idle_cleanup(
            async {
                cleanup_released.await.unwrap();
                Ok(())
            },
            &mut control,
            &config,
        )
        .await;
        (outcome, control)
    });
    let (restart, restart_reply) = test_mutation(
        MutationOperation::Restart,
        "93939393939393939393939393939393",
        0,
        Arc::new(AdmissionGate::pending()),
    );
    mutations.send(restart).await.unwrap();
    assert_eq!(
        restart_reply.await.unwrap().unwrap().disposition,
        MutationDisposition::Accepted
    );
    assert_eq!(lifecycle.borrow().command_revision, 1);
    release_cleanup.send(()).unwrap();
    let ((result, restart, shutdown), mut control) = cleanup.await.unwrap();
    result.unwrap();
    assert!(restart);
    assert!(!shutdown);
    assert_eq!(
        control.stopping_reason,
        Some(StoppingReason::ExplicitRestart)
    );
    control.launch_demand = false;
    control.publish_reaped_for_restart();
    assert_eq!(lifecycle.borrow().phase, ServerPhase::Stopped);

    let running = lifecycle_with_generation(ServerPhase::Running, 1);
    let (mut control, mutations, _wake_sender, _shutdown_sender, lifecycle) =
        test_control_with_mutations(running);
    control.begin_idle_stop(running.owned_running_cycle().unwrap());
    let config = test_config(&Endpoint::new("127.0.0.1", 9));
    let (restart, mut restart_reply) = test_mutation(
        MutationOperation::Restart,
        "94949494949494949494949494949494",
        0,
        Arc::new(AdmissionGate::pending()),
    );
    mutations.send(restart).await.unwrap();
    let (result, restart, shutdown) =
        wait_for_idle_cleanup(ready_cleanup(Ok(())), &mut control, &config).await;
    result.unwrap();
    assert!(!restart);
    assert!(!shutdown);
    assert!(matches!(
        restart_reply.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    control.set_phase(ServerPhase::Stopped);
    process_queued_mutation(&mut control, &config).await;
    assert_eq!(
        restart_reply.await.unwrap(),
        Err(MutationError::OperationRejected)
    );
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
            ErrorKind::UnexpectedEof | ErrorKind::ConnectionReset | ErrorKind::ConnectionAborted
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
        ReadinessOutcome::ExplicitMutation
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
async fn restart_precedes_ready_startup_timeout() {
    let mut config = no_rcon_config();
    config.startup_timeout = Duration::ZERO;
    let mut child = process::launch(&config.launch).unwrap();
    let starting = lifecycle_with_generation(ServerPhase::Starting, 1);
    let (mut control, mutations, _wake_sender, _shutdown_sender, lifecycle) =
        test_control_with_mutations(starting);
    let (restart, restart_reply) = test_mutation(
        MutationOperation::Restart,
        "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1",
        0,
        Arc::new(AdmissionGate::pending()),
    );
    mutations.send(restart).await.unwrap();

    assert!(matches!(
        wait_for_readiness(&mut child, &mut control, &config, &test_backend_endpoint()).await,
        ReadinessOutcome::ExplicitMutation
    ));
    assert_eq!(
        restart_reply.await.unwrap().unwrap().disposition,
        MutationDisposition::Accepted
    );
    assert_eq!(lifecycle.borrow().phase, ServerPhase::Stopping);
    assert_eq!(lifecycle.borrow().command_revision, 1);
    assert_eq!(
        control.stopping_reason,
        Some(StoppingReason::ExplicitRestart)
    );
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
async fn child_exit_precedes_ready_restart() {
    let mut config = rapid_exit_config(&Endpoint::new("127.0.0.1", 9));
    config.startup_timeout = Duration::ZERO;
    let mut child = process::launch(&config.launch).unwrap();
    let process_id = child.id().unwrap();
    wait_for_test_process_exit(process_id);
    let starting = lifecycle_with_generation(ServerPhase::Starting, 1);
    let (mut control, mutations, _wake_sender, _shutdown_sender, lifecycle) =
        test_control_with_mutations(starting);
    let gate = Arc::new(AdmissionGate::pending());
    let (restart, mut restart_reply) = test_mutation(
        MutationOperation::Restart,
        "b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1",
        0,
        Arc::clone(&gate),
    );
    mutations.send(restart).await.unwrap();

    assert!(matches!(
        wait_for_readiness(&mut child, &mut control, &config, &test_backend_endpoint()).await,
        ReadinessOutcome::Exited(_)
    ));
    assert_eq!(restart_reply.try_recv(), Err(TryRecvError::Empty));
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
    let mut control = test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);
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
    let mut control = test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);

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
    let mut control = test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);
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
    let mut control = test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);
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
    let mut control = test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);

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
    let mut control = test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);

    for (expected_streak, expected_delay) in [(2, 10), (3, 20)] {
        let (rcon_address, _first_poll, rcon_server) = spawn_player_count_rcon_server(1).await;
        let config = rapid_exit_config(&rcon_address);
        let child = process::launch(&config.launch).expect("short-lived test server should launch");
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
    let mut control = test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);
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
    let child = process::launch(&config.launch).expect("stable-window test server should launch");
    let (_wake_root, _wake_sender, wake_receiver) = wake_latch();
    let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
    let running = failed_lifecycle(ServerPhase::Running, 3);
    let (lifecycle_sender, mut lifecycle) = watch::channel(running);
    let mut control = test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);
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
        spawn_scripted_list_rcon_server(vec![player_count_reply(0), player_count_reply(0)]).await;

    let mut config = test_config(&rcon_address);
    config.idle_timeout = Duration::from_millis(30);
    config.poll_interval = Duration::from_secs(1);
    let mut child = process::launch(&config.launch).unwrap();
    let (_wake_root, wake_sender, wake_receiver) = wake_latch();
    let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
    let starting = lifecycle_with_generation(ServerPhase::Starting, 1);
    let (lifecycle_sender, lifecycle) = watch::channel(starting);
    let mut control = test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);
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
        spawn_scripted_list_rcon_server(vec![player_count_reply(0), player_count_reply(0)]).await;
    let mut config = test_config(&rcon_address);
    config.idle_timeout = Duration::from_secs(10);
    config.poll_interval = Duration::from_secs(100);
    let child = process::launch(&config.launch).expect("test server process should launch");
    let (_wake_root, wake_sender, wake_receiver) = wake_latch();
    let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
    let running = running_lifecycle(0);
    let (lifecycle_sender, mut lifecycle) = watch::channel(running);
    let mut control = test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);
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
    let mut control = test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);
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
    let (rcon_address, mut events, rcon_server) = spawn_scripted_list_rcon_server(Vec::new()).await;
    let mut config = test_config(&rcon_address);
    config.idle_timeout = Duration::from_secs(1);
    let mut child = process::launch(&config.launch).unwrap();
    let (_wake_root, wake_sender, wake_receiver) = wake_latch();
    let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
    let running = running_lifecycle(0);
    let (lifecycle_sender, lifecycle) = watch::channel(running);
    let mut control = test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);
    let backend_use = running_backend_use(running);
    let cycle = running.owned_running_cycle().unwrap();
    let mut state =
        RunningMonitorState::new(cycle, PlayerInspection::from_config(&config), running);
    let PlayerInspection::ConfiguredRcon { zero_anchor, .. } = &mut state.player_inspection else {
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
    let mut control = test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);
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
    let mut control = test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);
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
    let mut control = test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);
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
    let mut control = test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);
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

#[tokio::test(start_paused = true)]
async fn pre_stability_idle_stop_preserves_failure_history_through_reap_and_stopped() {
    let mut config = no_rcon_config();
    config.idle_timeout = Duration::from_secs(10);
    let child = process::launch(&config.launch).expect("test server process should launch");
    let (_wake_root, _wake_sender, wake_receiver) = wake_latch();
    let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
    let starting = failed_lifecycle(ServerPhase::Starting, 3);
    let (lifecycle_sender, mut lifecycle) = watch::channel(starting);
    let mut control = test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);
    let backend_use = test_backend_use();
    let cycle = control.publish_running(&backend_use);
    assert_retained_test_failure(*lifecycle.borrow(), ServerPhase::Running, 0, 1);

    let monitor = tokio::spawn(async move {
        let outcome = monitor_running_server(
            child,
            PlayerInspection::ProxyOnly,
            &mut control,
            &config,
            &backend_use,
            cycle,
        )
        .await;
        (outcome, control)
    });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(10)).await;
    wait_for_phase(&mut lifecycle, ServerPhase::Stopping).await;
    assert_retained_test_failure(*lifecycle.borrow(), ServerPhase::Stopping, 0, 1);

    tokio::time::resume();
    let (outcome, mut control) = monitor.await.expect("running monitor should not panic");
    assert_eq!(
        outcome.expect("automatic idle cleanup should succeed"),
        CycleOutcome::Stopped
    );
    assert_eq!(control.stopping_reason, Some(StoppingReason::AutomaticIdle));
    assert!(!control.launch_demand);
    assert_retained_test_failure(*lifecycle.borrow(), ServerPhase::Stopping, 0, 1);

    control.set_phase(ServerPhase::Stopped);
    assert_retained_test_failure(*lifecycle.borrow(), ServerPhase::Stopped, 0, 1);
}

#[tokio::test(start_paused = true)]
#[allow(
    clippy::too_many_lines,
    reason = "keeping the cleanup, final gate, and replacement sequence together makes the ordering proof explicit"
)]
async fn pre_stability_idle_restart_preserves_failure_history_through_replacement_running() {
    let exit_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test exit-signal listener should bind");
    let exit_signal_address = exit_listener
        .local_addr()
        .expect("test exit-signal listener should have an address");
    let backend_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test backend listener should bind");
    let backend_address = backend_listener
        .local_addr()
        .expect("test backend listener should have an address");
    let mut config = no_rcon_config();
    config.launch.arguments[2] = "supervisor::tests::signaled_exit_fixture".into();
    config
        .launch
        .arguments
        .insert(3, format!("exit-signal={exit_signal_address}").into());
    config.idle_timeout = Duration::from_secs(10);
    config.shutdown_timeout = Duration::from_secs(120);

    let child = process::launch(&config.launch).expect("test server process should launch");
    let (old_signal, _) = exit_listener
        .accept()
        .await
        .expect("old test process should report its launch");
    let starting = failed_lifecycle(ServerPhase::Starting, 3);
    let (mut control, mutations, _wake_sender, _shutdown_sender, mut lifecycle) =
        test_control_with_mutations(starting);
    let backend_use = test_backend_use();
    let cycle = control.publish_running(&backend_use);
    let monitor_backend_use = Arc::clone(&backend_use);
    let monitor = tokio::spawn(async move {
        let outcome = monitor_running_server(
            child,
            PlayerInspection::ProxyOnly,
            &mut control,
            &config,
            &monitor_backend_use,
            cycle,
        )
        .await;
        (outcome, control, config)
    });

    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(10)).await;
    wait_for_phase(&mut lifecycle, ServerPhase::Stopping).await;
    assert_retained_test_failure(*lifecycle.borrow(), ServerPhase::Stopping, 0, 1);

    let (restart, restart_reply) = test_mutation(
        MutationOperation::Restart,
        "97979797979797979797979797979797",
        0,
        Arc::new(AdmissionGate::pending()),
    );
    mutations
        .send(restart)
        .await
        .expect("restart should reach automatic-idle cleanup");
    let admission = restart_reply
        .await
        .expect("restart should receive a reply")
        .expect("restart should be admitted during cleanup");
    assert_eq!(admission.command_revision, 1);
    assert_eq!(admission.operation, MutationOperation::Restart);
    assert_retained_test_failure(*lifecycle.borrow(), ServerPhase::Stopping, 1, 1);

    tokio::time::resume();
    release_signaled_process(old_signal).await;
    let (outcome, mut control, config) = monitor.await.expect("running monitor should not panic");
    assert_eq!(
        outcome.expect("automatic idle restart cleanup should succeed"),
        CycleOutcome::Restart
    );
    assert_retained_test_failure(*lifecycle.borrow(), ServerPhase::Stopped, 1, 1);
    assert!(!control.launch_demand);

    let (probe_sender, probe_receiver) = mpsc::unbounded_channel();
    probe_sender
        .send(test_reconciliation(
            RconProbeClassification::Refused,
            BackendProbeClassification::Refused,
        ))
        .expect("clean final reconciliation should queue");
    let backend = BackendEndpoint {
        endpoint: endpoint(backend_address),
        connect_timeout: Duration::from_secs(1),
        scripted_probes: Some(Arc::new(tokio::sync::Mutex::new(probe_receiver))),
    };
    let mut last_conflict = None;
    assert!(matches!(
        reconcile_final_gate(&mut control, &config, &backend, &mut last_conflict).await,
        FinalGateOutcome::Launch
    ));
    assert_retained_test_failure(*lifecycle.borrow(), ServerPhase::Reconciling, 1, 1);
    assert!(!control.launch_demand);

    let next_generation = control
        .next_launch_generation()
        .expect("replacement generation should be available");
    let mut replacement =
        process::launch(&config.launch).expect("replacement process should launch");
    let (replacement_signal, _) = exit_listener
        .accept()
        .await
        .expect("replacement process should report its launch");
    control.record_launch_success(next_generation);
    assert_retained_test_failure(*lifecycle.borrow(), ServerPhase::Starting, 1, 2);
    assert!(matches!(
        wait_for_readiness(&mut replacement, &mut control, &config, &backend).await,
        ReadinessOutcome::Ready
    ));
    control.publish_running(&backend_use);
    assert_retained_test_failure(*lifecycle.borrow(), ServerPhase::Running, 1, 2);
    assert!(!control.launch_demand);

    release_signaled_process(replacement_signal).await;
    timeout(Duration::from_secs(5), replacement.wait())
        .await
        .expect("replacement reap should be bounded")
        .expect("replacement process should be reaped");
}

#[tokio::test(start_paused = true)]
async fn real_stability_window_clears_history_before_a_later_idle_stop() {
    let mut config = no_rcon_config();
    config.idle_timeout = STABILITY_WINDOW + Duration::from_secs(1);
    let child = process::launch(&config.launch).expect("test server process should launch");
    let (_wake_root, _wake_sender, wake_receiver) = wake_latch();
    let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
    let starting = failed_lifecycle(ServerPhase::Starting, 3);
    let (lifecycle_sender, mut lifecycle) = watch::channel(starting);
    let mut control = test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);
    let backend_use = test_backend_use();
    let cycle = control.publish_running(&backend_use);
    let monitor = tokio::spawn(async move {
        let outcome = monitor_running_server(
            child,
            PlayerInspection::ProxyOnly,
            &mut control,
            &config,
            &backend_use,
            cycle,
        )
        .await;
        (outcome, control)
    });

    tokio::task::yield_now().await;
    tokio::time::advance(STABILITY_WINDOW).await;
    wait_for_lifecycle(&mut lifecycle, |state| state.failure_streak == 0).await;
    let stable = *lifecycle.borrow();
    assert_eq!(stable.phase, ServerPhase::Running);
    assert_eq!(stable.failure, None);
    assert_eq!(stable.failure_streak, 0);
    assert_eq!(stable.command_revision, 0);
    assert_eq!(stable.launch_generation, 1);

    tokio::time::advance(Duration::from_secs(1)).await;
    wait_for_phase(&mut lifecycle, ServerPhase::Stopping).await;
    let stopping = *lifecycle.borrow();
    assert_eq!(stopping.failure, None);
    assert_eq!(stopping.failure_streak, 0);
    assert_eq!(stopping.command_revision, 0);
    assert_eq!(stopping.launch_generation, 1);

    tokio::time::resume();
    let (outcome, mut control) = monitor.await.expect("running monitor should not panic");
    assert_eq!(
        outcome.expect("automatic idle cleanup should succeed"),
        CycleOutcome::Stopped
    );
    assert!(!control.launch_demand);
    control.set_phase(ServerPhase::Stopped);
    let stopped = *lifecycle.borrow();
    assert_eq!(stopped.failure, None);
    assert_eq!(stopped.failure_streak, 0);
    assert_eq!(stopped.command_revision, 0);
    assert_eq!(stopped.launch_generation, 1);
}

#[tokio::test]
async fn wait_error_is_fatal_after_cleanup() {
    let config = test_config(&Endpoint::new("127.0.0.1", 9));
    let child = process::launch(&config.launch).expect("test server process should launch");
    let (_wake_root, _wake_sender, wake_receiver) = wake_latch();
    let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
    let running = lifecycle_with_generation(ServerPhase::Running, 1);
    let (lifecycle_sender, lifecycle) = watch::channel(running);
    let mut control = test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);

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
    let mut control = test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);
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
    let mut control = test_control_from_parts(wake_receiver, shutdown_receiver, lifecycle_sender);
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
    assert_eq!(lifecycle.borrow().failure_streak, 3);
    assert_eq!(lifecycle.borrow().failure, active.failure);
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
    assert_eq!(lifecycle.borrow().failure_streak, 3);
    assert_eq!(lifecycle.borrow().failure, active.failure);
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
