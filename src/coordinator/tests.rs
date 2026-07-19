use std::fs;
use std::net::{Ipv4Addr, TcpListener as StdTcpListener};
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

use super::*;

const TCP_NAMESPACE_ENV: &str = "MCSERVERNAP_TEST_TCP_NAMESPACE";

static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

fn temporary_directory() -> PathBuf {
    let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "mcservernap-coordinator-test-{}-{id}",
        std::process::id()
    ))
}

fn run_isolated_fixture(name: &str) {
    let namespace = crate::test_support::allocate_tcp_namespace();
    let output = Command::new(std::env::current_exe().expect("test executable should exist"))
        .args(["--ignored", "--exact", name, "--quiet"])
        .env(TCP_NAMESPACE_ENV, namespace.to_string())
        .output()
        .expect("isolated coordinator fixture should run");
    assert!(
        output.status.success(),
        "isolated coordinator fixture failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn isolated_public_listeners<const N: usize>() -> (Ipv4Addr, [StdTcpListener; N]) {
    // These fixtures deliberately release exact ports during ownership transfer. Give
    // each isolated child a coordinated loopback address and non-dynamic port window.
    let namespace = std::env::var(TCP_NAMESPACE_ENV)
        .expect("isolated coordinator fixture should receive a TCP namespace")
        .parse::<u8>()
        .expect("isolated coordinator TCP namespace should be valid");
    let address = crate::test_support::tcp_address(namespace);
    let addresses = crate::test_support::tcp_addresses::<N>(namespace);
    let listeners = addresses.map(|address| {
        StdTcpListener::bind(address).expect("isolated public listener reservation should bind")
    });
    (address, listeners)
}

fn server_table(
    id: &str,
    listener_address: Ipv4Addr,
    listener_port: u16,
    backend_port: u16,
    workdir: &str,
) -> String {
    format!(
        r#"
[servers.{id}]
minecraft_version = "26.2"

[servers.{id}.listener]
address = "{listener_address}"
port = {listener_port}

[servers.{id}.backend]
host = "127.0.0.1"
port = {backend_port}

[servers.{id}.command]
program = "java"
arguments = ["-jar", "server.jar"]
working_directory = "{workdir}"

[servers.{id}.timeouts]
poll_interval_seconds = 60
idle_timeout_seconds = 600
startup_timeout_seconds = 600
retry_interval_seconds = 2
command_timeout_seconds = 10
shutdown_timeout_seconds = 30
handshake_timeout_seconds = 5
proxy_connect_timeout_seconds = 10

[servers.{id}.motd]
text = "Sleeping"
color = "aqua"
bold = true

[servers.{id}.startup_message]
text = "Starting"
color = "light_purple"
bold = true
"#
    )
}

fn write_two_server_config(
    listener_address: Ipv4Addr,
    first_listener: u16,
    second_listener: u16,
    first_backend: u16,
    second_backend: u16,
) -> (PathBuf, PathBuf) {
    let directory = temporary_directory();
    fs::create_dir_all(directory.join("creative")).unwrap();
    fs::create_dir_all(directory.join("survival")).unwrap();
    let contents = format!(
        "schema_version = 3\nmax_connections = 1\n{}{}",
        server_table(
            "creative",
            listener_address,
            first_listener,
            first_backend,
            "creative"
        ),
        server_table(
            "survival",
            listener_address,
            second_listener,
            second_backend,
            "survival"
        )
    );
    let path = directory.join("cfg.toml");
    fs::write(&path, contents).unwrap();
    (directory, path)
}

fn write_three_server_config(
    listener_address: Ipv4Addr,
    listener_ports: [u16; 3],
    backend_ports: [u16; 3],
) -> (PathBuf, PathBuf) {
    let directory = temporary_directory();
    let servers = ["alpha", "beta", "gamma"];
    for server in servers {
        fs::create_dir_all(directory.join(server)).unwrap();
    }
    let contents = format!(
        "schema_version = 3\nmax_connections = 3\n{}{}{}",
        server_table(
            "alpha",
            listener_address,
            listener_ports[0],
            backend_ports[0],
            "alpha"
        ),
        server_table(
            "beta",
            listener_address,
            listener_ports[1],
            backend_ports[1],
            "beta"
        ),
        server_table(
            "gamma",
            listener_address,
            listener_ports[2],
            backend_ports[2],
            "gamma"
        )
    );
    let path = directory.join("cfg.toml");
    fs::write(&path, contents).unwrap();
    (directory, path)
}

fn pending_signal_watcher() -> SignalWatcher {
    SignalWatcher {
        task: Some(tokio::spawn(async {
            std::future::pending::<SignalWatcherExit>().await
        })),
    }
}

#[tokio::test]
async fn shared_shutdown_is_idempotent_and_retains_an_early_request() {
    let shutdown = Shutdown::new();
    assert!(shutdown.request());
    assert!(!shutdown.request());
    shutdown.wait().await;
}

#[tokio::test]
async fn control_task_panic_enters_global_fatal_shutdown() {
    let shutdown = Shutdown::new();
    let task = tokio::spawn(async {
        panic!("intentional control task panic");
        #[allow(unreachable_code)]
        Ok(())
    });
    record_control_result(task.await, false, &shutdown);
    assert!(shutdown.is_pending());
    let error = shutdown.finish().unwrap_err().to_string();
    assert!(error.contains("control accept loop panicked"));
}

#[test]
fn buffered_infrastructure_failure_is_drained_after_sender_drop() {
    let shutdown = Shutdown::new();
    let (sender, mut receiver) = mpsc::unbounded_channel();
    shutdown.fail(anyhow::anyhow!("buffered signal-listener failure"));
    sender.send(()).unwrap();
    drop(sender);
    let mut state = CoordinatorState::default();

    state.drain_infrastructure(&mut receiver);

    assert!(state.infrastructure_closed);
    assert!(shutdown.is_pending());
    assert!(
        shutdown
            .finish()
            .unwrap_err()
            .to_string()
            .contains("buffered signal-listener failure")
    );
}

#[tokio::test]
async fn watcher_panic_and_unexplained_closure_are_fatal() {
    for panic in [true, false] {
        let shutdown = Shutdown::new();
        let (sender, receiver) = mpsc::unbounded_channel();
        drop(sender);
        let task = if panic {
            tokio::spawn(async {
                panic!("intentional signal watcher panic");
            })
        } else {
            tokio::spawn(async { SignalWatcherExit::FailureReported })
        };
        let mut signals = SignalWatcher { task: Some(task) };
        while !signals.task.as_ref().unwrap().is_finished() {
            tokio::task::yield_now().await;
        }
        let bound = BoundDaemon {
            servers: Vec::new(),
            control: None,
            control_endpoint: None,
            runtime_overrides: VecDeque::new(),
        };

        let error = bound
            .activate_and_run(shutdown, receiver, &mut signals)
            .await
            .expect_err("unexplained watcher termination must be fatal");
        let message = error.to_string();
        assert!(message.contains("signal watcher"));
        assert!(message.contains(if panic {
            "panicked"
        } else {
            "without reporting"
        }));
    }
}

#[tokio::test]
async fn closed_watcher_channel_is_joined_before_shutdown_finishes() {
    let shutdown = Shutdown::new();
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let (closed_sender, closed_receiver) = oneshot::channel();
    let (release_sender, release_receiver) = oneshot::channel();
    let task = tokio::spawn(async move {
        drop(sender);
        closed_sender.send(()).unwrap();
        release_receiver.await.unwrap();
        SignalWatcherExit::FailureReported
    });
    closed_receiver.await.unwrap();
    assert_eq!(receiver.recv().await, None);
    let mut signals = SignalWatcher { task: Some(task) };
    let bound = BoundDaemon {
        servers: Vec::new(),
        control: None,
        control_endpoint: None,
        runtime_overrides: VecDeque::new(),
    };
    let mut run = Box::pin(bound.activate_and_run(shutdown, receiver, &mut signals));

    poll_fn(|context| match run.as_mut().poll(context) {
        Poll::Pending => Poll::Ready(()),
        Poll::Ready(_) => panic!("closed watcher channel must be joined before returning"),
    })
    .await;
    release_sender.send(()).unwrap();

    assert!(
        run.await
            .unwrap_err()
            .to_string()
            .contains("without reporting its cause")
    );
}

#[tokio::test]
async fn zero_activation_still_reports_buffered_watcher_failure() {
    let shutdown = Shutdown::new();
    let watcher_shutdown = shutdown.clone();
    let (sender, receiver) = mpsc::unbounded_channel();
    let task = tokio::spawn(async move {
        watcher_shutdown.fail(anyhow::anyhow!("fatal failure before activation"));
        sender.send(()).unwrap();
        SignalWatcherExit::FailureReported
    });
    let mut signals = SignalWatcher { task: Some(task) };
    while !signals.task.as_ref().unwrap().is_finished() {
        tokio::task::yield_now().await;
    }
    let bound = BoundDaemon {
        servers: Vec::new(),
        control: None,
        control_endpoint: None,
        runtime_overrides: VecDeque::new(),
    };

    let error = bound
        .activate_and_run(shutdown, receiver, &mut signals)
        .await
        .expect_err("fatal watcher failure must survive zero activation");
    assert!(
        error
            .to_string()
            .contains("fatal failure before activation")
    );
}

#[tokio::test]
async fn signal_watcher_acknowledges_registration_before_returning() {
    let shutdown = Shutdown::new();
    let (fatal_sender, mut fatal_receiver) = mpsc::unbounded_channel();
    let watcher = SignalWatcher::arm(shutdown.clone(), fatal_sender)
        .await
        .expect("platform signal listeners should register");
    assert!(!shutdown.is_pending());
    assert!(fatal_receiver.try_recv().is_err());
    drop(watcher);
}

#[tokio::test]
async fn one_shutdown_request_reaches_all_waiters_before_either_finishes() {
    let shutdown = Shutdown::new();
    let (started_sender, mut started_receiver) = mpsc::channel(2);
    let (release_sender, release_receiver) = watch::channel(false);
    let mut waiters = JoinSet::new();
    for _ in 0..2 {
        let shutdown = shutdown.clone();
        let started_sender = started_sender.clone();
        let mut release_receiver = release_receiver.clone();
        waiters.spawn(async move {
            shutdown.wait().await;
            started_sender.send(()).await.unwrap();
            release_receiver
                .wait_for(|released| *released)
                .await
                .unwrap();
        });
    }
    drop(started_sender);

    shutdown.request();
    started_receiver.recv().await.unwrap();
    started_receiver.recv().await.unwrap();
    assert_eq!(waiters.len(), 2);
    release_sender.send(true).unwrap();
    while waiters.join_next().await.is_some() {}
}

#[test]
fn client_panic_starts_other_runtime_cleanup_before_local_cleanup_finishes() {
    run_isolated_fixture(
        "coordinator::tests::client_panic_starts_other_runtime_cleanup_before_local_cleanup_finishes_fixture",
    );
}

#[tokio::test]
#[ignore = "runs through the parallel-safe public test wrapper"]
async fn client_panic_starts_other_runtime_cleanup_before_local_cleanup_finishes_fixture() {
    let (listener_address, [first_public, second_public]) = isolated_public_listeners();
    let first_backend = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let second_backend = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let first_address = first_public.local_addr().unwrap();
    let second_address = second_public.local_addr().unwrap();
    let (directory, path) = write_two_server_config(
        listener_address,
        first_address.port(),
        second_address.port(),
        first_backend.local_addr().unwrap().port(),
        second_backend.local_addr().unwrap().port(),
    );
    drop((first_public, second_public));

    let mut bound = DaemonCoordinator::prepare(&path)
        .unwrap()
        .bind_all()
        .await
        .unwrap();
    let (panic_sender, panic_receiver) = oneshot::channel();
    let (ready_sender, mut ready_receiver) = mpsc::unbounded_channel();
    let (cleanup_sender, mut cleanup_receiver) = mpsc::unbounded_channel();
    let (release_sender, release_receiver) = oneshot::channel();

    let first_ready = ready_sender.clone();
    let first_cleanup = cleanup_sender.clone();
    bound.runtime_overrides.push_back(Box::new(move |runtime| {
        runtime.spawn_client_task(async move {
            panic_receiver.await.unwrap();
            panic!("intentional coordinator client panic");
        });
        runtime.replace_supervisor(move |shutdown| async move {
            first_ready.send("creative").unwrap();
            shutdown.await.unwrap();
            first_cleanup.send("creative").unwrap();
            release_receiver.await.unwrap();
            Ok(())
        });
    }));

    bound.runtime_overrides.push_back(Box::new(move |runtime| {
        runtime.replace_supervisor(move |shutdown| async move {
            ready_sender.send("survival").unwrap();
            shutdown.await.unwrap();
            cleanup_sender.send("survival").unwrap();
            Ok(())
        });
    }));

    let shutdown = Shutdown::new();
    let (_fatal_sender, fatal_receiver) = mpsc::unbounded_channel();
    let daemon = tokio::spawn(async move {
        let mut signals = pending_signal_watcher();
        bound
            .activate_and_run(shutdown, fatal_receiver, &mut signals)
            .await
    });

    let mut ready = [
        ready_receiver.recv().await.unwrap(),
        ready_receiver.recv().await.unwrap(),
    ];
    ready.sort_unstable();
    assert_eq!(ready, ["creative", "survival"]);
    panic_sender.send(()).unwrap();

    let mut cleaning = [
        cleanup_receiver.recv().await.unwrap(),
        cleanup_receiver.recv().await.unwrap(),
    ];
    cleaning.sort_unstable();
    assert_eq!(cleaning, ["creative", "survival"]);
    assert!(!daemon.is_finished());
    release_sender.send(()).unwrap();

    let error = daemon
        .await
        .unwrap()
        .expect_err("a Minecraft client panic must fail the daemon");
    let message = format!("{error:#}");
    assert!(message.contains("server=creative"));
    assert!(message.contains("client task panicked"));

    StdTcpListener::bind(first_address).unwrap();
    StdTcpListener::bind(second_address).unwrap();
    fs::remove_dir_all(directory).ok();
}

#[test]
fn originating_fatal_remains_primary_while_runtime_cleanups_overlap() {
    run_isolated_fixture(
        "coordinator::tests::originating_fatal_remains_primary_while_runtime_cleanups_overlap_fixture",
    );
}

#[tokio::test]
#[ignore = "runs through the parallel-safe public test wrapper"]
async fn originating_fatal_remains_primary_while_runtime_cleanups_overlap_fixture() {
    let (listener_address, public) = isolated_public_listeners();
    let backend = [
        StdTcpListener::bind("127.0.0.1:0").unwrap(),
        StdTcpListener::bind("127.0.0.1:0").unwrap(),
        StdTcpListener::bind("127.0.0.1:0").unwrap(),
    ];
    let listener_ports = public
        .each_ref()
        .map(|listener| listener.local_addr().unwrap().port());
    let backend_ports = backend
        .each_ref()
        .map(|listener| listener.local_addr().unwrap().port());
    let (directory, path) =
        write_three_server_config(listener_address, listener_ports, backend_ports);
    drop(public);

    let mut bound = DaemonCoordinator::prepare(&path)
        .unwrap()
        .bind_all()
        .await
        .unwrap();
    let (fail_sender, fail_receiver) = oneshot::channel();
    bound.runtime_overrides.push_back(Box::new(move |runtime| {
        runtime.replace_supervisor(move |_| async move {
            fail_receiver.await.unwrap();
            Err(anyhow::anyhow!("originating supervisor failure"))
        });
    }));

    let (ready_sender, mut ready_receiver) = mpsc::unbounded_channel();
    let (cleanup_sender, mut cleanup_receiver) = mpsc::unbounded_channel();
    let (release_sender, release_receiver) = watch::channel(false);
    for server in ["beta", "gamma"] {
        let ready_sender = ready_sender.clone();
        let cleanup_sender = cleanup_sender.clone();
        let mut release_receiver = release_receiver.clone();
        bound.runtime_overrides.push_back(Box::new(move |runtime| {
            runtime.replace_supervisor(move |shutdown| async move {
                ready_sender.send(server).unwrap();
                shutdown.await.unwrap();
                cleanup_sender.send(server).unwrap();
                release_receiver
                    .wait_for(|released| *released)
                    .await
                    .unwrap();
                Err(anyhow::anyhow!("{server} cleanup failed"))
            });
        }));
    }
    drop((ready_sender, cleanup_sender));

    let shutdown = Shutdown::new();
    let (_fatal_sender, fatal_receiver) = mpsc::unbounded_channel();
    let daemon = tokio::spawn(async move {
        let mut signals = pending_signal_watcher();
        bound
            .activate_and_run(shutdown, fatal_receiver, &mut signals)
            .await
    });

    let mut ready = [
        ready_receiver.recv().await.unwrap(),
        ready_receiver.recv().await.unwrap(),
    ];
    ready.sort_unstable();
    assert_eq!(ready, ["beta", "gamma"]);
    fail_sender.send(()).unwrap();

    let mut cleaning = [
        cleanup_receiver.recv().await.unwrap(),
        cleanup_receiver.recv().await.unwrap(),
    ];
    cleaning.sort_unstable();
    assert_eq!(cleaning, ["beta", "gamma"]);
    assert!(!daemon.is_finished());
    release_sender.send(true).unwrap();

    let error = daemon.await.unwrap().unwrap_err();
    let message = format!("{error:#}");
    assert!(
        message
            .starts_with("[server=alpha] server supervisor failed: originating supervisor failure")
    );
    assert!(message.contains("[server=beta]"));
    assert!(message.contains("beta cleanup failed"));
    assert!(message.contains("[server=gamma]"));
    assert!(message.contains("gamma cleanup failed"));
    fs::remove_dir_all(directory).ok();
}

#[test]
fn bind_all_is_inert_and_holds_every_listener() {
    run_isolated_fixture("coordinator::tests::bind_all_is_inert_and_holds_every_listener_fixture");
}

#[tokio::test]
#[ignore = "runs through the parallel-safe public test wrapper"]
async fn bind_all_is_inert_and_holds_every_listener_fixture() {
    let (listener_address, [first_public, second_public]) = isolated_public_listeners();
    let first_backend = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let second_backend = StdTcpListener::bind("127.0.0.1:0").unwrap();
    first_backend.set_nonblocking(true).unwrap();
    second_backend.set_nonblocking(true).unwrap();
    let first_public_address = first_public.local_addr().unwrap();
    let second_public_address = second_public.local_addr().unwrap();
    let (directory, path) = write_two_server_config(
        listener_address,
        first_public_address.port(),
        second_public_address.port(),
        first_backend.local_addr().unwrap().port(),
        second_backend.local_addr().unwrap().port(),
    );
    drop((first_public, second_public));

    let bound = DaemonCoordinator::prepare(&path)
        .unwrap()
        .bind_all()
        .await
        .unwrap();
    assert!(StdTcpListener::bind(first_public_address).is_err());
    assert!(StdTcpListener::bind(second_public_address).is_err());
    assert_eq!(
        first_backend.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    assert_eq!(
        second_backend.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );

    drop(bound);
    StdTcpListener::bind(first_public_address).unwrap();
    StdTcpListener::bind(second_public_address).unwrap();
    fs::remove_dir_all(directory).ok();
}

#[test]
fn later_bind_failure_releases_earlier_listeners_without_activation() {
    run_isolated_fixture(
        "coordinator::tests::later_bind_failure_releases_earlier_listeners_without_activation_fixture",
    );
}

#[tokio::test]
#[ignore = "runs through the parallel-safe public test wrapper"]
async fn later_bind_failure_releases_earlier_listeners_without_activation_fixture() {
    let (listener_address, [first_public, second_public]) = isolated_public_listeners();
    let first_backend = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let second_backend = StdTcpListener::bind("127.0.0.1:0").unwrap();
    first_backend.set_nonblocking(true).unwrap();
    second_backend.set_nonblocking(true).unwrap();
    let first_address = first_public.local_addr().unwrap();
    let second_address = second_public.local_addr().unwrap();
    let (directory, path) = write_two_server_config(
        listener_address,
        first_address.port(),
        second_address.port(),
        first_backend.local_addr().unwrap().port(),
        second_backend.local_addr().unwrap().port(),
    );
    drop(first_public);

    let error = DaemonCoordinator::prepare(&path)
        .unwrap()
        .bind_all()
        .await
        .err()
        .expect("later occupied listener should fail bind-all");
    assert!(format!("{error:#}").contains("survival"));
    StdTcpListener::bind(first_address).expect("earlier listener must be released");
    assert_eq!(
        first_backend.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    assert_eq!(
        second_backend.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    drop(second_public);
    fs::remove_dir_all(directory).ok();
}

#[test]
fn shutdown_pending_during_activation_prevents_every_supervisor() {
    run_isolated_fixture(
        "coordinator::tests::shutdown_pending_during_activation_prevents_every_supervisor_fixture",
    );
}

#[tokio::test]
#[ignore = "runs through the parallel-safe public test wrapper"]
async fn shutdown_pending_during_activation_prevents_every_supervisor_fixture() {
    let (listener_address, [first_public, second_public]) = isolated_public_listeners();
    let first_backend = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let second_backend = StdTcpListener::bind("127.0.0.1:0").unwrap();
    first_backend.set_nonblocking(true).unwrap();
    second_backend.set_nonblocking(true).unwrap();
    let first_address = first_public.local_addr().unwrap();
    let second_address = second_public.local_addr().unwrap();
    let (directory, path) = write_two_server_config(
        listener_address,
        first_address.port(),
        second_address.port(),
        first_backend.local_addr().unwrap().port(),
        second_backend.local_addr().unwrap().port(),
    );
    drop((first_public, second_public));
    let bound = DaemonCoordinator::prepare(&path)
        .unwrap()
        .bind_all()
        .await
        .unwrap();
    let shutdown = Shutdown::new();
    shutdown.request();
    let (_fatal_sender, fatal_receiver) = mpsc::unbounded_channel();
    let mut signals = pending_signal_watcher();

    bound
        .activate_and_run(shutdown, fatal_receiver, &mut signals)
        .await
        .unwrap();
    assert_eq!(
        first_backend.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    assert_eq!(
        second_backend.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    StdTcpListener::bind(first_address).unwrap();
    StdTcpListener::bind(second_address).unwrap();
    fs::remove_dir_all(directory).ok();
}

#[test]
fn first_runtime_failure_prevents_later_activation() {
    run_isolated_fixture(
        "coordinator::tests::first_runtime_failure_prevents_later_activation_fixture",
    );
}

#[tokio::test]
#[ignore = "runs through the parallel-safe public test wrapper"]
async fn first_runtime_failure_prevents_later_activation_fixture() {
    let (listener_address, [first_public, second_public]) = isolated_public_listeners();
    let first_backend = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let second_backend = StdTcpListener::bind("127.0.0.1:0").unwrap();
    second_backend.set_nonblocking(true).unwrap();
    let first_address = first_public.local_addr().unwrap();
    let second_address = second_public.local_addr().unwrap();
    let (directory, path) = write_two_server_config(
        listener_address,
        first_address.port(),
        second_address.port(),
        first_backend.local_addr().unwrap().port(),
        second_backend.local_addr().unwrap().port(),
    );
    drop((first_public, second_public));
    let mut bound = DaemonCoordinator::prepare(&path)
        .unwrap()
        .bind_all()
        .await
        .unwrap();
    bound.runtime_overrides.push_back(Box::new(|runtime| {
        runtime.replace_supervisor(|_| async {
            panic!("intentional coordinator supervisor panic");
        });
    }));
    let shutdown = Shutdown::new();
    let (_fatal_sender, fatal_receiver) = mpsc::unbounded_channel();
    let mut signals = pending_signal_watcher();

    let error = bound
        .activate_and_run(shutdown, fatal_receiver, &mut signals)
        .await
        .expect_err("first runtime panic must stop activation");
    let message = format!("{error:#}");
    assert!(message.contains("server=creative"));
    assert!(message.contains("supervisor panicked"));
    assert_eq!(
        second_backend.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    StdTcpListener::bind(first_address).unwrap();
    StdTcpListener::bind(second_address).unwrap();
    fs::remove_dir_all(directory).ok();
}
