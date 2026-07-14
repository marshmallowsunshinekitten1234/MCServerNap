use std::collections::{BTreeMap, HashMap};
use std::future::{Future as _, poll_fn};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Poll;

use anyhow::{Context, Result, bail};
use tokio::sync::{Semaphore, mpsc, oneshot, watch};
use tokio::task::{JoinHandle, JoinSet};

use crate::config;
use crate::context::ServerContext;
use crate::runtime::{BoundServerRuntime, PreparedServerRuntime, ServerRuntime};

#[derive(Clone)]
struct Shutdown {
    state: watch::Sender<bool>,
}

impl Shutdown {
    fn new() -> Self {
        let (state, _) = watch::channel(false);
        Self { state }
    }

    fn request(&self) -> bool {
        self.state.send_if_modified(|pending| {
            if *pending {
                false
            } else {
                *pending = true;
                true
            }
        })
    }

    fn is_pending(&self) -> bool {
        *self.state.borrow()
    }

    async fn wait(&self) {
        let mut receiver = self.state.subscribe();
        if *receiver.borrow() {
            return;
        }
        while receiver.changed().await.is_ok() {
            if *receiver.borrow_and_update() {
                return;
            }
        }
    }
}

struct SignalWatcher {
    task: Option<JoinHandle<SignalWatcherExit>>,
}

enum SignalWatcherExit {
    ShutdownRequested,
    FailureReported,
}

impl SignalWatcher {
    async fn arm(shutdown: Shutdown, fatal: mpsc::UnboundedSender<anyhow::Error>) -> Result<Self> {
        let (armed_sender, armed_receiver) = oneshot::channel::<std::result::Result<(), String>>();
        let task = tokio::spawn(async move {
            #[cfg(unix)]
            let mut terminate =
                match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                    Ok(signal) => signal,
                    Err(error) => {
                        let _ = armed_sender.send(Err(format!(
                            "failed to register the Unix SIGTERM listener: {error}"
                        )));
                        return SignalWatcherExit::FailureReported;
                    }
                };

            let mut ctrl_c = Box::pin(tokio::signal::ctrl_c());
            let initial_ctrl_c = poll_fn(|context| match ctrl_c.as_mut().poll(context) {
                Poll::Pending => Poll::Ready(None),
                Poll::Ready(result) => Poll::Ready(Some(result)),
            })
            .await;
            match initial_ctrl_c {
                Some(Err(error)) => {
                    let _ = armed_sender.send(Err(format!(
                        "failed to register the Ctrl+C listener: {error}"
                    )));
                    return SignalWatcherExit::FailureReported;
                }
                Some(Ok(())) => {
                    if armed_sender.send(Ok(())).is_ok() {
                        log::info!("Ctrl+C received; shutting down");
                        shutdown.request();
                    }
                    return SignalWatcherExit::ShutdownRequested;
                }
                None => {
                    if armed_sender.send(Ok(())).is_err() {
                        return SignalWatcherExit::FailureReported;
                    }
                }
            }

            #[cfg(unix)]
            let exit = tokio::select! {
                result = &mut ctrl_c => signal_result(result, "Ctrl+C", &shutdown, &fatal),
                signal = terminate.recv() => {
                    if signal.is_some() {
                        log::info!("SIGTERM received; shutting down");
                        shutdown.request();
                        SignalWatcherExit::ShutdownRequested
                    } else {
                        let _ = fatal.send(anyhow::anyhow!("Unix SIGTERM listener closed unexpectedly"));
                        shutdown.request();
                        SignalWatcherExit::FailureReported
                    }
                }
            };

            #[cfg(not(unix))]
            let exit = signal_result(ctrl_c.await, "Ctrl+C", &shutdown, &fatal);

            exit
        });

        match armed_receiver.await {
            Ok(Ok(())) => Ok(Self { task: Some(task) }),
            Ok(Err(error)) => {
                task.abort();
                bail!(error)
            }
            Err(_) => {
                task.abort();
                bail!("signal watcher stopped before confirming registration")
            }
        }
    }
}

impl Drop for SignalWatcher {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

fn signal_result(
    result: std::io::Result<()>,
    name: &str,
    shutdown: &Shutdown,
    fatal: &mpsc::UnboundedSender<anyhow::Error>,
) -> SignalWatcherExit {
    match result {
        Ok(()) => {
            log::info!("{name} received; shutting down");
            shutdown.request();
            SignalWatcherExit::ShutdownRequested
        }
        Err(error) => {
            let _ = fatal.send(anyhow::anyhow!("{name} listener failed: {error}"));
            shutdown.request();
            SignalWatcherExit::FailureReported
        }
    }
}

pub struct DaemonCoordinator;

struct PreparedDaemon {
    servers: BTreeMap<String, (ServerContext, PreparedServerRuntime)>,
}

struct BoundDaemon {
    servers: Vec<(ServerContext, BoundServerRuntime)>,
    #[cfg(test)]
    fail_first_runtime: bool,
}

impl DaemonCoordinator {
    pub async fn run(config_path: &Path) -> Result<()> {
        let prepared = Self::prepare(config_path)?;
        let shutdown = Shutdown::new();
        let (fatal_sender, fatal_receiver) = mpsc::unbounded_channel();
        let mut signals = SignalWatcher::arm(shutdown.clone(), fatal_sender).await?;
        let bound = prepared.bind_all().await?;
        bound
            .activate_and_run(shutdown, fatal_receiver, &mut signals)
            .await
    }

    fn prepare(config_path: &Path) -> Result<PreparedDaemon> {
        let prepared = config::load(config_path)?;
        let connection_limit = Arc::new(Semaphore::new(prepared.max_connections));
        let mut servers = BTreeMap::new();
        for (server_id, server) in prepared.servers {
            let runtime = ServerRuntime::prepare(
                server.runtime,
                server.responder,
                Arc::clone(&connection_limit),
            )
            .with_context(|| format!("[server={server_id}] runtime preparation failed"))?;
            servers.insert(server_id, (server.context, runtime));
        }
        Ok(PreparedDaemon { servers })
    }
}

impl PreparedDaemon {
    async fn bind_all(self) -> Result<BoundDaemon> {
        let mut bound = Vec::with_capacity(self.servers.len());
        for (server_id, (context, prepared)) in self.servers {
            let runtime = prepared
                .bind()
                .await
                .with_context(|| format!("[server={server_id}] listener binding failed"))?;
            bound.push((context, runtime));
        }
        Ok(BoundDaemon {
            servers: bound,
            #[cfg(test)]
            fail_first_runtime: false,
        })
    }
}

enum RuntimeTaskExit {
    Returned {
        result: Result<()>,
        shutdown_was_delivered: bool,
    },
    Panicked(tokio::task::JoinError),
}

struct RuntimeTaskOutput {
    context: ServerContext,
    exit: RuntimeTaskExit,
}

#[derive(Default)]
struct CoordinatorState {
    primary_error: Option<anyhow::Error>,
    additional_errors: Vec<anyhow::Error>,
    infrastructure_closed: bool,
    watcher_failure_received: bool,
    watcher_observed: bool,
}

impl CoordinatorState {
    fn record_fatal(&mut self, error: anyhow::Error, shutdown: &Shutdown) {
        if self.primary_error.is_none() {
            self.primary_error = Some(error);
            shutdown.request();
        } else {
            self.additional_errors.push(error);
        }
    }

    fn record_infrastructure_failure(&mut self, error: anyhow::Error, shutdown: &Shutdown) {
        self.watcher_failure_received = true;
        self.record_fatal(error, shutdown);
    }

    fn drain_infrastructure(
        &mut self,
        failures: &mut mpsc::UnboundedReceiver<anyhow::Error>,
        shutdown: &Shutdown,
    ) {
        loop {
            match failures.try_recv() {
                Ok(error) => self.record_infrastructure_failure(error, shutdown),
                Err(mpsc::error::TryRecvError::Empty) => return,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    self.infrastructure_closed = true;
                    return;
                }
            }
        }
    }

    fn observe_watcher(
        &mut self,
        result: std::result::Result<SignalWatcherExit, tokio::task::JoinError>,
        failures: &mut mpsc::UnboundedReceiver<anyhow::Error>,
        shutdown: &Shutdown,
    ) {
        self.watcher_observed = true;
        self.drain_infrastructure(failures, shutdown);
        match result {
            Ok(SignalWatcherExit::ShutdownRequested) => {
                if !shutdown.is_pending() {
                    self.record_fatal(
                        anyhow::anyhow!(
                            "signal watcher returned after a signal without requesting shutdown"
                        ),
                        shutdown,
                    );
                }
            }
            Ok(SignalWatcherExit::FailureReported) => {
                if !self.watcher_failure_received {
                    self.record_fatal(
                        anyhow::anyhow!(
                            "signal watcher stopped after a failure without reporting its cause"
                        ),
                        shutdown,
                    );
                }
            }
            Err(error) => self.record_fatal(
                anyhow::anyhow!("signal watcher task panicked or was cancelled: {error}"),
                shutdown,
            ),
        }
    }

    fn observe_runtime(
        &mut self,
        completed: std::result::Result<
            (tokio::task::Id, RuntimeTaskOutput),
            tokio::task::JoinError,
        >,
        contexts: &mut HashMap<tokio::task::Id, ServerContext>,
        shutdown: &Shutdown,
    ) {
        match completed {
            Ok((task_id, output)) => {
                contexts.remove(&task_id);
                match output.exit {
                    RuntimeTaskExit::Returned {
                        result: Err(error),
                        ..
                    } => self.record_fatal(
                        anyhow::anyhow!("[server={}] {error:#}", output.context),
                        shutdown,
                    ),
                    RuntimeTaskExit::Returned {
                        result: Ok(()),
                        shutdown_was_delivered: false,
                    } => self.record_fatal(
                        anyhow::anyhow!(
                            "[server={}] complete runtime stopped unexpectedly before shutdown",
                            output.context
                        ),
                        shutdown,
                    ),
                    RuntimeTaskExit::Returned {
                        result: Ok(()),
                        shutdown_was_delivered: true,
                    } => {}
                    RuntimeTaskExit::Panicked(error) => self.record_fatal(
                        anyhow::anyhow!(
                            "[server={}] complete runtime panicked; cleanup of this runtime is uncertain: {error}",
                            output.context
                        ),
                        shutdown,
                    ),
                }
            }
            Err(error) => {
                let context = contexts
                    .remove(&error.id())
                    .map_or_else(|| "unknown".to_owned(), |context| context.to_string());
                self.record_fatal(
                    anyhow::anyhow!("[server={context}] runtime monitor task panicked: {error}"),
                    shutdown,
                );
            }
        }
    }

    fn finish(self) -> Result<()> {
        let Some(primary) = self.primary_error else {
            return Ok(());
        };
        if self.additional_errors.is_empty() {
            return Err(primary);
        }
        let additional = self
            .additional_errors
            .into_iter()
            .map(|error| format!("{error:#}"))
            .collect::<Vec<_>>()
            .join("; ");
        bail!("{primary:#}; additional runtime cleanup failures: {additional}")
    }
}

async fn observe_pending_events(
    state: &mut CoordinatorState,
    failures: &mut mpsc::UnboundedReceiver<anyhow::Error>,
    watcher: &mut JoinHandle<SignalWatcherExit>,
    runtimes: &mut JoinSet<RuntimeTaskOutput>,
    contexts: &mut HashMap<tokio::task::Id, ServerContext>,
    shutdown: &Shutdown,
) {
    tokio::task::yield_now().await;
    state.drain_infrastructure(failures, shutdown);
    if !state.watcher_observed && watcher.is_finished() {
        state.observe_watcher(watcher.await, failures, shutdown);
    }
    while let Some(completed) = runtimes.try_join_next_with_id() {
        state.observe_runtime(completed, contexts, shutdown);
    }
}

fn spawn_runtime(
    runtimes: &mut JoinSet<RuntimeTaskOutput>,
    contexts: &mut HashMap<tokio::task::Id, ServerContext>,
    context: ServerContext,
    runtime: ServerRuntime,
    shutdown: &Shutdown,
) {
    let runtime_shutdown = shutdown.clone();
    let returned_context = context.clone();
    let task = runtimes.spawn(async move {
        let inner_shutdown = runtime_shutdown.clone();
        let shutdown_delivered = Arc::new(AtomicBool::new(false));
        let delivered_by_shutdown = Arc::clone(&shutdown_delivered);
        let runtime_task = tokio::spawn(runtime.run_until(async move {
            inner_shutdown.wait().await;
            delivered_by_shutdown.store(true, Ordering::Release);
            Ok(())
        }));
        let exit = match runtime_task.await {
            Ok(result) => {
                let shutdown_was_delivered = shutdown_delivered.load(Ordering::Acquire);
                if result.is_err() || !shutdown_was_delivered {
                    runtime_shutdown.request();
                }
                RuntimeTaskExit::Returned {
                    result,
                    shutdown_was_delivered,
                }
            }
            Err(error) => {
                runtime_shutdown.request();
                RuntimeTaskExit::Panicked(error)
            }
        };
        RuntimeTaskOutput {
            context: returned_context,
            exit,
        }
    });
    contexts.insert(task.id(), context);
}

impl BoundDaemon {
    async fn activate_and_run(
        self,
        shutdown: Shutdown,
        mut infrastructure_failures: mpsc::UnboundedReceiver<anyhow::Error>,
        signals: &mut SignalWatcher,
    ) -> Result<()> {
        let mut watcher = signals
            .task
            .take()
            .expect("armed signal watcher must own one task");
        let mut runtimes = JoinSet::new();
        let mut contexts = HashMap::new();
        let mut state = CoordinatorState::default();
        #[cfg(test)]
        let mut fail_next_runtime = self.fail_first_runtime;
        for (context, bound) in self.servers {
            observe_pending_events(
                &mut state,
                &mut infrastructure_failures,
                &mut watcher,
                &mut runtimes,
                &mut contexts,
                &shutdown,
            )
            .await;
            if shutdown.is_pending() {
                break;
            }
            let runtime = bound.activate();
            #[cfg(test)]
            let mut runtime = runtime;
            #[cfg(test)]
            if fail_next_runtime {
                runtime.replace_supervisor_with_panic();
                fail_next_runtime = false;
            }
            spawn_runtime(&mut runtimes, &mut contexts, context, runtime, &shutdown);
        }

        observe_pending_events(
            &mut state,
            &mut infrastructure_failures,
            &mut watcher,
            &mut runtimes,
            &mut contexts,
            &shutdown,
        )
        .await;

        while !runtimes.is_empty() {
            tokio::select! {
                biased;

                failure = infrastructure_failures.recv(), if !state.infrastructure_closed => {
                    match failure {
                        Some(error) => state.record_infrastructure_failure(error, &shutdown),
                        None => state.infrastructure_closed = true,
                    }
                }
                result = &mut watcher, if !state.watcher_observed => {
                    state.observe_watcher(result, &mut infrastructure_failures, &shutdown);
                }
                completed = runtimes.join_next_with_id() => {
                    if let Some(completed) = completed {
                        state.observe_runtime(completed, &mut contexts, &shutdown);
                    } else {
                        break;
                    }
                }
            }
        }

        state.drain_infrastructure(&mut infrastructure_failures, &shutdown);
        if !state.watcher_observed && watcher.is_finished() {
            state.observe_watcher(
                (&mut watcher).await,
                &mut infrastructure_failures,
                &shutdown,
            );
        }
        if !state.watcher_observed {
            watcher.abort();
            match (&mut watcher).await {
                Err(error) if error.is_cancelled() => {}
                result => {
                    state.observe_watcher(result, &mut infrastructure_failures, &shutdown);
                }
            }
        }
        state.drain_infrastructure(&mut infrastructure_failures, &shutdown);
        state.finish()
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::net::TcpListener as StdTcpListener;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

    fn temporary_directory() -> PathBuf {
        let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "mcservernap-coordinator-test-{}-{id}",
            std::process::id()
        ))
    }

    fn server_table(id: &str, listener_port: u16, backend_port: u16, workdir: &str) -> String {
        format!(
            r#"
[servers.{id}]
minecraft_version = "26.2"

[servers.{id}.listener]
address = "127.0.0.1"
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
            server_table("creative", first_listener, first_backend, "creative"),
            server_table("survival", second_listener, second_backend, "survival")
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

    #[test]
    fn buffered_infrastructure_failure_is_drained_after_sender_drop() {
        let shutdown = Shutdown::new();
        let (sender, mut receiver) = mpsc::unbounded_channel();
        sender
            .send(anyhow::anyhow!("buffered signal-listener failure"))
            .unwrap();
        drop(sender);
        let mut state = CoordinatorState::default();

        state.drain_infrastructure(&mut receiver, &shutdown);

        assert!(state.infrastructure_closed);
        assert!(shutdown.is_pending());
        assert!(
            state
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
                fail_first_runtime: false,
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
    async fn zero_activation_still_reports_buffered_watcher_failure() {
        let shutdown = Shutdown::new();
        let watcher_shutdown = shutdown.clone();
        let (sender, receiver) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            sender
                .send(anyhow::anyhow!("fatal failure before activation"))
                .unwrap();
            watcher_shutdown.request();
            SignalWatcherExit::FailureReported
        });
        let mut signals = SignalWatcher { task: Some(task) };
        while !signals.task.as_ref().unwrap().is_finished() {
            tokio::task::yield_now().await;
        }
        let bound = BoundDaemon {
            servers: Vec::new(),
            fail_first_runtime: false,
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

    #[tokio::test]
    async fn bind_all_is_inert_and_holds_every_listener() {
        let first_public = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let second_public = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let first_backend = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let second_backend = StdTcpListener::bind("127.0.0.1:0").unwrap();
        first_backend.set_nonblocking(true).unwrap();
        second_backend.set_nonblocking(true).unwrap();
        let first_public_address = first_public.local_addr().unwrap();
        let second_public_address = second_public.local_addr().unwrap();
        let (directory, path) = write_two_server_config(
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

    #[tokio::test]
    async fn later_bind_failure_releases_earlier_listeners_without_activation() {
        let first_public = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let second_public = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let first_backend = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let second_backend = StdTcpListener::bind("127.0.0.1:0").unwrap();
        first_backend.set_nonblocking(true).unwrap();
        second_backend.set_nonblocking(true).unwrap();
        let first_address = first_public.local_addr().unwrap();
        let second_address = second_public.local_addr().unwrap();
        let (directory, path) = write_two_server_config(
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

    #[tokio::test]
    async fn shutdown_pending_during_activation_prevents_every_supervisor() {
        let first_public = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let second_public = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let first_backend = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let second_backend = StdTcpListener::bind("127.0.0.1:0").unwrap();
        first_backend.set_nonblocking(true).unwrap();
        second_backend.set_nonblocking(true).unwrap();
        let first_address = first_public.local_addr().unwrap();
        let second_address = second_public.local_addr().unwrap();
        let (directory, path) = write_two_server_config(
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

    #[tokio::test]
    async fn first_runtime_failure_prevents_later_activation() {
        let first_public = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let second_public = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let first_backend = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let second_backend = StdTcpListener::bind("127.0.0.1:0").unwrap();
        second_backend.set_nonblocking(true).unwrap();
        let first_address = first_public.local_addr().unwrap();
        let second_address = second_public.local_addr().unwrap();
        let (directory, path) = write_two_server_config(
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
        bound.fail_first_runtime = true;
        let shutdown = Shutdown::new();
        let (_fatal_sender, fatal_receiver) = mpsc::unbounded_channel();
        let mut signals = pending_signal_watcher();

        let error = bound
            .activate_and_run(shutdown, fatal_receiver, &mut signals)
            .await
            .expect_err("first runtime panic must stop activation");
        assert!(error.to_string().contains("server=creative"));
        assert_eq!(
            second_backend.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        StdTcpListener::bind(first_address).unwrap();
        StdTcpListener::bind(second_address).unwrap();
        fs::remove_dir_all(directory).ok();
    }
}
