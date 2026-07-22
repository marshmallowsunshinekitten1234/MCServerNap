#[cfg(test)]
use std::collections::VecDeque;
use std::collections::{BTreeMap, HashMap};
use std::future::{Future as _, poll_fn};
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Poll;

use anyhow::{Context, Result, bail};
use tokio::sync::{Semaphore, mpsc, oneshot, watch};
use tokio::task::{JoinHandle, JoinSet};

use crate::config;
use crate::context::ServerContext;
use crate::control::{BoundEndpoint, ControlServer, InstanceLock, PreparedRegistry};
use crate::runtime::{
    ActivatedServerRuntime, BoundServerRuntime, PreparedServerRuntime, ServerRuntime,
};

#[derive(Clone)]
struct Shutdown {
    state: watch::Sender<bool>,
    failures: Arc<Mutex<FatalErrors>>,
}

#[derive(Clone)]
struct RuntimeStop {
    state: watch::Sender<bool>,
}

impl RuntimeStop {
    fn new() -> Self {
        let (state, _) = watch::channel(false);
        Self { state }
    }

    fn request(&self) {
        self.state.send_replace(true);
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

#[derive(Default)]
struct FatalErrors {
    primary: Option<anyhow::Error>,
    additional: Vec<anyhow::Error>,
}

impl FatalErrors {
    fn record(&mut self, error: anyhow::Error) {
        if self.primary.is_none() {
            self.primary = Some(error);
        } else {
            self.additional.push(error);
        }
    }

    fn finish(self) -> Result<()> {
        let Some(primary) = self.primary else {
            return Ok(());
        };
        if self.additional.is_empty() {
            return Err(primary);
        }
        let additional = self
            .additional
            .into_iter()
            .map(|error| format!("{error:#}"))
            .collect::<Vec<_>>()
            .join("; ");
        bail!("{primary:#}; additional daemon failures: {additional}")
    }
}

impl Shutdown {
    fn new() -> Self {
        let (state, _) = watch::channel(false);
        Self {
            state,
            failures: Arc::new(Mutex::new(FatalErrors::default())),
        }
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

    fn fail(&self, error: anyhow::Error) {
        let mut failures = self
            .failures
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        failures.record(error);
        self.request();
    }

    fn finish(&self) -> Result<()> {
        let failures = {
            let mut failures = self
                .failures
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            std::mem::take(&mut *failures)
        };
        failures.finish()
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
    async fn arm(shutdown: Shutdown, fatal: mpsc::UnboundedSender<()>) -> Result<Self> {
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
                        shutdown.fail(anyhow::anyhow!("Unix SIGTERM listener closed unexpectedly"));
                        let _ = fatal.send(());
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
    fatal: &mpsc::UnboundedSender<()>,
) -> SignalWatcherExit {
    match result {
        Ok(()) => {
            log::info!("{name} received; shutting down");
            shutdown.request();
            SignalWatcherExit::ShutdownRequested
        }
        Err(error) => {
            shutdown.fail(anyhow::anyhow!("{name} listener failed: {error}"));
            let _ = fatal.send(());
            SignalWatcherExit::FailureReported
        }
    }
}

pub struct DaemonCoordinator;

struct PreparedDaemon {
    servers: BTreeMap<String, (ServerContext, PreparedServerRuntime)>,
    control: PreparedRegistry,
}

struct BoundDaemon {
    servers: Vec<(ServerContext, BoundServerRuntime)>,
    control: Option<PreparedRegistry>,
    control_endpoint: Option<BoundEndpoint>,
    #[cfg(test)]
    runtime_overrides: VecDeque<RuntimeOverride>,
}

#[cfg(test)]
type RuntimeOverride = Box<dyn FnOnce(&mut ServerRuntime) + Send>;

impl DaemonCoordinator {
    pub async fn run(config_path: &Path) -> Result<()> {
        let prepared = Self::prepare(config_path)?;
        let shutdown = Shutdown::new();
        let (fatal_sender, fatal_receiver) = mpsc::unbounded_channel();
        let mut signals = SignalWatcher::arm(shutdown.clone(), fatal_sender).await?;
        let instance_lock = InstanceLock::acquire()?;
        let mut bound = prepared.bind_all().await?;
        bound.control_endpoint = Some(instance_lock.bind()?);
        bound
            .activate_and_run(shutdown, fatal_receiver, &mut signals)
            .await?;
        drop(instance_lock);
        Ok(())
    }

    fn prepare(config_path: &Path) -> Result<PreparedDaemon> {
        let prepared = config::load(config_path)?;
        let control = PreparedRegistry::new(prepared.servers.keys())?;
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
        Ok(PreparedDaemon { servers, control })
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
            control: Some(self.control),
            control_endpoint: None,
            #[cfg(test)]
            runtime_overrides: VecDeque::new(),
        })
    }
}

#[derive(Default)]
struct CoordinatorState {
    infrastructure_closed: bool,
    watcher_failure_received: bool,
    watcher_observed: bool,
}

impl CoordinatorState {
    fn record_infrastructure_failure(&mut self) {
        self.watcher_failure_received = true;
    }

    fn drain_infrastructure(&mut self, failures: &mut mpsc::UnboundedReceiver<()>) {
        loop {
            match failures.try_recv() {
                Ok(()) => self.record_infrastructure_failure(),
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
        failures: &mut mpsc::UnboundedReceiver<()>,
        shutdown: &Shutdown,
    ) {
        self.watcher_observed = true;
        self.drain_infrastructure(failures);
        match result {
            Ok(SignalWatcherExit::ShutdownRequested) => {
                if !shutdown.is_pending() {
                    shutdown.fail(anyhow::anyhow!(
                        "signal watcher returned after a signal without requesting shutdown"
                    ));
                }
            }
            Ok(SignalWatcherExit::FailureReported) => {
                if !self.watcher_failure_received {
                    shutdown.fail(anyhow::anyhow!(
                        "signal watcher stopped after a failure without reporting its cause"
                    ));
                }
            }
            Err(error) => shutdown.fail(anyhow::anyhow!(
                "signal watcher task panicked or was cancelled: {error}"
            )),
        }
    }

    fn observe_runtime(
        completed: std::result::Result<(tokio::task::Id, ()), tokio::task::JoinError>,
        contexts: &mut HashMap<tokio::task::Id, ServerContext>,
        shutdown: &Shutdown,
    ) {
        match completed {
            Ok((task_id, ())) => {
                contexts.remove(&task_id);
            }
            Err(error) => {
                let context = contexts
                    .remove(&error.id())
                    .map_or_else(|| "unknown".to_owned(), |context| context.to_string());
                shutdown.fail(anyhow::anyhow!(
                    "[server={context}] runtime monitor task panicked: {error}"
                ));
            }
        }
    }
}

async fn observe_pending_events(
    state: &mut CoordinatorState,
    failures: &mut mpsc::UnboundedReceiver<()>,
    watcher: &mut JoinHandle<SignalWatcherExit>,
    runtimes: &mut JoinSet<()>,
    contexts: &mut HashMap<tokio::task::Id, ServerContext>,
    shutdown: &Shutdown,
) {
    tokio::task::yield_now().await;
    state.drain_infrastructure(failures);
    if !state.watcher_observed && watcher.is_finished() {
        state.observe_watcher(watcher.await, failures, shutdown);
    }
    while let Some(completed) = runtimes.try_join_next_with_id() {
        CoordinatorState::observe_runtime(completed, contexts, shutdown);
    }
}

fn spawn_runtime(
    runtimes: &mut JoinSet<()>,
    contexts: &mut HashMap<tokio::task::Id, ServerContext>,
    context: ServerContext,
    runtime: ServerRuntime,
    shutdown: &Shutdown,
    runtime_stop: &RuntimeStop,
) {
    let runtime_shutdown = shutdown.clone();
    let runtime_stop = runtime_stop.clone();
    let returned_context = context.clone();
    let task = runtimes.spawn(async move {
        let mut fatal = runtime.fatal_observer();
        let shutdown_delivered = Arc::new(AtomicBool::new(false));
        let delivered_by_shutdown = Arc::clone(&shutdown_delivered);
        let mut runtime_task = tokio::spawn(runtime.run_until(async move {
            runtime_stop.wait().await;
            delivered_by_shutdown.store(true, Ordering::Release);
            Ok(())
        }));
        let mut fatal_closed = false;
        let runtime_result = loop {
            tokio::select! {
                biased;

                result = &mut runtime_task => break result,
                changed = fatal.changed(), if !fatal_closed => {
                    match changed {
                        Ok(()) if *fatal.borrow_and_update() => {
                            runtime_shutdown.request();
                            break runtime_task.await;
                        }
                        Ok(()) => {}
                        Err(_) => fatal_closed = true,
                    }
                }
            }
        };
        match runtime_result {
            Ok(result) => {
                let shutdown_was_delivered = shutdown_delivered.load(Ordering::Acquire);
                match (result, shutdown_was_delivered) {
                    (Ok(()), true) => {}
                    (Ok(()), false) => {
                        runtime_shutdown.fail(anyhow::anyhow!(
                            "[server={returned_context}] complete runtime stopped unexpectedly before shutdown"
                        ));
                    }
                    (Err(error), _) => {
                        runtime_shutdown.fail(anyhow::anyhow!(
                            "[server={returned_context}] {error:#}"
                        ));
                    }
                }
            }
            Err(error) => {
                runtime_shutdown.fail(anyhow::anyhow!(
                    "[server={returned_context}] complete runtime panicked; cleanup of this runtime is uncertain: {error}"
                ));
            }
        }
    });
    contexts.insert(task.id(), context);
}

impl BoundDaemon {
    #[allow(
        clippy::too_many_lines,
        reason = "startup, fatal observation, ordered control shutdown, and runtime draining are one coordinator state machine"
    )]
    async fn activate_and_run(
        self,
        shutdown: Shutdown,
        mut infrastructure_failures: mpsc::UnboundedReceiver<()>,
        signals: &mut SignalWatcher,
    ) -> Result<()> {
        let mut watcher = signals
            .task
            .take()
            .expect("armed signal watcher must own one task");
        let runtime_stop = RuntimeStop::new();
        let mut runtime_stop_requested = false;
        let mut runtimes = JoinSet::new();
        let mut contexts = HashMap::new();
        let expected_runtime_count = self.servers.len();
        let mut control_handles = BTreeMap::new();
        let mut control_preparation = self.control;
        let mut control_endpoint = self.control_endpoint;
        let mut control_shutdown = None;
        let mut admission_stopped = None;
        let mut control_task: Option<JoinHandle<Result<()>>> = None;
        let mut state = CoordinatorState::default();
        #[cfg(test)]
        let mut runtime_overrides = self.runtime_overrides;
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
            let ActivatedServerRuntime { runtime, control } = bound.activate();
            control_handles.insert(context.id().to_owned(), control);
            #[cfg(test)]
            let mut runtime = runtime;
            #[cfg(test)]
            if let Some(apply) = runtime_overrides.pop_front() {
                apply(&mut runtime);
            }
            spawn_runtime(
                &mut runtimes,
                &mut contexts,
                context,
                runtime,
                &shutdown,
                &runtime_stop,
            );
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

        if control_handles.len() == expected_runtime_count
            && !shutdown.is_pending()
            && let (Some(prepared), Some(mut endpoint)) =
                (control_preparation.take(), control_endpoint.take())
        {
            match prepared.activate(control_handles) {
                Ok(registry) => {
                    let (shutdown_sender, shutdown_receiver) = oneshot::channel();
                    let (stopped_sender, stopped_receiver) = oneshot::channel();
                    control_shutdown = Some(shutdown_sender);
                    admission_stopped = Some(stopped_receiver);
                    control_task = Some(tokio::spawn(
                        ControlServer::new(endpoint, registry)
                            .run(shutdown_receiver, stopped_sender),
                    ));
                    log::info!("Local control protocol v1 is ready");
                }
                Err(error) => {
                    shutdown.fail(error.context("control registry activation failed"));
                    if let Err(cleanup_error) = endpoint.stop() {
                        shutdown
                            .fail(cleanup_error.context("control endpoint cleanup also failed"));
                    }
                }
            }
        }

        if control_task.is_none() {
            if let Some(mut endpoint) = control_endpoint.take()
                && let Err(error) = endpoint.stop()
            {
                shutdown.fail(error.context("control endpoint cleanup failed before activation"));
            }
            if shutdown.is_pending() {
                runtime_stop.request();
                runtime_stop_requested = true;
            }
        }

        while !runtimes.is_empty() || control_task.is_some() {
            if shutdown.is_pending() && !runtime_stop_requested {
                if let Some(sender) = control_shutdown.take() {
                    let _ = sender.send(());
                }
                if let Some(stopped) = admission_stopped.take()
                    && stopped.await.is_err()
                    && let Some(task) = control_task.take()
                {
                    record_control_result(task.await, true, &shutdown);
                }
                runtime_stop.request();
                runtime_stop_requested = true;
            }

            tokio::select! {
                biased;

                () = shutdown.wait(), if !runtime_stop_requested => {}
                failure = infrastructure_failures.recv(), if !state.infrastructure_closed => {
                    match failure {
                        Some(()) => state.record_infrastructure_failure(),
                        None => state.infrastructure_closed = true,
                    }
                }
                result = &mut watcher, if !state.watcher_observed => {
                    state.observe_watcher(result, &mut infrastructure_failures, &shutdown);
                }
                completed = runtimes.join_next_with_id(), if !runtimes.is_empty() => {
                    if let Some(completed) = completed {
                        CoordinatorState::observe_runtime(completed, &mut contexts, &shutdown);
                    }
                }
                result = async {
                    control_task
                        .as_mut()
                        .expect("guarded control task must be present")
                        .await
                }, if control_task.is_some() => {
                    control_task = None;
                    record_control_result(result, runtime_stop_requested, &shutdown);
                }
            }
        }

        if shutdown.is_pending() && !runtime_stop_requested {
            if let Some(sender) = control_shutdown.take() {
                let _ = sender.send(());
            }
            if let Some(stopped) = admission_stopped.take() {
                let _ = stopped.await;
            }
            runtime_stop.request();
        }

        state.drain_infrastructure(&mut infrastructure_failures);
        if !state.watcher_observed && (watcher.is_finished() || state.infrastructure_closed) {
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
        state.drain_infrastructure(&mut infrastructure_failures);
        shutdown.finish()
    }
}

fn record_control_result(
    result: std::result::Result<Result<()>, tokio::task::JoinError>,
    shutdown_requested: bool,
    shutdown: &Shutdown,
) {
    match result {
        Ok(Ok(())) if shutdown_requested => {}
        Ok(Ok(())) => shutdown.fail(anyhow::anyhow!(
            "control accept loop stopped unexpectedly before daemon shutdown"
        )),
        Ok(Err(error)) => shutdown.fail(error.context("control infrastructure failed")),
        Err(error) => shutdown.fail(anyhow::anyhow!(
            "control accept loop panicked or was cancelled: {error}"
        )),
    }
}

#[cfg(test)]
mod tests;
