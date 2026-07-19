use std::fmt::Write as _;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read as _, Write as _};
use std::os::windows::fs::OpenOptionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result, ensure};
use serde::{Deserialize, Serialize};
use tokio::net::windows::named_pipe::{
    ClientOptions, NamedPipeClient, NamedPipeServer, PipeMode, ServerOptions,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tokio::time::{Instant, sleep_until};

const LOCATOR_VERSION: u16 = 1;
const MAX_LOCATOR_BYTES: u64 = 256;
const PIPE_PREFIX: &str = r"\\.\pipe\mcservernap-control-";
const ERROR_PIPE_BUSY: i32 = 231;
const ERROR_SHARING_VIOLATION: i32 = 32;
const PIPE_BUSY_RETRY: Duration = Duration::from_millis(25);

pub(crate) type ControlStream = NamedPipeClient;

#[derive(Clone)]
struct EndpointPaths {
    directory: PathBuf,
    lock: PathBuf,
    locator: PathBuf,
    temporary_locator: PathBuf,
}

pub(crate) struct InstanceLock {
    _lock: File,
    paths: EndpointPaths,
}

pub(crate) struct BoundEndpoint {
    waiting: Option<NamedPipeServer>,
    active: bool,
    pipe_name: String,
    locator: PathBuf,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Locator {
    version: u16,
    pipe: String,
}

impl InstanceLock {
    pub(crate) fn acquire() -> Result<Self> {
        Self::acquire_at(endpoint_paths()?)
    }

    fn acquire_at(paths: EndpointPaths) -> Result<Self> {
        fs::create_dir_all(&paths.directory)
            .context("failed to create the per-principal control directory")?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .share_mode(0)
            .open(&paths.lock)
            .map_err(|error| {
                if error.raw_os_error() == Some(ERROR_SHARING_VIOLATION) {
                    anyhow::anyhow!(
                        "another MCServerNap daemon already owns the control endpoint for this OS principal"
                    )
                } else {
                    anyhow::Error::new(error)
                        .context("failed to open the per-principal control instance lock")
                }
            })?;
        ensure!(
            lock.metadata()
                .context("failed to inspect the persistent control lock")?
                .is_file(),
            "control lock must be a regular file"
        );
        Ok(Self { _lock: lock, paths })
    }

    pub(crate) fn bind(&self) -> Result<BoundEndpoint> {
        remove_if_present(&self.paths.locator)
            .context("failed to remove a stale control locator")?;
        remove_if_present(&self.paths.temporary_locator)
            .context("failed to remove a stale temporary control locator")?;

        let pipe_name = random_pipe_name()?;
        let waiting = create_pipe(&pipe_name, true)
            .context("failed to create the initial per-principal control pipe")?;
        publish_locator(
            &self.paths.temporary_locator,
            &self.paths.locator,
            &pipe_name,
        )?;
        Ok(BoundEndpoint {
            waiting: Some(waiting),
            active: true,
            pipe_name,
            locator: self.paths.locator.clone(),
        })
    }
}

impl BoundEndpoint {
    pub(super) async fn accept(
        &mut self,
        permits: &Arc<Semaphore>,
    ) -> io::Result<Option<(NamedPipeServer, OwnedSemaphorePermit)>> {
        let waiting = self
            .waiting
            .as_ref()
            .ok_or_else(|| io::Error::other("control pipe has no waiting instance"))?;
        waiting.connect().await?;
        let connected = self
            .waiting
            .take()
            .expect("connected control pipe must remain the waiting instance");
        match Arc::clone(permits).try_acquire_owned() {
            Ok(permit) => {
                let replacement = create_pipe(&self.pipe_name, false)?;
                self.waiting = Some(replacement);
                Ok(Some((connected, permit)))
            }
            Err(TryAcquireError::NoPermits) => {
                let _ = connected.disconnect();
                drop(connected);
                let replacement = create_pipe(&self.pipe_name, false)?;
                self.waiting = Some(replacement);
                Ok(None)
            }
            Err(TryAcquireError::Closed) => {
                Err(io::Error::other("control exchange limiter closed"))
            }
        }
    }

    pub(crate) fn stop(&mut self) -> Result<()> {
        if !self.active {
            return Ok(());
        }
        self.active = false;
        drop(self.waiting.take());
        let locator = read_locator(&self.locator)
            .context("control locator disappeared or became invalid before cleanup")?;
        ensure!(
            locator == self.pipe_name,
            "control locator no longer names this daemon's pipe"
        );
        fs::remove_file(&self.locator).context("failed to remove the stopped control locator")
    }
}

impl Drop for BoundEndpoint {
    fn drop(&mut self) {
        if let Err(error) = self.stop() {
            log::warn!("Control endpoint cleanup failed: {error}");
        }
    }
}

pub(crate) async fn connect(deadline: Instant) -> io::Result<ControlStream> {
    let pipe_name = tokio::task::spawn_blocking(|| {
        let paths = endpoint_paths()?;
        read_locator(&paths.locator)
    })
    .await
    .map_err(|_| io::Error::other("control endpoint discovery task failed"))?
    .map_err(|error| io::Error::other(error.to_string()))?;

    connect_pipe(&pipe_name, deadline).await
}

async fn connect_pipe(pipe_name: &str, deadline: Instant) -> io::Result<ControlStream> {
    loop {
        match ClientOptions::new()
            .pipe_mode(PipeMode::Byte)
            .open(pipe_name)
        {
            Ok(client) => return Ok(client),
            Err(error) if error.raw_os_error() == Some(ERROR_PIPE_BUSY) => {
                let now = Instant::now();
                if now >= deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "control pipe remained busy",
                    ));
                }
                let retry = now + PIPE_BUSY_RETRY;
                sleep_until(if retry < deadline { retry } else { deadline }).await;
            }
            Err(error) => return Err(error),
        }
    }
}

fn endpoint_paths() -> Result<EndpointPaths> {
    let directory = dirs::data_local_dir()
        .context("Windows FOLDERID_LocalAppData is unavailable")?
        .join("MCServerNap");
    Ok(paths_in(directory))
}

fn paths_in(directory: PathBuf) -> EndpointPaths {
    EndpointPaths {
        lock: directory.join("control.lock"),
        locator: directory.join("control.endpoint"),
        temporary_locator: directory.join("control.endpoint.tmp"),
        directory,
    }
}

fn random_pipe_name() -> Result<String> {
    let mut suffix = [0; 16];
    getrandom::fill(&mut suffix).context("failed to obtain OS randomness for the control pipe")?;
    let mut name = String::with_capacity(PIPE_PREFIX.len() + suffix.len() * 2);
    name.push_str(PIPE_PREFIX);
    for byte in suffix {
        write!(name, "{byte:02x}").expect("writing to a String cannot fail");
    }
    Ok(name)
}

fn create_pipe(name: &str, first: bool) -> io::Result<NamedPipeServer> {
    ServerOptions::new()
        .pipe_mode(PipeMode::Byte)
        .access_inbound(true)
        .access_outbound(true)
        .reject_remote_clients(true)
        .first_pipe_instance(first)
        .create(name)
}

fn publish_locator(temporary: &Path, destination: &Path, pipe_name: &str) -> Result<()> {
    let contents = serde_json::to_vec(&Locator {
        version: LOCATOR_VERSION,
        pipe: pipe_name.to_owned(),
    })
    .context("failed to encode the control locator")?;
    ensure!(
        contents.len() as u64 <= MAX_LOCATOR_BYTES,
        "control locator exceeds its fixed size bound"
    );
    let publication = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(temporary)
            .context("failed to create the temporary control locator")?;
        file.write_all(&contents)
            .context("failed to write the temporary control locator")?;
        file.sync_all()
            .context("failed to complete the temporary control locator")?;
        drop(file);
        fs::rename(temporary, destination)
            .context("failed to atomically publish the completed control locator")
    })();
    if publication.is_err() {
        let _ = remove_if_present(temporary);
    }
    publication
}

fn read_locator(path: &Path) -> Result<String> {
    let file = File::open(path).context("failed to open the control locator")?;
    let length = file
        .metadata()
        .context("failed to inspect the control locator")?
        .len();
    ensure!(
        length <= MAX_LOCATOR_BYTES,
        "control locator exceeds its fixed size bound"
    );
    let mut contents =
        Vec::with_capacity(usize::try_from(length).expect("bounded locator length must fit usize"));
    file.take(MAX_LOCATOR_BYTES + 1)
        .read_to_end(&mut contents)
        .context("failed to read the control locator")?;
    ensure!(
        contents.len() as u64 == length,
        "control locator changed while it was read"
    );
    let locator: Locator =
        serde_json::from_slice(&contents).context("control locator has invalid syntax")?;
    ensure!(
        locator.version == LOCATOR_VERSION,
        "control locator version is unsupported"
    );
    validate_pipe_name(&locator.pipe)?;
    Ok(locator.pipe)
}

fn validate_pipe_name(name: &str) -> Result<()> {
    let Some(suffix) = name.strip_prefix(PIPE_PREFIX) else {
        anyhow::bail!("control locator does not name a local MCServerNap pipe");
    };
    ensure!(
        suffix.len() == 32
            && suffix
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "control locator pipe suffix is invalid"
    );
    Ok(())
}

fn remove_if_present(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicU64, Ordering};

    use tokio::io::AsyncWriteExt as _;
    use tokio::sync::oneshot;
    use tokio::time::{Duration, Instant, advance};

    use super::*;
    use crate::control::{
        ControlServer, PreparedRegistry, ReadFrame, Request, Response, SuccessResult, encode_frame,
        read_frame,
    };
    use crate::runtime::RuntimeControlHandle;
    use crate::supervisor::{LifecycleState, ServerPhase};

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

    fn temporary_directory() -> PathBuf {
        let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "mcservernap-control-windows-test-{}-{id}",
            std::process::id()
        ))
    }

    fn test_registry() -> (
        crate::control::Registry,
        tokio::sync::watch::Sender<LifecycleState>,
    ) {
        let id = "survival".to_owned();
        let prepared = PreparedRegistry::new([&id].into_iter()).unwrap();
        let (handle, authority) =
            RuntimeControlHandle::test(&id, LifecycleState::test_phase(ServerPhase::Stopped));
        (
            prepared.activate(BTreeMap::from([(id, handle)])).unwrap(),
            authority,
        )
    }

    #[test]
    fn local_app_data_discovery_uses_the_known_folder_wrapper() {
        let known_folder = dirs::data_local_dir().unwrap();
        let paths = endpoint_paths().unwrap();
        assert_eq!(paths.directory, known_folder.join("MCServerNap"));
    }

    #[test]
    fn locator_parser_is_strict_and_local_only() {
        let directory = temporary_directory();
        fs::create_dir(&directory).unwrap();
        let locator = directory.join("locator");
        let valid_pipe = random_pipe_name().unwrap();
        let invalid = [
            br#"{"version":2,"pipe":"\\\\.\\pipe\\mcservernap-control-00000000000000000000000000000000"}"#.as_slice(),
            br#"{"version":1,"pipe":"\\\\server\\pipe\\mcservernap-control-00000000000000000000000000000000"}"#,
            br#"{"version":1,"pipe":"\\\\.\\pipe\\mcservernap-control-ABCDEF00000000000000000000000000"}"#,
            br#"{"version":1,"pipe":"\\\\.\\pipe\\mcservernap-control-short"}"#,
            br#"{"version":1,"pipe":"\\\\.\\pipe\\mcservernap-control-00000000000000000000000000000000","extra":1}"#,
            br#"{"version":1,"pipe":"\\\\.\\pipe\\mcservernap-control-00000000000000000000000000000000"} trailing"#,
            br#"{"version":1,"version":1,"pipe":"\\\\.\\pipe\\mcservernap-control-00000000000000000000000000000000"}"#,
        ];
        for contents in invalid {
            fs::write(&locator, contents).unwrap();
            assert!(read_locator(&locator).is_err());
        }
        fs::write(
            &locator,
            serde_json::to_vec(&Locator {
                version: LOCATOR_VERSION,
                pipe: valid_pipe.clone(),
            })
            .unwrap(),
        )
        .unwrap();
        assert_eq!(read_locator(&locator).unwrap(), valid_pipe);
        fs::remove_dir_all(directory).ok();
    }

    #[test]
    fn locator_publication_is_completed_then_atomically_placed() {
        let directory = temporary_directory();
        fs::create_dir(&directory).unwrap();
        let temporary = directory.join("temporary");
        let destination = directory.join("destination");
        let pipe = random_pipe_name().unwrap();
        publish_locator(&temporary, &destination, &pipe).unwrap();
        assert!(!temporary.exists());
        assert_eq!(read_locator(&destination).unwrap(), pipe);
        fs::remove_dir_all(directory).ok();
    }

    #[test]
    fn share_mode_lock_rejects_a_second_daemon() {
        let directory = temporary_directory();
        let paths = paths_in(directory.clone());
        let first = InstanceLock::acquire_at(paths.clone()).unwrap();
        let error = InstanceLock::acquire_at(paths).err().unwrap().to_string();
        assert!(error.contains("another MCServerNap daemon"));
        drop(first);
        assert!(directory.join("control.lock").is_file());
        fs::remove_dir_all(directory).ok();
    }

    #[tokio::test]
    async fn stale_locator_is_replaced_only_after_first_pipe_exists() {
        let directory = temporary_directory();
        let paths = paths_in(directory.clone());
        fs::create_dir(&directory).unwrap();
        fs::write(&paths.locator, b"stale").unwrap();
        let lock = InstanceLock::acquire_at(paths.clone()).unwrap();
        let mut endpoint = lock.bind().unwrap();
        let published = read_locator(&paths.locator).unwrap();
        assert_eq!(published, endpoint.pipe_name);
        let client = ClientOptions::new().open(&published).unwrap();
        drop(client);
        endpoint.stop().unwrap();
        assert!(!paths.locator.exists());
        assert!(paths.lock.exists());
        drop(lock);
        fs::remove_dir_all(directory).ok();
    }

    #[tokio::test]
    async fn first_pipe_instance_excludes_an_existing_name() {
        let name = random_pipe_name().unwrap();
        let first = create_pipe(&name, true).unwrap();
        assert!(create_pipe(&name, true).is_err());
        drop(first);
    }

    #[tokio::test]
    async fn same_principal_duplex_exchange_uses_default_client_security_qos() {
        let directory = temporary_directory();
        let paths = paths_in(directory.clone());
        let lock = InstanceLock::acquire_at(paths.clone()).unwrap();
        let endpoint = lock.bind().unwrap();
        let pipe_name = endpoint.pipe_name.clone();
        let (registry, _authority) = test_registry();
        let (stop_sender, stop_receiver) = oneshot::channel();
        let (stopped_sender, stopped_receiver) = oneshot::channel();
        let server =
            tokio::spawn(ControlServer::new(endpoint, registry).run(stop_receiver, stopped_sender));

        let mut malformed = connect_pipe(&pipe_name, Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        malformed.write_all(&[0, 0, 0, 1]).await.unwrap();
        let ReadFrame::Version { version, body } = read_frame(&mut malformed).await.unwrap();
        assert_eq!(version, 2);
        assert!(matches!(
            serde_json::from_slice::<Response>(&body).unwrap(),
            Response::Error { .. }
        ));
        drop(malformed);

        let mut client = connect_pipe(&pipe_name, Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        client
            .write_all(&encode_frame(&Request::List).unwrap())
            .await
            .unwrap();
        let ReadFrame::Version { version, body } = read_frame(&mut client).await.unwrap();
        assert_eq!(version, 2);
        assert!(matches!(
            serde_json::from_slice::<Response>(&body).unwrap(),
            Response::Success {
                result: SuccessResult::List { .. }
            }
        ));
        drop(client);
        stop_sender.send(()).unwrap();
        stopped_receiver.await.unwrap();
        server.await.unwrap().unwrap();
        assert!(!paths.locator.exists());
        assert!(paths.lock.exists());
        drop(lock);
        fs::remove_dir_all(directory).ok();
    }

    #[tokio::test(start_paused = true)]
    async fn pipe_busy_retries_share_the_original_client_deadline() {
        let name = random_pipe_name().unwrap();
        let server = create_pipe(&name, true).unwrap();
        let occupying_client = ClientOptions::new().open(&name).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        let attempt = tokio::spawn(async move { connect_pipe(&name, deadline).await });
        tokio::task::yield_now().await;
        advance(Duration::from_secs(5)).await;
        let error = attempt.await.unwrap().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        drop((occupying_client, server));
    }

    #[tokio::test]
    async fn missing_or_stale_pipe_is_unavailable() {
        let missing = random_pipe_name().unwrap();
        assert!(
            connect_pipe(&missing, Instant::now() + Duration::from_secs(5))
                .await
                .is_err()
        );

        let directory = temporary_directory();
        fs::create_dir(&directory).unwrap();
        let locator = directory.join("control.endpoint");
        publish_locator(&directory.join("control.endpoint.tmp"), &locator, &missing).unwrap();
        let stale = read_locator(&locator).unwrap();
        assert!(
            connect_pipe(&stale, Instant::now() + Duration::from_secs(5))
                .await
                .is_err()
        );
        fs::remove_dir_all(directory).ok();
    }

    async fn open_when_ready(name: &str) -> NamedPipeClient {
        for _ in 0..10_000 {
            match ClientOptions::new().open(name) {
                Ok(client) => return client,
                Err(error) if error.raw_os_error() == Some(ERROR_PIPE_BUSY) => {
                    tokio::task::yield_now().await;
                }
                Err(error) => panic!("pipe unexpectedly became unavailable: {error}"),
            }
        }
        panic!("pipe did not create a replacement instance");
    }

    async fn await_rejection(client: &NamedPipeClient) {
        let mut byte = [0];
        for _ in 0..10_000 {
            match client.try_read(&mut byte) {
                Ok(0) => return,
                Ok(_) => panic!("overloaded connection received unexpected data"),
                Err(error)
                    if error.kind() == io::ErrorKind::WouldBlock
                        || matches!(error.raw_os_error(), Some(109 | 233)) =>
                {
                    if matches!(error.raw_os_error(), Some(109 | 233)) {
                        return;
                    }
                    tokio::task::yield_now().await;
                }
                Err(error) => panic!("unexpected overload connection result: {error}"),
            }
        }
        panic!("overloaded connection was not rejected");
    }

    #[tokio::test(start_paused = true)]
    async fn repeated_overload_keeps_a_replacement_accepting_and_shutdown_stops_admission() {
        let directory = temporary_directory();
        let paths = paths_in(directory.clone());
        let lock = InstanceLock::acquire_at(paths.clone()).unwrap();
        let endpoint = lock.bind().unwrap();
        let pipe_name = endpoint.pipe_name.clone();
        let (registry, _authority) = test_registry();
        let (stop_sender, stop_receiver) = oneshot::channel();
        let (stopped_sender, stopped_receiver) = oneshot::channel();
        let server =
            tokio::spawn(ControlServer::new(endpoint, registry).run(stop_receiver, stopped_sender));

        let mut occupied = Vec::new();
        for _ in 0..32 {
            occupied.push(open_when_ready(&pipe_name).await);
        }
        for _ in 0..4 {
            let rejected = open_when_ready(&pipe_name).await;
            await_rejection(&rejected).await;
        }

        stop_sender.send(()).unwrap();
        stopped_receiver.await.unwrap();
        assert!(ClientOptions::new().open(&pipe_name).is_err());
        server.await.unwrap().unwrap();
        drop(occupied);
        assert!(!paths.locator.exists());
        drop(lock);
        fs::remove_dir_all(directory).ok();
    }

    #[tokio::test]
    async fn guarded_shutdown_removes_only_this_daemons_locator() {
        let directory = temporary_directory();
        let paths = paths_in(directory.clone());
        let lock = InstanceLock::acquire_at(paths.clone()).unwrap();
        let mut endpoint = lock.bind().unwrap();
        let other_pipe = random_pipe_name().unwrap();
        fs::write(
            &paths.locator,
            serde_json::to_vec(&Locator {
                version: LOCATOR_VERSION,
                pipe: other_pipe,
            })
            .unwrap(),
        )
        .unwrap();
        assert!(endpoint.stop().is_err());
        assert!(paths.locator.exists());
        fs::remove_file(&paths.locator).unwrap();
        drop(lock);
        fs::remove_dir_all(directory).ok();
    }

    #[test]
    #[ignore = "launched as a control lock inheritance fixture"]
    fn lock_inheritance_fixture() {
        std::thread::sleep(Duration::from_secs(30));
    }

    #[test]
    fn control_lock_handle_does_not_survive_spawn() {
        let directory = temporary_directory();
        let paths = paths_in(directory.clone());
        let lock = InstanceLock::acquire_at(paths.clone()).unwrap();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "control::windows::tests::lock_inheritance_fixture",
                "--quiet",
            ])
            .spawn()
            .unwrap();
        drop(lock);
        let replacement = InstanceLock::acquire_at(paths).unwrap();
        child.kill().ok();
        child.wait().ok();
        drop(replacement);
        fs::remove_dir_all(directory).ok();
    }
}
