use std::fs::{self, File, Permissions};
use std::io;
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::{
    DirBuilderExt as _, FileTypeExt as _, MetadataExt as _, PermissionsExt as _,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context as _, Result, ensure};
use nix::errno::Errno;
use nix::fcntl::{Flock, FlockArg, OFlag, open};
use nix::sys::stat::Mode;
use nix::unistd::Uid;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tokio::time::Instant;

pub(crate) type ControlStream = UnixStream;

#[derive(Clone)]
struct EndpointPaths {
    socket: PathBuf,
    lock: PathBuf,
}

pub(crate) struct InstanceLock {
    _lock: Flock<File>,
    paths: EndpointPaths,
}

pub(crate) struct BoundEndpoint {
    listener: Option<UnixListener>,
    socket: PathBuf,
    identity: SocketIdentity,
}

#[derive(Clone, Copy)]
struct SocketIdentity {
    device: u64,
    inode: u64,
}

impl InstanceLock {
    pub(crate) fn acquire() -> Result<Self> {
        let effective_uid = Uid::effective().as_raw();
        let paths = discover_paths(effective_uid)?;
        prepare_endpoint_directory(&paths, effective_uid, true)?;
        Self::acquire_at(paths, effective_uid)
    }

    fn acquire_at(paths: EndpointPaths, effective_uid: u32) -> Result<Self> {
        let file = open_lock_file(&paths.lock, effective_uid)?;
        let lock = Flock::lock(file, FlockArg::LockExclusiveNonblock).map_err(|(_, error)| {
            if matches!(error, Errno::EAGAIN) {
                anyhow::anyhow!(
                    "another MCServerNap daemon already owns the control endpoint for this OS principal"
                )
            } else {
                anyhow::anyhow!("failed to lock the per-principal control instance: {error}")
            }
        })?;
        Ok(Self { _lock: lock, paths })
    }

    pub(crate) fn bind(&self) -> Result<BoundEndpoint> {
        recover_stale_socket(&self.paths.socket, Uid::effective().as_raw())?;
        let listener = UnixListener::bind(&self.paths.socket)
            .context("failed to bind the per-principal control socket")?;
        let metadata = fs::symlink_metadata(&self.paths.socket)
            .context("failed to inspect the bound control socket")?;
        ensure!(
            metadata.file_type().is_socket(),
            "bound control endpoint is not a Unix socket"
        );
        let mut endpoint = BoundEndpoint {
            listener: Some(listener),
            socket: self.paths.socket.clone(),
            identity: SocketIdentity {
                device: metadata.dev(),
                inode: metadata.ino(),
            },
        };
        if let Err(error) = fs::set_permissions(&self.paths.socket, Permissions::from_mode(0o600)) {
            let permission_error = anyhow::Error::new(error)
                .context("failed to set control socket permissions to 0600");
            return match endpoint.stop() {
                Ok(()) => Err(permission_error),
                Err(cleanup_error) => Err(permission_error.context(format!(
                    "control socket cleanup also failed: {cleanup_error:#}"
                ))),
            };
        }
        Ok(endpoint)
    }
}

impl BoundEndpoint {
    pub(super) async fn accept(
        &mut self,
        permits: &Arc<Semaphore>,
    ) -> io::Result<Option<(ControlStream, OwnedSemaphorePermit)>> {
        let listener = self
            .listener
            .as_ref()
            .ok_or_else(|| io::Error::other("control listener is not active"))?;
        let (stream, _) = listener.accept().await?;
        match Arc::clone(permits).try_acquire_owned() {
            Ok(permit) => Ok(Some((stream, permit))),
            Err(TryAcquireError::NoPermits) => Ok(None),
            Err(TryAcquireError::Closed) => {
                Err(io::Error::other("control exchange limiter closed"))
            }
        }
    }

    pub(crate) fn stop(&mut self) -> Result<()> {
        let Some(listener) = self.listener.take() else {
            return Ok(());
        };
        drop(listener);
        let metadata = fs::symlink_metadata(&self.socket)
            .context("control socket disappeared before guarded cleanup")?;
        ensure!(
            metadata.file_type().is_socket()
                && metadata.dev() == self.identity.device
                && metadata.ino() == self.identity.inode,
            "control socket identity changed before guarded cleanup"
        );
        fs::remove_file(&self.socket).context("failed to remove the stopped control socket")
    }
}

impl Drop for BoundEndpoint {
    fn drop(&mut self) {
        if let Err(error) = self.stop() {
            log::warn!("Control endpoint cleanup failed: {error}");
        }
    }
}

pub(crate) async fn connect(_deadline: Instant) -> io::Result<ControlStream> {
    let paths = tokio::task::spawn_blocking(|| {
        let effective_uid = Uid::effective().as_raw();
        let paths = discover_paths(effective_uid)?;
        prepare_endpoint_directory(&paths, effective_uid, false)?;
        Ok::<_, anyhow::Error>(paths)
    })
    .await
    .map_err(|_| io::Error::other("control endpoint discovery task failed"))?
    .map_err(|error| io::Error::other(error.to_string()))?;
    UnixStream::connect(paths.socket).await
}

fn discover_paths(effective_uid: u32) -> Result<EndpointPaths> {
    let parent = PathBuf::from(format!("/tmp/mcservernap-{effective_uid}"));
    let socket = parent.join("control.sock");
    validate_socket_path(&socket)?;
    Ok(EndpointPaths {
        lock: parent.join("control.lock"),
        socket,
    })
}

fn prepare_endpoint_directory(
    paths: &EndpointPaths,
    effective_uid: u32,
    create: bool,
) -> Result<()> {
    let parent = paths
        .socket
        .parent()
        .expect("fixed control socket path must have a parent");
    prepare_private_directory(parent, effective_uid, create)
}

fn prepare_private_directory(path: &Path, effective_uid: u32, create: bool) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound && create => {
            let mut builder = fs::DirBuilder::new();
            builder.mode(0o700);
            builder
                .create(path)
                .context("failed to create the private control directory")?;
            fs::set_permissions(path, Permissions::from_mode(0o700))
                .context("failed to set private control directory permissions")?;
        }
        Err(error) => {
            return Err(error).context("private control directory is unavailable");
        }
    }
    validate_private_directory(path, effective_uid)
}

fn validate_private_directory(path: &Path, effective_uid: u32) -> Result<()> {
    let metadata = fs::symlink_metadata(path).context("failed to inspect control directory")?;
    ensure!(
        !metadata.file_type().is_symlink() && metadata.file_type().is_dir(),
        "control directory must be a real directory, not a symlink"
    );
    ensure!(
        metadata.uid() == effective_uid,
        "control directory is not owned by the effective UID"
    );
    let mode = metadata.mode() & 0o777;
    ensure!(mode == 0o700, "control directory must have mode 0700");
    Ok(())
}

fn validate_socket_path(path: &Path) -> Result<()> {
    let bytes = path.as_os_str().as_bytes();
    ensure!(
        !bytes.contains(&0),
        "control socket path contains a NUL byte"
    );
    let maximum = std::mem::size_of::<nix::libc::sockaddr_un>()
        - std::mem::offset_of!(nix::libc::sockaddr_un, sun_path)
        - 1;
    ensure!(
        bytes.len() <= maximum,
        "control socket path is {} bytes; this platform supports at most {maximum}",
        bytes.len()
    );
    Ok(())
}

fn open_lock_file(path: &Path, effective_uid: u32) -> Result<File> {
    let common = OFlag::O_RDWR | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW;
    let mode = Mode::S_IRUSR | Mode::S_IWUSR;
    let (descriptor, created) = match open(path, common | OFlag::O_CREAT | OFlag::O_EXCL, mode) {
        Ok(descriptor) => (descriptor, true),
        Err(Errno::EEXIST) => (
            open(path, common, mode)
                .context("failed to open the persistent control lock without following symlinks")?,
            false,
        ),
        Err(error) => {
            return Err(error).context("failed to create the persistent control lock");
        }
    };
    let file = File::from(descriptor);
    let metadata = file
        .metadata()
        .context("failed to inspect the persistent control lock")?;
    ensure!(
        metadata.file_type().is_file() && metadata.uid() == effective_uid,
        "control lock must be a regular file owned by the effective UID"
    );
    if created {
        file.set_permissions(Permissions::from_mode(0o600))
            .context("failed to set control lock permissions")?;
    } else {
        ensure!(
            metadata.mode() & 0o777 == 0o600,
            "existing control lock must have mode 0600"
        );
    }
    Ok(file)
}

fn recover_stale_socket(path: &Path, effective_uid: u32) -> Result<()> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error).context("failed to inspect the control socket path"),
    };
    ensure!(
        !metadata.file_type().is_symlink(),
        "refusing to replace a control socket symlink"
    );
    ensure!(
        metadata.file_type().is_socket(),
        "refusing to replace a non-socket control endpoint entry"
    );
    ensure!(
        metadata.uid() == effective_uid,
        "refusing to replace a control socket owned by another UID"
    );
    fs::remove_file(path).context("failed to remove the stale owner-controlled control socket")
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::symlink;
    use std::os::unix::net::UnixListener as StdUnixListener;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use super::*;

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

    fn temporary_directory() -> PathBuf {
        let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "mcservernap-control-unix-test-{}-{id}",
            std::process::id()
        ))
    }

    fn private_directory() -> PathBuf {
        let directory = temporary_directory();
        fs::create_dir(&directory).unwrap();
        fs::set_permissions(&directory, Permissions::from_mode(0o700)).unwrap();
        directory
    }

    fn test_paths(directory: &Path) -> EndpointPaths {
        EndpointPaths {
            socket: directory.join("control.sock"),
            lock: directory.join("control.lock"),
        }
    }

    #[test]
    fn unix_location_is_fixed_per_effective_uid() {
        let paths = discover_paths(1_234).unwrap();
        assert_eq!(
            paths.lock,
            PathBuf::from("/tmp/mcservernap-1234/control.lock")
        );
        assert_eq!(
            paths.socket,
            PathBuf::from("/tmp/mcservernap-1234/control.sock")
        );
    }

    #[test]
    fn daemon_and_client_discovery_derive_the_same_paths() {
        let uid = Uid::effective().as_raw();
        let daemon = discover_paths(uid).unwrap();
        let client = discover_paths(uid).unwrap();
        assert_eq!(daemon.lock, client.lock);
        assert_eq!(daemon.socket, client.socket);
    }

    #[test]
    #[ignore = "launched with alternate discovery environments"]
    fn fixed_discovery_environment_fixture() {
        let uid = Uid::effective().as_raw();
        let paths = discover_paths(uid).unwrap();
        assert_eq!(
            paths.lock,
            PathBuf::from(format!("/tmp/mcservernap-{uid}/control.lock"))
        );
        assert_eq!(
            paths.socket,
            PathBuf::from(format!("/tmp/mcservernap-{uid}/control.sock"))
        );
    }

    #[test]
    fn environment_cannot_select_another_lock_namespace() {
        for (xdg_runtime_dir, temporary_directory) in [
            ("/tmp/mcservernap-test-xdg-a", "/tmp/mcservernap-test-tmp-a"),
            ("/tmp/mcservernap-test-xdg-b", "/tmp/mcservernap-test-tmp-b"),
        ] {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--ignored",
                    "--exact",
                    "control::unix::tests::fixed_discovery_environment_fixture",
                    "--quiet",
                ])
                .env("XDG_RUNTIME_DIR", xdg_runtime_dir)
                .env("TMPDIR", temporary_directory)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "fixed discovery fixture failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }

    #[test]
    fn newly_created_private_parent_uses_mode_0700() {
        let directory = temporary_directory();
        prepare_private_directory(&directory, Uid::effective().as_raw(), true).unwrap();
        assert_eq!(
            fs::symlink_metadata(&directory).unwrap().mode() & 0o777,
            0o700
        );
        fs::remove_dir_all(directory).ok();
    }

    #[test]
    fn parent_ownership_mode_and_symlink_are_validated() {
        let uid = Uid::effective().as_raw();
        let directory = private_directory();
        assert!(validate_private_directory(&directory, uid).is_ok());
        assert!(validate_private_directory(&directory, uid.wrapping_add(1)).is_err());
        fs::set_permissions(&directory, Permissions::from_mode(0o750)).unwrap();
        assert!(validate_private_directory(&directory, uid).is_err());
        fs::set_permissions(&directory, Permissions::from_mode(0o700)).unwrap();
        let link = directory.with_extension("link");
        symlink(&directory, &link).unwrap();
        assert!(validate_private_directory(&link, uid).is_err());
        fs::remove_file(link).ok();
        fs::remove_dir_all(directory).ok();
    }

    #[test]
    fn lock_is_persistent_regular_mode_0600_and_rejects_symlinks() {
        let directory = private_directory();
        let uid = Uid::effective().as_raw();
        let paths = test_paths(&directory);
        let lock = InstanceLock::acquire_at(paths.clone(), uid).unwrap();
        let metadata = fs::symlink_metadata(&paths.lock).unwrap();
        assert!(metadata.file_type().is_file());
        assert_eq!(metadata.mode() & 0o777, 0o600);
        drop(lock);
        assert!(paths.lock.exists());
        fs::remove_file(&paths.lock).unwrap();
        let target = directory.join("target");
        fs::write(&target, b"").unwrap();
        symlink(&target, &paths.lock).unwrap();
        assert!(InstanceLock::acquire_at(paths, uid).is_err());
        fs::remove_dir_all(directory).ok();
    }

    #[test]
    fn same_uid_lock_attempts_share_one_namespace_and_the_second_is_rejected() {
        let directory = private_directory();
        let uid = Uid::effective().as_raw();
        let daemon_paths = test_paths(&directory);
        let competing_paths = daemon_paths.clone();
        assert_eq!(daemon_paths.lock, competing_paths.lock);
        assert_eq!(daemon_paths.socket, competing_paths.socket);
        let first = InstanceLock::acquire_at(daemon_paths, uid).unwrap();
        let error = InstanceLock::acquire_at(competing_paths, uid)
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("another MCServerNap daemon"));
        drop(first);
        fs::remove_dir_all(directory).ok();
    }

    #[test]
    fn stale_socket_recovers_but_non_socket_and_socket_symlink_do_not() {
        let directory = private_directory();
        let uid = Uid::effective().as_raw();
        let socket = directory.join("control.sock");
        let stale = StdUnixListener::bind(&socket).unwrap();
        drop(stale);
        recover_stale_socket(&socket, uid).unwrap();
        assert!(!socket.exists());

        fs::write(&socket, b"not a socket").unwrap();
        assert!(recover_stale_socket(&socket, uid).is_err());
        fs::remove_file(&socket).unwrap();
        let target = directory.join("target.sock");
        let target_listener = StdUnixListener::bind(&target).unwrap();
        symlink(&target, &socket).unwrap();
        assert!(recover_stale_socket(&socket, uid).is_err());
        drop(target_listener);
        fs::remove_dir_all(directory).ok();
    }

    #[tokio::test]
    async fn normal_cleanup_unlinks_exact_socket_and_retains_lock() {
        let directory = private_directory();
        let uid = Uid::effective().as_raw();
        let paths = test_paths(&directory);
        let lock = InstanceLock::acquire_at(paths.clone(), uid).unwrap();
        let mut endpoint = lock.bind().unwrap();
        assert_eq!(
            fs::symlink_metadata(&paths.socket).unwrap().mode() & 0o777,
            0o600
        );
        endpoint.stop().unwrap();
        assert!(!paths.socket.exists());
        assert!(paths.lock.exists());
        drop(lock);
        fs::remove_dir_all(directory).ok();
    }

    #[tokio::test]
    async fn missing_or_stale_socket_is_unavailable() {
        let directory = private_directory();
        let socket = directory.join("control.sock");
        assert!(UnixStream::connect(&socket).await.is_err());
        let stale = StdUnixListener::bind(&socket).unwrap();
        drop(stale);
        assert!(UnixStream::connect(&socket).await.is_err());
        fs::remove_dir_all(directory).ok();
    }

    #[tokio::test]
    async fn cleanup_refuses_a_replaced_socket_identity() {
        let directory = private_directory();
        let uid = Uid::effective().as_raw();
        let paths = test_paths(&directory);
        let lock = InstanceLock::acquire_at(paths.clone(), uid).unwrap();
        let mut endpoint = lock.bind().unwrap();
        fs::remove_file(&paths.socket).unwrap();
        let replacement = StdUnixListener::bind(&paths.socket).unwrap();
        assert!(endpoint.stop().is_err());
        assert!(paths.socket.exists());
        drop(replacement);
        fs::remove_file(&paths.socket).ok();
        drop(lock);
        fs::remove_dir_all(directory).ok();
    }

    #[test]
    fn socket_path_length_is_rejected_before_binding() {
        let directory = PathBuf::from("/").join("x".repeat(1_000));
        let error = validate_socket_path(&directory).unwrap_err().to_string();
        assert!(error.contains("supports at most"));
    }

    #[test]
    #[ignore = "launched as a control lock exec inheritance fixture"]
    fn lock_inheritance_fixture() {
        std::thread::sleep(Duration::from_secs(30));
    }

    #[test]
    fn control_lock_descriptor_does_not_survive_exec() {
        let directory = private_directory();
        let uid = Uid::effective().as_raw();
        let paths = test_paths(&directory);
        let lock = InstanceLock::acquire_at(paths.clone(), uid).unwrap();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "control::unix::tests::lock_inheritance_fixture",
                "--quiet",
            ])
            .spawn()
            .unwrap();
        drop(lock);
        let replacement = InstanceLock::acquire_at(paths, uid).unwrap();
        child.kill().ok();
        child.wait().ok();
        drop(replacement);
        fs::remove_dir_all(directory).ok();
    }
}
