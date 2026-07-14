use std::ffi::OsString;
#[cfg(unix)]
use std::os::unix::process::CommandExt as _;
use std::path::PathBuf;
use std::process::Stdio;

use anyhow::{Context, Result};
use tokio::io::AsyncWriteExt as _;
use tokio::process::{Child, ChildStdin, Command};

#[cfg(unix)]
use nix::errno::Errno;
#[cfg(unix)]
use nix::sys::signal::{Signal, killpg};
#[cfg(unix)]
use nix::unistd::Pid;
#[cfg(all(target_os = "windows", not(test)))]
const CREATE_NEW_CONSOLE: u32 = 0x0000_0010;

#[derive(Debug)]
pub struct LaunchCommand {
    pub program: OsString,
    pub arguments: Vec<OsString>,
    pub working_directory: PathBuf,
}

impl LaunchCommand {
    #[must_use]
    pub fn new(program: OsString, arguments: Vec<OsString>, working_directory: PathBuf) -> Self {
        Self {
            program,
            arguments,
            working_directory,
        }
    }
}

pub struct OwnedProcess {
    child: Child,
    stdin: ChildStdin,
}

impl OwnedProcess {
    pub async fn wait(&mut self) -> std::io::Result<std::process::ExitStatus> {
        self.child.wait().await
    }

    pub fn try_wait(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
        self.child.try_wait()
    }

    #[cfg(test)]
    #[must_use]
    pub fn id(&self) -> Option<u32> {
        self.child.id()
    }

    pub async fn send_console_stop(&mut self) -> Result<()> {
        self.stdin
            .write_all(b"stop\n")
            .await
            .context("failed to write the console stop command")?;
        self.stdin
            .flush()
            .await
            .context("failed to flush the console stop command")
    }
}

#[allow(
    clippy::unnecessary_debug_formatting,
    reason = "OS command values need escaped, non-lossy debug logging"
)]
pub fn launch(launch: &LaunchCommand) -> Result<OwnedProcess> {
    let mut process = Command::new(&launch.program);
    process
        .args(&launch.arguments)
        .current_dir(&launch.working_directory)
        .stdin(Stdio::piped())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);

    #[cfg(all(target_os = "windows", not(test)))]
    process.creation_flags(CREATE_NEW_CONSOLE);

    // A dedicated process group lets forced shutdown include launch scripts and descendants.
    #[cfg(unix)]
    process.as_std_mut().process_group(0);

    let mut child = process
        .spawn()
        .with_context(|| format!("failed to launch server command {:?}", launch.program))?;
    let stdin = child
        .stdin
        .take()
        .expect("piped server stdin must be available after launch");
    log::info!(
        "Launched server command {:?} {:?}",
        launch.program,
        launch.arguments
    );
    Ok(OwnedProcess { child, stdin })
}

/// Forcefully terminate the server process tree.
pub async fn terminate(process: &mut OwnedProcess) -> Result<()> {
    let child = &mut process.child;
    if child
        .try_wait()
        .context("failed to query the server process")?
        .is_some()
    {
        return Ok(());
    }

    #[cfg(target_os = "windows")]
    {
        let process_id = child.id().context("server process has no process ID")?;
        let status = Command::new("taskkill")
            .args(["/F", "/T", "/PID", &process_id.to_string()])
            .status()
            .await
            .context("failed to invoke taskkill")?;

        if !status.success()
            && child
                .try_wait()
                .context("failed to query the server process after taskkill")?
                .is_none()
        {
            child
                .kill()
                .await
                .context("taskkill failed and the direct process kill also failed")?;
        }
    }

    #[cfg(unix)]
    {
        let process_id = child
            .id()
            .context("server process has no process ID")?
            .try_into()
            .context("server process ID exceeds the Unix PID range")?;
        match killpg(Pid::from_raw(process_id), Signal::SIGKILL) {
            Ok(()) | Err(Errno::ESRCH) => {}
            Err(error) => {
                log::warn!(
                    "Failed to terminate server process group ({error}); killing the direct child"
                );
                child
                    .kill()
                    .await
                    .context("process-group and direct process termination both failed")?;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Read as _;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use tokio::time::timeout;

    use super::*;

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

    fn temporary_directory() -> PathBuf {
        let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "mcservernap-process-test-{}-{id}",
            std::process::id()
        ))
    }

    fn fixture_launch(test_name: &str, working_directory: PathBuf) -> LaunchCommand {
        let executable = std::env::current_exe().expect("test executable path should be known");
        LaunchCommand::new(
            executable.into_os_string(),
            vec![
                "--ignored".into(),
                "--exact".into(),
                test_name.into(),
                "--quiet".into(),
            ],
            working_directory,
        )
    }

    #[test]
    #[ignore = "launched as a structured process test fixture"]
    fn structured_launch_fixture() {
        let expected: Vec<OsString> = vec![
            "--ignored".into(),
            "--exact".into(),
            "process::tests::structured_launch_fixture".into(),
            "--quiet".into(),
        ];
        assert_eq!(std::env::args_os().skip(1).collect::<Vec<_>>(), expected);

        let mut command = [0; 5];
        std::io::stdin()
            .read_exact(&mut command)
            .expect("fixture should receive its console command");
        assert_eq!(&command, b"stop\n");
        fs::write("launch-observed", b"ok")
            .expect("fixture should write in its configured working directory");
    }

    #[test]
    #[ignore = "launched as a forced process termination fixture"]
    fn forced_termination_fixture() {
        std::thread::sleep(Duration::from_secs(30));
    }

    #[tokio::test]
    async fn structured_launch_preserves_program_arguments_working_directory_and_stdin() {
        let directory = temporary_directory();
        fs::create_dir(&directory).expect("test working directory should be created");
        let launch = fixture_launch(
            "process::tests::structured_launch_fixture",
            directory.clone(),
        );
        let expected_program = std::env::current_exe()
            .expect("test executable path should be known")
            .into_os_string();

        assert_eq!(&launch.program, &expected_program);
        let mut child = super::launch(&launch).expect("structured command should launch");
        child
            .send_console_stop()
            .await
            .expect("console stop should preserve exact stdin ownership");
        let status = timeout(Duration::from_secs(5), child.wait())
            .await
            .expect("fixture exit should be bounded")
            .expect("fixture should be waitable");

        assert!(status.success());
        assert_eq!(
            fs::read(directory.join("launch-observed"))
                .expect("fixture should write relative to the configured directory"),
            b"ok"
        );
        fs::remove_dir_all(directory).ok();
    }

    #[tokio::test]
    async fn structured_launch_retains_forced_termination() {
        let directory = temporary_directory();
        fs::create_dir(&directory).expect("test working directory should be created");
        let launch = fixture_launch(
            "process::tests::forced_termination_fixture",
            directory.clone(),
        );
        let mut child = super::launch(&launch).expect("termination fixture should launch");

        terminate(&mut child)
            .await
            .expect("structured launch should retain process-tree termination");
        timeout(Duration::from_secs(5), child.wait())
            .await
            .expect("terminated fixture should be reaped promptly")
            .expect("terminated fixture should remain waitable");
        fs::remove_dir_all(directory).ok();
    }
}
