use std::ffi::OsString;
#[cfg(unix)]
use std::os::unix::process::CommandExt as _;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use tokio::io::AsyncWriteExt as _;
use tokio::process::{Child, ChildStdin, Command};

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
    removed_environment: Arc<[OsString]>,
}

impl LaunchCommand {
    #[must_use]
    pub fn new(program: OsString, arguments: Vec<OsString>, working_directory: PathBuf) -> Self {
        Self {
            program,
            arguments,
            working_directory,
            removed_environment: Arc::default(),
        }
    }

    #[must_use]
    pub(crate) fn with_removed_environment(mut self, names: Arc<[OsString]>) -> Self {
        self.removed_environment = names;
        self
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
    for name in launch.removed_environment.iter() {
        process.env_remove(name);
    }

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
        let tree_result = Command::new("taskkill")
            .args(["/F", "/T", "/PID", &process_id.to_string()])
            .status()
            .await;
        match tree_result {
            Ok(status) if status.success() => {}
            Ok(status) => {
                return terminate_direct_after_tree_failure(
                    child,
                    anyhow!("taskkill /T exited with status {status}"),
                )
                .await;
            }
            Err(error) => {
                return terminate_direct_after_tree_failure(
                    child,
                    anyhow!(error).context("failed to invoke taskkill /T"),
                )
                .await;
            }
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
            Ok(()) => {}
            Err(error) => {
                return terminate_direct_after_tree_failure(
                    child,
                    anyhow!("failed to terminate server process group: {error}"),
                )
                .await;
            }
        }
    }

    Ok(())
}

async fn terminate_direct_after_tree_failure(
    child: &mut Child,
    tree_error: anyhow::Error,
) -> Result<()> {
    match child.try_wait() {
        Ok(Some(_)) => Err(tree_error.context(
            "the direct server process already exited, but descendant cleanup remains uncertain",
        )),
        Ok(None) => match child.kill().await {
            Ok(()) => Err(tree_error.context(
                "direct server process termination was requested, but descendant cleanup remains uncertain",
            )),
            Err(error) => Err(tree_error.context(format!(
                "direct server process termination also failed: {error}; descendant cleanup remains uncertain"
            ))),
        },
        Err(query_error) => match child.kill().await {
            Ok(()) => Err(tree_error.context(format!(
                "failed to query the direct server process ({query_error}); direct termination was requested, but descendant cleanup remains uncertain"
            ))),
            Err(kill_error) => Err(tree_error.context(format!(
                "failed to query the direct server process ({query_error}) and direct termination also failed ({kill_error}); descendant cleanup remains uncertain"
            ))),
        },
    }
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

    #[test]
    #[ignore = "launched inside an environment-removal subprocess"]
    fn removed_environment_child_fixture() {
        assert!(std::env::var_os("MCSERVERNAP_TEST_SECRET_A").is_none());
        assert!(std::env::var_os("MCSERVERNAP_TEST_SECRET_B").is_none());
        let output = std::env::args_os()
            .find_map(|argument| {
                argument
                    .to_string_lossy()
                    .strip_prefix("environment-output=")
                    .map(PathBuf::from)
            })
            .expect("child fixture should receive its output path");
        fs::write(output, b"removed").expect("child fixture should report success");
    }

    #[test]
    #[ignore = "launched with private environment from the parent test"]
    fn removed_environment_parent_fixture() {
        assert_eq!(
            std::env::var_os("MCSERVERNAP_TEST_SECRET_A"),
            Some(OsString::from("sentinel-a"))
        );
        assert_eq!(
            std::env::var_os("MCSERVERNAP_TEST_SECRET_B"),
            Some(OsString::from("sentinel-b"))
        );
        let output = std::env::args_os()
            .find_map(|argument| {
                argument
                    .to_string_lossy()
                    .strip_prefix("environment-output=")
                    .map(PathBuf::from)
            })
            .expect("parent fixture should receive its output path");
        let executable = std::env::current_exe().unwrap();
        let launch = LaunchCommand::new(
            executable.into_os_string(),
            vec![
                "--ignored".into(),
                "--exact".into(),
                "process::tests::removed_environment_child_fixture".into(),
                format!("environment-output={}", output.display()).into(),
                "--quiet".into(),
            ],
            std::env::current_dir().unwrap(),
        )
        .with_removed_environment(
            vec![
                OsString::from("MCSERVERNAP_TEST_SECRET_A"),
                OsString::from("MCSERVERNAP_TEST_SECRET_B"),
            ]
            .into(),
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let mut child = super::launch(&launch).unwrap();
            let status = child.wait().await.unwrap();
            assert!(status.success());
        });
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

    #[tokio::test]
    async fn direct_child_fallback_preserves_uncertain_process_tree_failure() {
        let directory = temporary_directory();
        fs::create_dir(&directory).unwrap();
        let launch = fixture_launch(
            "process::tests::forced_termination_fixture",
            directory.clone(),
        );
        let mut process = super::launch(&launch).unwrap();

        let error = terminate_direct_after_tree_failure(
            &mut process.child,
            anyhow!("simulated process-tree termination failure"),
        )
        .await
        .expect_err("direct-child fallback must retain uncertain descendant cleanup");
        let message = format!("{error:#}");
        assert!(message.contains("simulated process-tree termination failure"));
        assert!(message.contains("descendant cleanup remains uncertain"));
        timeout(Duration::from_secs(5), process.wait())
            .await
            .expect("direct child should be reaped after fallback")
            .expect("direct child should remain waitable");
        fs::remove_dir_all(directory).ok();
    }

    #[test]
    fn configured_secret_variables_are_removed_from_every_child_environment() {
        let directory = temporary_directory();
        fs::create_dir(&directory).unwrap();
        let output = directory.join("environment-observed");
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                OsString::from("--ignored"),
                OsString::from("--exact"),
                OsString::from("process::tests::removed_environment_parent_fixture"),
                format!("environment-output={}", output.display()).into(),
                OsString::from("--quiet"),
            ])
            .env("MCSERVERNAP_TEST_SECRET_A", "sentinel-a")
            .env("MCSERVERNAP_TEST_SECRET_B", "sentinel-b")
            .status()
            .expect("environment parent fixture should launch");
        assert!(status.success());
        assert_eq!(fs::read(&output).unwrap(), b"removed");
        fs::remove_dir_all(directory).ok();
    }
}
