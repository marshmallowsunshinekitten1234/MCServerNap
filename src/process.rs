#[cfg(unix)]
use std::os::unix::process::CommandExt as _;
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

pub fn launch(command: &str, arguments: &[String]) -> Result<OwnedProcess> {
    let mut process = Command::new(command);
    process
        .args(arguments)
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
        .with_context(|| format!("failed to launch server command {command:?}"))?;
    let stdin = child
        .stdin
        .take()
        .expect("piped server stdin must be available after launch");
    log::info!("Launched server command {command:?} {arguments:?}");
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
