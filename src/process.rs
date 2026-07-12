#[cfg(unix)]
use std::os::unix::process::CommandExt as _;

use anyhow::{Context, Result};
use tokio::process::{Child, Command};

#[cfg(unix)]
use nix::errno::Errno;
#[cfg(unix)]
use nix::sys::signal::{Signal, killpg};
#[cfg(unix)]
use nix::unistd::Pid;
#[cfg(all(target_os = "windows", not(test)))]
const CREATE_NEW_CONSOLE: u32 = 0x0000_0010;

pub fn launch(command: &str, arguments: &[String]) -> Result<Child> {
    let mut process = Command::new(command);
    process.args(arguments).kill_on_drop(true);

    #[cfg(all(target_os = "windows", not(test)))]
    process.creation_flags(CREATE_NEW_CONSOLE);

    // A dedicated process group lets forced shutdown include launch scripts and descendants.
    #[cfg(unix)]
    process.as_std_mut().process_group(0);

    let child = process
        .spawn()
        .with_context(|| format!("failed to launch server command {command:?}"))?;
    log::info!("Launched server command {command:?} {arguments:?}");
    Ok(child)
}

/// Forcefully terminate the server process tree.
pub async fn terminate(child: &mut Child) -> Result<()> {
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
