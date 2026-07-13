use std::sync::{Arc, Mutex};

use tokio::sync::{Notify, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};
use tokio::time::Instant;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BackendCycle(u64);

impl BackendCycle {
    pub(crate) const fn from_launch_generation(launch_generation: u64) -> Self {
        Self(launch_generation)
    }
}

pub(crate) struct BackendUseCoordinator {
    gate: Arc<RwLock<()>>,
    activity: Arc<ActivityTracker>,
}

impl BackendUseCoordinator {
    pub(crate) fn new() -> Self {
        Self {
            gate: Arc::new(RwLock::new(())),
            activity: Arc::new(ActivityTracker {
                state: Mutex::new(None),
                changed: Notify::new(),
            }),
        }
    }

    pub(crate) async fn acquire_shared(&self, cycle: BackendCycle) -> BackendUseLease {
        BackendUseLease {
            _guard: Arc::clone(&self.gate).read_owned().await,
            cycle,
        }
    }

    pub(crate) async fn acquire_exclusive(&self) -> OwnedRwLockWriteGuard<()> {
        Arc::clone(&self.gate).write_owned().await
    }

    pub(crate) fn begin_cycle(&self, cycle: BackendCycle, running_since: Instant) {
        *self.activity.state.lock().expect("activity mutex poisoned") = Some(ActivityState {
            cycle,
            active_login_transfer_sessions: 0,
            running_since,
            last_final_session_disconnect: None,
        });
    }

    pub(crate) fn activity_snapshot(&self) -> Option<CurrentCycleActivity> {
        self.activity
            .state
            .lock()
            .expect("activity mutex poisoned")
            .as_ref()
            .map(ActivityState::snapshot)
    }

    pub(crate) fn activity_notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.activity.changed.notified()
    }

    pub(crate) fn establish_login_transfer_session(
        &self,
        lease: BackendUseLease,
    ) -> Result<LoginTransferProxySession, BackendUseLease> {
        let cycle = lease.cycle();
        {
            let mut activity = self.activity.state.lock().expect("activity mutex poisoned");
            let Some(activity) = activity.as_mut() else {
                return Err(lease);
            };
            if activity.cycle != cycle {
                return Err(lease);
            }
            activity.active_login_transfer_sessions = activity
                .active_login_transfer_sessions
                .checked_add(1)
                .expect("active proxy-session count overflowed");
        }
        self.activity.changed.notify_one();
        Ok(LoginTransferProxySession {
            lease,
            activity: Arc::clone(&self.activity),
        })
    }
}

#[derive(Debug)]
pub(crate) struct BackendUseLease {
    _guard: OwnedRwLockReadGuard<()>,
    cycle: BackendCycle,
}

impl BackendUseLease {
    pub(crate) const fn cycle(&self) -> BackendCycle {
        self.cycle
    }
}

pub(crate) struct LoginTransferProxySession {
    lease: BackendUseLease,
    activity: Arc<ActivityTracker>,
}

impl Drop for LoginTransferProxySession {
    fn drop(&mut self) {
        let cycle = self.lease.cycle();
        {
            let mut activity = self.activity.state.lock().expect("activity mutex poisoned");
            let Some(activity) = activity.as_mut() else {
                return;
            };
            if activity.cycle != cycle {
                return;
            }
            activity.active_login_transfer_sessions = activity
                .active_login_transfer_sessions
                .checked_sub(1)
                .expect("active proxy-session count underflowed");
            if activity.active_login_transfer_sessions == 0 {
                activity.last_final_session_disconnect = Some(Instant::now());
            }
        }
        self.activity.changed.notify_one();
    }
}

struct ActivityTracker {
    state: Mutex<Option<ActivityState>>,
    changed: Notify,
}

struct ActivityState {
    cycle: BackendCycle,
    active_login_transfer_sessions: usize,
    running_since: Instant,
    last_final_session_disconnect: Option<Instant>,
}

impl ActivityState {
    fn snapshot(&self) -> CurrentCycleActivity {
        CurrentCycleActivity {
            cycle: self.cycle,
            active_login_transfer_sessions: self.active_login_transfer_sessions,
            running_since: self.running_since,
            last_final_session_disconnect: self.last_final_session_disconnect,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CurrentCycleActivity {
    cycle: BackendCycle,
    active_login_transfer_sessions: usize,
    running_since: Instant,
    last_final_session_disconnect: Option<Instant>,
}

impl CurrentCycleActivity {
    pub(crate) const fn cycle(self) -> BackendCycle {
        self.cycle
    }

    pub(crate) const fn active_login_transfer_sessions(self) -> usize {
        self.active_login_transfer_sessions
    }

    #[cfg(test)]
    pub(crate) const fn last_final_session_disconnect(self) -> Option<Instant> {
        self.last_final_session_disconnect
    }

    pub(crate) fn proxy_idle_anchor(self) -> Option<Instant> {
        if self.active_login_transfer_sessions == 0 {
            Some(
                self.last_final_session_disconnect
                    .unwrap_or(self.running_since),
            )
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::sync::Arc;

    use tokio::sync::{Barrier, oneshot};
    use tokio::time::Duration;

    use super::*;

    fn cycle(value: u64) -> BackendCycle {
        BackendCycle::from_launch_generation(value)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shared_lease_blocks_exclusive_acquisition() {
        let coordinator = Arc::new(BackendUseCoordinator::new());
        coordinator.begin_cycle(cycle(1), Instant::now());
        let lease = coordinator.acquire_shared(cycle(1)).await;
        let writer_started = Arc::new(Barrier::new(2));
        let (writer_acquired, acquired) = oneshot::channel();
        let task = {
            let coordinator = Arc::clone(&coordinator);
            let writer_started = Arc::clone(&writer_started);
            tokio::spawn(async move {
                writer_started.wait().await;
                let _guard = coordinator.acquire_exclusive().await;
                writer_acquired.send(()).unwrap();
            })
        };

        writer_started.wait().await;
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        drop(lease);
        acquired.await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn queued_writer_prevents_a_later_reader_from_bypassing_it() {
        let coordinator = Arc::new(BackendUseCoordinator::new());
        coordinator.begin_cycle(cycle(1), Instant::now());
        let first_reader = coordinator.acquire_shared(cycle(1)).await;
        let (writer_polling, writer_polled) = oneshot::channel();
        let (release_writer, writer_release) = oneshot::channel();
        let writer = {
            let coordinator = Arc::clone(&coordinator);
            tokio::spawn(async move {
                writer_polling.send(()).unwrap();
                let _guard = coordinator.acquire_exclusive().await;
                writer_release.await.unwrap();
            })
        };
        writer_polled.await.unwrap();
        tokio::task::yield_now().await;

        let later_reader = {
            let coordinator = Arc::clone(&coordinator);
            tokio::spawn(async move { coordinator.acquire_shared(cycle(1)).await })
        };
        tokio::task::yield_now().await;
        drop(first_reader);
        tokio::task::yield_now().await;
        assert!(!later_reader.is_finished());
        release_writer.send(()).unwrap();
        writer.await.unwrap();
        drop(later_reader.await.unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn overlapping_sessions_timestamp_only_the_final_disconnect() {
        let coordinator = BackendUseCoordinator::new();
        let running_since = Instant::now();
        coordinator.begin_cycle(cycle(1), running_since);
        let first = coordinator
            .establish_login_transfer_session(coordinator.acquire_shared(cycle(1)).await)
            .unwrap();
        let second = coordinator
            .establish_login_transfer_session(coordinator.acquire_shared(cycle(1)).await)
            .unwrap();
        assert_eq!(
            coordinator
                .activity_snapshot()
                .unwrap()
                .active_login_transfer_sessions(),
            2
        );

        tokio::time::advance(Duration::from_secs(5)).await;
        drop(first);
        let snapshot = coordinator.activity_snapshot().unwrap();
        assert_eq!(snapshot.active_login_transfer_sessions(), 1);
        assert_eq!(snapshot.last_final_session_disconnect(), None);

        tokio::time::advance(Duration::from_secs(7)).await;
        let disconnected_at = Instant::now();
        drop(second);
        let snapshot = coordinator.activity_snapshot().unwrap();
        assert_eq!(snapshot.active_login_transfer_sessions(), 0);
        assert_eq!(
            snapshot.last_final_session_disconnect(),
            Some(disconnected_at)
        );
        assert_eq!(snapshot.proxy_idle_anchor(), Some(disconnected_at));
    }

    #[tokio::test(start_paused = true)]
    async fn old_cycle_drop_cannot_change_new_cycle_activity() {
        let coordinator = BackendUseCoordinator::new();
        coordinator.begin_cycle(cycle(1), Instant::now());
        let old_session = coordinator
            .establish_login_transfer_session(coordinator.acquire_shared(cycle(1)).await)
            .unwrap();
        tokio::time::advance(Duration::from_secs(1)).await;
        let new_anchor = Instant::now();
        coordinator.begin_cycle(cycle(2), new_anchor);

        tokio::time::advance(Duration::from_secs(1)).await;
        drop(old_session);
        let snapshot = coordinator.activity_snapshot().unwrap();
        assert_eq!(snapshot.cycle(), cycle(2));
        assert_eq!(snapshot.active_login_transfer_sessions(), 0);
        assert_eq!(snapshot.last_final_session_disconnect(), None);
        assert_eq!(snapshot.proxy_idle_anchor(), Some(new_anchor));
    }

    #[tokio::test(start_paused = true)]
    async fn task_abortion_drops_a_session_once() {
        let coordinator = Arc::new(BackendUseCoordinator::new());
        coordinator.begin_cycle(cycle(1), Instant::now());
        let lease = coordinator.acquire_shared(cycle(1)).await;
        let session = coordinator.establish_login_transfer_session(lease).unwrap();
        let (owned, ownership_confirmed) = oneshot::channel();
        let task = tokio::spawn(async move {
            let _session = session;
            owned.send(()).unwrap();
            pending::<()>().await;
        });
        ownership_confirmed.await.unwrap();

        tokio::time::advance(Duration::from_secs(1)).await;
        let disconnected_at = Instant::now();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let snapshot = coordinator.activity_snapshot().unwrap();
        assert_eq!(snapshot.active_login_transfer_sessions(), 0);
        assert_eq!(
            snapshot.last_final_session_disconnect(),
            Some(disconnected_at)
        );
    }
}
