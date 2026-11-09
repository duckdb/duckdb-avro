//! Poison-tolerant locking for the extension crates: `Mutex`, `RwLock` and `Condvar` accessors that recover the guard
//! from a `PoisonError` instead of panicking. Poison-tolerant accessors for the extension crates, which
//! cannot reach (they link only this crate and the C API types); see its module documentation for the invariant
//!: an engine error raised by unwinding passes through the guards on the stack and
//! poisons the locks, and the C++ has no poisoning, so the next access recovers the guard. The lock rule is enforced
//! by `clippy.toml` (`disallowed-methods`): extension code calls `lock_ignore_poison()` and friends, never `lock()`.

// the poison-tolerant accessors are the one place that calls the raw lock methods
#![expect(clippy::disallowed_methods, reason = "the poison-tolerant accessors themselves")]

use std::sync::{
    Arc, Condvar, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard, TryLockError, WaitTimeoutResult,
};
use std::thread::Scope;
use std::time::Duration;

/// Locks `mutex`, recovering the guard when the lock is poisoned (see the module documentation).
#[inline]
pub fn lock_ignore_poison<T: ?Sized>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Read-locks `lock`, recovering the guard when the lock is poisoned.
#[inline]
pub fn read_ignore_poison<T: ?Sized>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Write-locks `lock`, recovering the guard when the lock is poisoned.
#[inline]
pub fn write_ignore_poison<T: ?Sized>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Poison-tolerant `Mutex` accessors.
pub trait MutexExt<T: ?Sized> {
    /// `lock()`, recovering the guard when the lock is poisoned.
    fn lock_ignore_poison(&self) -> MutexGuard<'_, T>;
    /// `try_lock()`, recovering the guard when the lock is poisoned; `None` when it is held by someone else.
    fn try_lock_ignore_poison(&self) -> Option<MutexGuard<'_, T>>;
}

impl<T: ?Sized> MutexExt<T> for Mutex<T> {
    #[inline]
    fn lock_ignore_poison(&self) -> MutexGuard<'_, T> {
        lock_ignore_poison(self)
    }

    #[inline]
    fn try_lock_ignore_poison(&self) -> Option<MutexGuard<'_, T>> {
        match self.try_lock() {
            Ok(guard) => Some(guard),
            Err(TryLockError::Poisoned(poisoned)) => Some(poisoned.into_inner()),
            Err(TryLockError::WouldBlock) => None,
        }
    }
}

/// Poison-tolerant `RwLock` accessors.
pub trait RwLockExt<T: ?Sized> {
    /// `read()`, recovering the guard when the lock is poisoned.
    fn read_ignore_poison(&self) -> RwLockReadGuard<'_, T>;
    /// `write()`, recovering the guard when the lock is poisoned.
    fn write_ignore_poison(&self) -> RwLockWriteGuard<'_, T>;
    /// `try_read()`, recovering the guard when the lock is poisoned; `None` when a writer holds the lock.
    fn try_read_ignore_poison(&self) -> Option<RwLockReadGuard<'_, T>>;
    /// `try_write()`, recovering the guard when the lock is poisoned; `None` when the lock is held.
    fn try_write_ignore_poison(&self) -> Option<RwLockWriteGuard<'_, T>>;
}

impl<T: ?Sized> RwLockExt<T> for RwLock<T> {
    #[inline]
    fn read_ignore_poison(&self) -> RwLockReadGuard<'_, T> {
        read_ignore_poison(self)
    }

    #[inline]
    fn write_ignore_poison(&self) -> RwLockWriteGuard<'_, T> {
        write_ignore_poison(self)
    }

    #[inline]
    fn try_read_ignore_poison(&self) -> Option<RwLockReadGuard<'_, T>> {
        match self.try_read() {
            Ok(guard) => Some(guard),
            Err(TryLockError::Poisoned(poisoned)) => Some(poisoned.into_inner()),
            Err(TryLockError::WouldBlock) => None,
        }
    }

    #[inline]
    fn try_write_ignore_poison(&self) -> Option<RwLockWriteGuard<'_, T>> {
        match self.try_write() {
            Ok(guard) => Some(guard),
            Err(TryLockError::Poisoned(poisoned)) => Some(poisoned.into_inner()),
            Err(TryLockError::WouldBlock) => None,
        }
    }
}

/// Poison-tolerant `Condvar` waits: the mutex a waiter reacquires may have been poisoned by another thread's unwind
/// while the waiter slept.
pub trait CondvarExt {
    /// `wait(guard)`, recovering the guard when the mutex is poisoned. Spurious wake-ups are possible, as with
    /// `Condvar::wait`.
    fn wait_ignore_poison<'a, T>(&self, guard: MutexGuard<'a, T>) -> MutexGuard<'a, T>;
    /// `wait_while(guard, condition)`, recovering the guard when the mutex is poisoned.
    fn wait_while_ignore_poison<'a, T, F: FnMut(&mut T) -> bool>(
        &self,
        guard: MutexGuard<'a, T>,
        condition: F,
    ) -> MutexGuard<'a, T>;
    /// `wait_timeout(guard, timeout)`, recovering the guard when the mutex is poisoned.
    fn wait_timeout_ignore_poison<'a, T>(
        &self,
        guard: MutexGuard<'a, T>,
        timeout: Duration,
    ) -> (MutexGuard<'a, T>, WaitTimeoutResult);
    /// `wait_timeout_while(guard, timeout, condition)`, recovering the guard when the mutex is poisoned.
    fn wait_timeout_while_ignore_poison<'a, T, F: FnMut(&mut T) -> bool>(
        &self,
        guard: MutexGuard<'a, T>,
        timeout: Duration,
        condition: F,
    ) -> (MutexGuard<'a, T>, WaitTimeoutResult);
}

impl CondvarExt for Condvar {
    #[inline]
    fn wait_ignore_poison<'a, T>(&self, guard: MutexGuard<'a, T>) -> MutexGuard<'a, T> {
        self.wait(guard).unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[inline]
    fn wait_while_ignore_poison<'a, T, F: FnMut(&mut T) -> bool>(
        &self,
        guard: MutexGuard<'a, T>,
        condition: F,
    ) -> MutexGuard<'a, T> {
        self.wait_while(guard, condition).unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[inline]
    fn wait_timeout_ignore_poison<'a, T>(
        &self,
        guard: MutexGuard<'a, T>,
        timeout: Duration,
    ) -> (MutexGuard<'a, T>, WaitTimeoutResult) {
        self.wait_timeout(guard, timeout).unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[inline]
    fn wait_timeout_while_ignore_poison<'a, T, F: FnMut(&mut T) -> bool>(
        &self,
        guard: MutexGuard<'a, T>,
        timeout: Duration,
        condition: F,
    ) -> (MutexGuard<'a, T>, WaitTimeoutResult) {
        self.wait_timeout_while(guard, timeout, condition).unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Widens the calling thread's affinity mask to the process's (the main thread's, `sched_getaffinity(getpid())`): a
/// thread inherits the mask of the thread that spawned it, and a worker the scheduler pinned to one CPU
/// (`pin_threads`) would otherwise hand every helper it spawns that one CPU - `CALL dsdgen(sf=1)` at 96 pinned
/// threads ran 8.3 s instead of 0.9 s whenever the function's task landed on a pinned worker. Linux only (the pinning
/// is); the mask stays as inherited when the call fails.
fn adopt_process_affinity() {
    #[cfg(target_os = "linux")]
    {
        use std::ffi::c_int;
        /// `cpu_set_t`: `CPU_SETSIZE` (1024) bits.
        #[repr(C)]
        struct CpuSet([u64; 16]);
        unsafe extern "C" {
            fn getpid() -> c_int;
            fn sched_getaffinity(pid: c_int, cpusetsize: usize, mask: *mut CpuSet) -> c_int;
            fn sched_setaffinity(pid: c_int, cpusetsize: usize, mask: *const CpuSet) -> c_int;
        }
        let mut set = CpuSet([0; 16]);
        // SAFETY(ffi): `set` is a `cpu_set_t` of our layout that lives for both calls and `cpusetsize` is its size;
        // `getpid()` names the main thread (the process's mask), pid 0 the calling thread.
        unsafe {
            if sched_getaffinity(getpid(), size_of::<CpuSet>(), &mut set) == 0 {
                sched_setaffinity(0, size_of::<CpuSet>(), &set);
            }
        }
    }
}

/// `std::thread::Builder::spawn_scoped` for a group of workers, with the fallback of `TaskSchedulerPool::
/// RelaunchThreads`: when the system refuses a thread (`EAGAIN` / `ENOMEM` at the thread or commit limit of the process
/// or the box), no further worker is launched and the items of the rest come back to the caller - which runs them on
/// its own thread when none could be started - instead of the panic of `Scope::spawn`, which ends the thread it runs
/// on and leaves the started workers waiting for work that never comes. `work` runs once per started worker with its
/// item; the returned items are the unstarted ones, in order. `stack_size` is the workers' stack (the default when
/// `None`).
pub fn spawn_workers<'scope, 'env, T, F>(
    scope: &'scope Scope<'scope, 'env>,
    name: &str,
    stack_size: Option<usize>,
    items: Vec<T>,
    work: &'scope F,
) -> Vec<T>
where
    T: Send + 'scope,
    F: Fn(T) + Sync + 'scope,
{
    // every worker takes its item out of a shared slot, so the item of a worker the system refuses stays available
    let slots: Vec<Arc<Mutex<Option<T>>>> = items.into_iter().map(|item| Arc::new(Mutex::new(Some(item)))).collect();
    let mut unstarted = Vec::new();
    for (index, slot) in slots.iter().enumerate() {
        let thread_slot = Arc::clone(slot);
        let mut builder = std::thread::Builder::new().name(name.to_string());
        if let Some(size) = stack_size {
            builder = builder.stack_size(size);
        }
        let spawned = builder.spawn_scoped(scope, move || {
            adopt_process_affinity();
            if let Some(item) = lock_ignore_poison(&thread_slot).take() {
                work(item);
            }
        });
        if spawned.is_err() {
            // stop launching: the system has no thread for this worker, so it has none for the next either
            unstarted.extend(slots[index..].iter().filter_map(|slot| lock_ignore_poison(slot).take()));
            break;
        }
    }
    unstarted
}

// kept: the extension crates' worker-thread helper under refused thread creation (concurrency)
#[cfg(test)]
mod spawn_tests {
    use super::*;

    #[test]
    fn spawn_workers_runs_every_item() {
        let seen = Mutex::new(Vec::new());
        let work = |item: usize| lock_ignore_poison(&seen).push(item);
        let unstarted = std::thread::scope(|scope| spawn_workers(scope, "test workers", None, vec![0, 1, 2, 3], &work));
        assert!(unstarted.is_empty());
        let mut seen = lock_ignore_poison(&seen).clone();
        seen.sort_unstable();
        assert_eq!(seen, vec![0, 1, 2, 3]);
    }

    #[test]
    fn spawn_workers_returns_the_items_of_the_threads_the_system_refuses() {
        // a stack the system cannot map: no worker starts, every item comes back in order and nothing ran
        let seen = Mutex::new(Vec::new());
        let work = |item: usize| lock_ignore_poison(&seen).push(item);
        let unstarted = std::thread::scope(|scope| {
            spawn_workers(scope, "test workers", Some(usize::MAX / 2), vec![0, 1, 2], &work)
        });
        assert_eq!(unstarted, vec![0, 1, 2]);
        assert!(lock_ignore_poison(&seen).is_empty());
        // the caller runs them itself
        for item in unstarted {
            work(item);
        }
        assert_eq!(*lock_ignore_poison(&seen), vec![0, 1, 2]);
    }
}
