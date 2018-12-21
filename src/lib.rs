extern crate futures;
extern crate parking_lot;

use parking_lot::Mutex;
use futures::prelude::*;
use futures::task::{self, Task, AtomicTask};
use futures::executor::{self, Notify};

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// Future that resolves when inner work finishes or on exit signal firing.
#[derive(Clone)]
pub struct UntilExit<F> {
    inner: F,
    exit: Exit,
}

impl<F: Future> Future for UntilExit<F> {
    type Item = Option<F::Item>;
    type Error = F::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.inner.poll() {
            Ok(Async::Ready(x)) => Ok(Async::Ready(Some(x))),
            Ok(Async::NotReady) => Ok(self.exit.check().map(|()| None)),
            Err(e) => Err(e),
        }
    }
}

struct Notifier {
    signalled: AtomicBool,
    outer_task: AtomicTask,
}

impl Notify for Notifier {
    // id isn't necessary because this is a notifier for a single
    // exit future.
    fn notify(&self, _: usize) {
        self.signalled.store(true, Ordering::Release);
        self.outer_task.notify()
    }
}

struct ExitInner {
    shared_id: usize,
    notifier: Arc<Notifier>,
}

impl ExitInner {
    fn check(&self, shared: &Shared) -> Async<()> {
        // set up the outer task correctly so when the inner
        // task is notified, we get woken up.
        self.notifier.outer_task.register();

        // if any poll was signalled by the inner task, use it.
        if !self.notifier.signalled.fetch_and(false, Ordering::SeqCst) {
            return Async::NotReady
        }

        // do heavy check with inner task. ID doesn't matter.
        executor::with_notify(&self.notifier, 0, || {
            if shared.is_live_and_notify(self.shared_id) {
                Async::NotReady
            } else {
                Async::Ready(())
            }
        })
    }
}

/// Future that resolves when the exit signal has fired.
pub struct Exit {
    inner: Option<ExitInner>,
    shared: Arc<Shared>,
}

impl Exit {
    /// Check if the signal is live outside of the context of a task and 
    /// without scheduling a wakeup.
    pub fn is_live(&self) -> bool {
        self.shared.waiting.lock().0
    }

    /// Perform given work until complete.
    pub fn until<F: IntoFuture>(self, f: F) -> UntilExit<F::Future> {
        UntilExit {
            inner: f.into_future(),
            exit: self,
        }
    } 

    fn check(&mut self) -> Async<()> {
        let shared = &self.shared;

        // lazily register and initialize.
        let inner = self.inner.get_or_insert_with(|| {
            let shared_id = shared.register();
            let notifier = Arc::new(Notifier {
                // ensure an initial poll happens.
                signalled: AtomicBool::new(true),
                outer_task: AtomicTask::new(),
            });

            ExitInner { shared_id, notifier }
        });

        inner.check(shared)
    }
}

impl Future for Exit {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<(), ()> {
        Ok(self.check())
    }
}

impl Clone for Exit {
    fn clone(&self) -> Exit {
        Exit { inner: None, shared: self.shared.clone() }
    }
}

struct Shared {
    count: AtomicUsize,
    waiting: Mutex<(bool, HashMap<usize, Task>)>,
}

impl Shared {
    fn set(&self) {
        let wake_up = {
            let mut waiting = self.waiting.lock();
            waiting.0 = false;
            ::std::mem::replace(&mut waiting.1, HashMap::new())
        };

        for (_, task) in wake_up {
            task.notify()
        }
    }

    fn register(&self) -> usize {
        self.count.fetch_add(1, Ordering::SeqCst)
    }

    // should be called only in the context of a task.
    // returns whether the exit counter is live.
    fn is_live_and_notify(&self, id: usize) -> bool {
        let mut waiting = self.waiting.lock();

        if waiting.0 {
            let _ = waiting.1.insert(id, task::current());
        }

        waiting.0
    }
}

/// Exit signal that fires either manually or on drop.
pub struct Signal {
    shared: Option<Arc<Shared>>,
}

impl Signal {
    fn fire_inner(&mut self) {
        if let Some(signal) = self.shared.take() {
            signal.set()
        }
    }

    /// Fire the signal manually.
    pub fn fire(mut self) {
        self.fire_inner()
    }
}

impl Drop for Signal {
    fn drop(&mut self) {
        self.fire_inner()
    }
}

/// Create a signal and exit pair. `Exit` is a future that resolves when the
/// `Signal` object is either dropped or has `fire` called on it.
pub fn signal() -> (Signal, Exit) {
    let shared = Arc::new(Shared {
        count: AtomicUsize::new(1),
        waiting: Mutex::new((true, HashMap::new())),
    });

    (
        Signal { shared: Some(shared.clone()) },
        Exit { inner: None, shared },
    )
}

#[cfg(test)]
mod tests {
    use futures::future;
    use super::*;

    #[test]
    fn it_works() {
        let (signal, exit_a) = signal();
        let exit_b = exit_a.clone();
        let exit_c = exit_b.clone();

        assert!(exit_a.is_live() && exit_b.is_live());

        let barrier = Arc::new(::std::sync::Barrier::new(2));
        let thread_barrier = barrier.clone();
        let handle = ::std::thread::spawn(move || {
            let barrier = ::futures::future::lazy(move || { thread_barrier.wait(); Ok(()) });

            assert!(exit_a.join3(exit_b, barrier).wait().is_ok());
        });

        barrier.wait();
        signal.fire();

        let _ = handle.join();
        assert!(!exit_c.is_live());
        assert!(exit_c.wait().is_ok());
    }

    #[test]
    fn exit_signal_are_send_and_sync() {
        fn is_send_and_sync<T: Send + Sync>() {}

        is_send_and_sync::<Exit>();
        is_send_and_sync::<Signal>();
    }

    #[test]
    fn work_until() {
        let (signal, exit) = signal();
        let work_a = exit.clone().until(future::ok::<_, ()>(5));
        assert_eq!(work_a.wait().unwrap(), Some(5));

        signal.fire();
        let work_b = exit.until(::futures::future::empty::<(), ()>());
        assert_eq!(work_b.wait().unwrap(), None);
    }

    #[test]
    fn works_from_other_thread() {
        let (signal, exit) = signal();

        ::std::thread::spawn(move || {
            ::std::thread::sleep(::std::time::Duration::from_millis(2500));
            signal.fire();
        });

        exit.wait().unwrap();
    }

    #[test]
    fn clone_works() {
        let (_signal, mut exit) = signal();

        future::lazy(move || {
            exit.poll().unwrap();
            assert!(exit.inner.is_some());
            
            let mut exit2 = exit.clone();
            assert!(exit2.inner.is_none());
            exit2.poll().unwrap();

            assert!(exit.inner.unwrap().shared_id != exit2.inner.unwrap().shared_id);
            future::ok::<(), ()>(())
        }).wait().unwrap();
    }
}
