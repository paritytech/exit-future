extern crate futures;
extern crate parking_lot;

use parking_lot::Mutex;
use futures::prelude::*;
use futures::task::{self, Task};

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

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

/// Future that resolves when the exit signal has fired.
pub struct Exit {
    id: usize,
    inner: Arc<Inner>,
}

impl Exit {
    /// Check if the signal is live outside of the context of a task and 
    /// without scheduling a wakeup.
    pub fn is_live(&self) -> bool {
        self.inner.waiting.lock().0
    }

    /// Perform given work until complete.
    pub fn until<F: IntoFuture>(self, f: F) -> UntilExit<F::Future> {
        UntilExit {
            inner: f.into_future(),
            exit: self,
        }
    } 

    fn check(&self) -> Async<()> {
        if self.inner.check(self.id) {
            Async::NotReady
        } else {
            Async::Ready(())
        }
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
        let new_id = self.inner.register();
        Exit { id: new_id, inner: self.inner.clone() }
    }
}

struct Inner {
    count: AtomicUsize,
    waiting: Mutex<(bool, HashMap<usize, Task>)>,
}

impl Inner {
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
    fn check(&self, id: usize) -> bool {
        let mut waiting = self.waiting.lock();

        if waiting.0 {
            waiting.1.entry(id).or_insert_with(task::current);
        }

        waiting.0
    }
}

/// Exit signal that fires either manually or on drop.
pub struct Signal {
    inner: Option<Arc<Inner>>,
}

impl Signal {
    fn fire_inner(&mut self) {
        if let Some(signal) = self.inner.take() {
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
    let inner = Arc::new(Inner {
        count: AtomicUsize::new(0),
        waiting: Mutex::new((true, HashMap::new())),
    });

    (
        Signal { inner: Some(inner.clone()) },
        Exit { id: 0, inner },
    )
}

#[cfg(test)]
mod tests {
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
        use futures::future;

        let (signal, exit) = signal();
        let work_a = exit.clone().until(future::ok::<_, ()>(5));
        assert_eq!(work_a.wait().unwrap(), Some(5));

        signal.fire();
        let work_b = exit.until(::futures::future::empty::<(), ()>());
        assert_eq!(work_b.wait().unwrap(), None);
    }
}
