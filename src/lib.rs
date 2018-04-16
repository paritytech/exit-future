extern crate futures;
extern crate parking_lot;

use parking_lot::Mutex;
use futures::prelude::*;
use futures::task::{self, Task};

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

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
}

impl Future for Exit {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<(), ()> {
        if self.inner.check(self.id) {
            Ok(Async::NotReady)
        } else {
            Ok(Async::Ready(()))
        }
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
}
