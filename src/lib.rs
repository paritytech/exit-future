extern crate futures;
extern crate parking_lot;
extern crate pin_utils;

use std::sync::Arc;
use std::pin::Pin;
use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, Ordering};
use futures::task::{Context, Poll};
use futures::future::Future;
use pin_utils::unsafe_pinned;

/// Future that resolves when inner work finishes or on exit signal firing.
#[derive(Clone)]
pub struct UntilExit<F> {
    inner: F,
    exit: Exit,
}

impl<F> UntilExit<F> {
    unsafe_pinned!(inner: F);
}

impl<F: Future> Future for UntilExit<F> {
    type Output = Option<F::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let is_live = self.exit.is_live();
        let inner = self.inner();
        match inner.poll(cx) {
            Poll::Ready(x) => Poll::Ready(Some(x)),
            Poll::Pending => {
                if is_live {
                    Poll::Pending
                } else {
                    Poll::Ready(None)
                }
            },
        }
    }
}

/// Future that resolves when the exit signal has fired.
#[derive(Clone)]
pub struct Exit {
    shared: Arc<RefCell<Shared>>,
}

impl Exit {
    /// Check if the signal is live outside of the context of a task.
    pub fn is_live(&self) -> bool {
        self.shared.borrow_mut().is_live()
    }

    /// Perform given work until complete.
    pub fn until<F: Future>(self, f: F) -> UntilExit<F> {
        UntilExit {
            inner: f,
            exit: self,
        }
    }
}

impl Future for Exit {
    type Output = Result<(), ()>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.shared.borrow_mut().is_live() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}

unsafe impl Send for Exit {}
unsafe impl Sync for Exit {}

struct Shared {
    waiting: AtomicBool
}
impl Shared {
    fn set(&mut self) {
        self.waiting.store(false, Ordering::Relaxed);
    }

    fn is_live(&self) -> bool {
        self.waiting.load(Ordering::Relaxed)
    }
}

/// Exit signal that fires either manually or on drop.
#[derive(Clone)]
pub struct Signal {
    shared: Arc<RefCell<Shared>>,
}

impl Signal {
    fn fire_inner(&self) {
        self.shared.borrow_mut().set();
    }

    /// Fire the signal manually.
    pub fn fire(self) {
        self.fire_inner()
    }

    /// Get an exit future.
    pub fn make_exit(&self) -> Exit {
        Exit { shared: self.shared.clone() }
    }
}

impl Drop for Signal {
    fn drop(&mut self) {
        self.fire_inner()
    }
}

unsafe impl Send for Signal {}
unsafe impl Sync for Signal {}

/// Create a signal and exit pair. `Exit` is a future that resolves when the
/// `Signal` object is either dropped or has `fire` called on it.
pub fn signal() -> (Signal, Exit) {
    let signal = signal_only();
    let exit = signal.make_exit();

    (signal, exit)
}

/// Create only a signal.
pub fn signal_only() -> Signal {
    let shared = Arc::new(RefCell::new(Shared {
        waiting: AtomicBool::new(true)
    }));

    Signal { shared }
}

#[cfg(test)]
mod tests {
    use futures::future;
    use futures::future::join3;
    use futures::executor::block_on;
    use super::*;

    #[test]
    fn it_works() {
        let (signal, exit_a) = signal();
        let exit_b = exit_a.clone();
        let exit_c = exit_b.clone();

        assert!(exit_a.is_live() && exit_b.is_live());

        let barrier = Arc::new(::std::sync::Barrier::new(2));
        let thread_barrier = barrier.clone();
        let shared_a = exit_a.clone();
        let shared_b = exit_b.clone();
        let handle = ::std::thread::spawn(move || {
            let barrier = ::futures::future::lazy(move |_| -> Result<(), ()> {thread_barrier.wait(); Ok(())});
            let (a, b, c) = block_on(join3(shared_a, shared_b, barrier));
            assert_eq!(a, Ok(()));
            assert_eq!(b, Ok(()));
            assert_eq!(c, Ok(()));
        });

        assert!(exit_c.is_live());
        barrier.wait();
        signal.fire();
        let _ = handle.join();
        assert!(!exit_c.is_live());
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
        assert_eq!(block_on(work_a), Some(Ok(5)));

        signal.fire();
        let work_b = exit.until(future::pending::<()>());
        assert_eq!(block_on(work_b), None);
    }

    #[test]
    fn works_from_other_thread() {
        let (signal, exit) = signal();

        ::std::thread::spawn(move || {
            ::std::thread::sleep(::std::time::Duration::from_millis(2500));
            signal.fire();
        });

        block_on(exit).unwrap();
    }

    // #[test]
    // fn clone_works() {
    //     let (_signal, mut exit) = signal();

    //     future::lazy(move || {
    //         exit.poll().unwrap();
    //         assert!(exit.inner.is_some());

    //         let mut exit2 = exit.clone();
    //         assert!(exit2.inner.is_none());
    //         exit2.poll().unwrap();

    //         assert!(exit.inner.unwrap().shared_id != exit2.inner.unwrap().shared_id);
    //         future::ok::<(), ()>(())
    //     }).wait().unwrap();
    // }
}
