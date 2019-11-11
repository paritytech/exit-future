extern crate futures;
extern crate parking_lot;

use std::sync::Arc;
use std::pin::Pin;
use std::task::{Poll, Context};
use std::ops::DerefMut;
use futures::{Future, FutureExt, channel::oneshot, future::{select, Either}, executor::block_on};
use parking_lot::Mutex;

#[derive(Clone)]
pub struct Exit(Arc<Mutex<oneshot::Receiver<()>>>);

impl Future for Exit {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let mut receiver = Pin::into_inner(self).0.lock();
        Pin::new(receiver.deref_mut()).poll(cx).map(|_| ())
    }
}

impl Exit {
    pub fn is_live(&self) -> bool {
        self.0.lock().try_recv().is_ok()
    }

    pub fn until<F: Future + Unpin>(self, future: F) -> impl Future<Output = Option<F::Output>> {
        select(self, future)
            .map(|either| match either {
                Either::Left(_) => None,
                Either::Right((output, _)) => Some(output)
            })
    }

    pub fn wait(self) {
        block_on(self)
    }
}

pub struct Signal(oneshot::Sender<()>);

impl Signal {
    pub fn fire(self) -> Result<(), ()> {
        self.0.send(())
    }
}

pub fn signal() -> (Signal, Exit) {
    let (sender, receiver) = oneshot::channel();
    (Signal(sender), Exit(Arc::new(Mutex::new(receiver))))
}

#[cfg(test)]
mod tests {
    use futures::future::{join3, ready, pending, lazy};
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
            let barrier = ::futures::future::lazy(move |_| {
                thread_barrier.wait();
            });

            block_on(join3(exit_a, exit_b, barrier));
        });

        barrier.wait();
        signal.fire().unwrap();

        let _ = handle.join();
        assert!(!exit_c.is_live());
        exit_c.wait()
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
        let work_a = exit.clone().until(ready(5));
        assert_eq!(block_on(work_a), Some(5));

        signal.fire().unwrap();
        let work_b = exit.until(pending::<()>());
        assert_eq!(block_on(work_b), None);
    }

    #[test]
    fn works_from_other_thread() {
        let (signal, exit) = signal();

        ::std::thread::spawn(move || {
            ::std::thread::sleep(::std::time::Duration::from_millis(2500));
            signal.fire().unwrap();
        });

        block_on(exit);
    }

    #[test]
    fn clone_works() {
        let (_signal, mut exit) = signal();

        let future = lazy(move |cx| {
            let _ = Pin::new(&mut exit).poll(cx);

            let mut exit2 = exit.clone();
            let _ = Pin::new(&mut exit2).poll(cx);
        });

        block_on(future)
    }
}
