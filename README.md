## [Documentation](https://docs.rs/exit-future)
----

Create a `Signal` and cloneable `Exit` future that fires when `Signal` is fired. Used to coordinate exit between multiple event-loop threads.

```rust
use futures::executor::block_on;
let (signal, exit) = exit_future::signal();

::std::thread::spawn(move || {
    // future resolves when signal fires
    block_on(exit);
});

let _ = signal.fire();
```
