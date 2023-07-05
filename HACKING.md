# Logging
This crate makes use of [`tracing-subscriber`] to perform structured logging.
All log messages should be lowercase to make grepping easier - I've determined
scientifically that this is less work than always adding `-i`. Many log messages
also have an action field. For messages that inform the reader of something being
done use this field. For example, prefer:
```
info!(action = "shaving the yaks")
```
to
```
info!("shaving the yaks")
```
Whereas informing the reader of something happening has no frills:
```
warn!("one yak was too hairy")
```

# Channels
The channel used in this crate is [`async_std::channel`], an `mpmc` bounded
or unbounded channel. There are several factors influencing this decicion:
1. We need a unique receiver for each item in the channel. That is, if one thread
receives an item, no other thread claims it. The [`tokio::sync::broadcast::channel`]
does not fulfill these semantics as multiple threads can receive the same item.
2. For borrow checker/thread spawning reasons senders/receivers must not need
mutable references to `self` unless we want a `Mutex` involved.
3. The channel's methods must be cancellation safe.

Another common pattern used in this crate is sending a [`tokio::sync::oneshot::Sender`]
with some data over the channel and then `.await`ing the receiver. For reference, a
`oneshot` channel can send one message. Thus, the return message is a confirmation
that the receiver received and processed the data.


[`tracing-subscriber`]: https://docs.rs/tracing-subscriber/latest/tracing_subscriber/
[`async_std::channel`]: https://docs.rs/async-std/latest/async_std/channel/index.html
[`tokio::sync::broadcast::channel`]: https://docs.rs/tokio/latest/tokio/sync/broadcast/index.html
[`tokio::oneshot`]: https://docs.rs/tokio/latest/tokio/sync/oneshot/index.html
