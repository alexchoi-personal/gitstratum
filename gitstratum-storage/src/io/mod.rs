#[cfg(all(target_os = "linux", feature = "io_uring"))]
pub mod uring;

#[cfg(not(all(target_os = "linux", feature = "io_uring")))]
pub mod fallback;

#[cfg(all(target_os = "linux", feature = "io_uring"))]
pub use uring::{IoCompletion, IoOperation, UringHandle, DEFAULT_RING_SIZE};

#[cfg(not(all(target_os = "linux", feature = "io_uring")))]
pub use fallback::{IoCompletion, IoOperation, UringHandle, DEFAULT_RING_SIZE};

pub mod async_queue;
pub use async_queue::{AsyncMultiQueueIo, AsyncUringQueue, IoQueueConfig, IoResult};
