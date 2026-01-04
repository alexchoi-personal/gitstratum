#[cfg(all(target_os = "linux", feature = "io_uring"))]
pub(crate) mod uring;

#[cfg(not(all(target_os = "linux", feature = "io_uring")))]
pub(crate) mod fallback;

#[cfg(all(target_os = "linux", feature = "io_uring"))]
pub(crate) use uring::UringHandle;

#[cfg(not(all(target_os = "linux", feature = "io_uring")))]
pub(crate) use fallback::UringHandle;

pub(crate) mod async_queue;
pub(crate) use async_queue::{AsyncMultiQueueIo, IoQueueConfig};
