#[cfg(all(unix, feature = "jemalloc"))]
#[global_allocator]
pub(crate) static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
