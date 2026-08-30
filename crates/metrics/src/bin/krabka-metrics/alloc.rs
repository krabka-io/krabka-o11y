#[cfg(all(unix, feature = "heap-profiling"))]
#[global_allocator]
pub(crate) static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
