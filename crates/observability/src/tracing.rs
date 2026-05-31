use tracing_subscriber::fmt;
use tracing_subscriber::{prelude::*, EnvFilter, Registry};

pub fn init_tracing() {
    let fmt_layer = fmt::layer()
        .with_target(true)
        .with_thread_ids(true)
        .with_line_number(true)
        .with_file(true);

    let filter_layer = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap();

    let registry = Registry::default().with(filter_layer).with(fmt_layer);

    tracing::subscriber::set_global_default(registry).expect("Failed to set tracing subscriber");
}
