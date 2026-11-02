use scylla_proxy::{
    Condition, Reaction as _, RequestFrame, RequestOpcode, RequestReaction, RequestRule,
    ResponseFrame,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// Request rules that let a dry-mode proxy node complete the CQL handshake:
/// OPTIONS is answered with an empty SUPPORTED, and STARTUP and REGISTER with READY.
pub(crate) fn dry_mode_handshake_rules() -> Vec<RequestRule> {
    vec![
        RequestRule(
            Condition::RequestOpcode(RequestOpcode::Options),
            RequestReaction::forge_response(Arc::new(|frame: RequestFrame| {
                ResponseFrame::forged_supported(frame.params, &HashMap::new()).unwrap()
            })),
        ),
        RequestRule(
            Condition::or(
                Condition::RequestOpcode(RequestOpcode::Startup),
                Condition::RequestOpcode(RequestOpcode::Register),
            ),
            RequestReaction::forge_response(Arc::new(|frame: RequestFrame| {
                ResponseFrame::forged_ready(frame.params)
            })),
        ),
    ]
}

pub(crate) fn setup_tracing() {
    let testing_layer = tracing_subscriber::fmt::layer()
        .with_test_writer()
        .with_filter(tracing_subscriber::EnvFilter::from_default_env());
    let noop_layer = tracing_subscriber::fmt::layer().with_writer(std::io::sink);
    let _ = tracing_subscriber::registry()
        .with(testing_layer)
        .with(noop_layer)
        .try_init();
}

/// A `tracing` writer that accumulates the emitted log text in memory, so that a test can
/// assert on what was (and was not) logged.
#[derive(Clone, Default)]
pub(crate) struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

impl CapturedLogs {
    pub(crate) fn text(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

impl std::io::Write for CapturedLogs {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLogs {
    type Writer = CapturedLogs;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Runs `f` with a scoped subscriber that appends everything logged at ERROR level to
/// `capture`. Only works for synchronous `f`, which is what makes the thread-local dispatcher
/// - and thus the capture - apply.
pub(crate) fn capturing_errors<T>(capture: &CapturedLogs, f: impl FnOnce() -> T) -> T {
    let subscriber = tracing_subscriber::fmt()
        .with_writer(capture.clone())
        .with_max_level(tracing::Level::ERROR)
        .without_time()
        .finish();
    tracing::subscriber::with_default(subscriber, f)
}
