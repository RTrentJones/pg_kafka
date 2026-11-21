//! Shared helpers for the fuzz targets.

use std::sync::Once;

/// Install (once) a panic hook that treats a panic as a crash only when it is raised in
/// pg_kafka's own code (`src/kafka/...` of this repository). The kafka-protocol body
/// decoders index without bounds checks and panic on truncated input; the listener
/// contains those with `catch_unwind`, so for a target that goes through
/// `parse_request` they are expected, not findings. libFuzzer's own hook (abort) still
/// runs for every other panic, and an unwinding panic that escapes the target aborts
/// too.
pub fn crash_only_on_own_panics() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let fuzzer_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let ours = info.location().is_none_or(|l| {
                let f = l.file();
                (f.contains("src/kafka/") || f.contains("fuzz_targets/")) && !f.contains(".cargo/")
            });
            if ours {
                fuzzer_hook(info);
            }
        }));
    });
}
