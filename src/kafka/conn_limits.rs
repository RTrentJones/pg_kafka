//! Connection limits of the network edge: per-listener and
//! per-IP connection caps, and a per-IP rate limit on pre-authentication credential
//! lookups. Pure logic (no I/O, no pgrx), shared by the accept loops on the network
//! thread; the settings come from GUCs read when the worker starts
//! (`auth::settings::raw_from_gucs`).

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Default of `pg_kafka.max_connections` (per listener).
pub const DEFAULT_MAX_CONNECTIONS: i32 = 1024;
/// Default of `pg_kafka.max_connections_per_ip`.
pub const DEFAULT_MAX_CONNECTIONS_PER_IP: i32 = 256;
/// Default of `pg_kafka.connections_max_idle_ms` (Kafka's `connections.max.idle.ms`).
pub const DEFAULT_CONNECTIONS_MAX_IDLE_MS: i32 = 600_000;
/// Default of `pg_kafka.request_read_timeout_ms`.
pub const DEFAULT_REQUEST_READ_TIMEOUT_MS: i32 = 60_000;
/// Upper bound of the connection-count GUCs.
pub const MAX_CONNECTIONS_LIMIT: i32 = 100_000;

/// Credential lookups (SASL client-first / PLAIN tokens) one IP address may start per
/// second, sustained.
pub const SASL_LOOKUPS_PER_IP_PER_SEC: f64 = 10.0;
/// Burst of credential lookups one IP address may start at once.
pub const SASL_LOOKUP_BURST_COUNT: u16 = 20;
/// [`SASL_LOOKUP_BURST_COUNT`] as a token count (`f64::from` is not usable in a const).
pub const SASL_LOOKUP_BURST: f64 = SASL_LOOKUP_BURST_COUNT as f64;
/// Shortest idle timeout or request read deadline, in milliseconds.
const MIN_TIMEOUT_MS: i32 = 100;
/// Entries kept in the per-IP table before idle ones are swept.
const SWEEP_THRESHOLD: usize = 4096;

/// The network limits, validated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NetworkLimits {
    /// Concurrent connections per listener (each listener has its own cap, so a flood on
    /// one listener cannot lock clients out of another, e.g. a loopback admin listener).
    pub max_connections: usize,
    /// Concurrent connections from one IP address, across all listeners.
    pub max_connections_per_ip: usize,
    /// Close a connection that has neither sent a request nor had a response written
    /// for this long, with no request outstanding.
    pub connections_max_idle: Duration,
    /// A request frame must arrive completely within this long of its first byte.
    pub request_read_timeout: Duration,
}

impl Default for NetworkLimits {
    fn default() -> Self {
        NetworkLimits::from_raw(
            DEFAULT_MAX_CONNECTIONS,
            DEFAULT_MAX_CONNECTIONS_PER_IP,
            DEFAULT_CONNECTIONS_MAX_IDLE_MS,
            DEFAULT_REQUEST_READ_TIMEOUT_MS,
        )
    }
}

impl NetworkLimits {
    /// From GUC values (already range-checked by Postgres; clamped again here).
    pub fn from_raw(
        max_connections: i32,
        max_connections_per_ip: i32,
        connections_max_idle_ms: i32,
        request_read_timeout_ms: i32,
    ) -> Self {
        // Both clamps keep the value positive, so the conversions cannot fail.
        let ms = |v: i32| {
            Duration::from_millis(u64::try_from(v.max(MIN_TIMEOUT_MS)).unwrap_or(u64::MAX))
        };
        let count = |v: i32| usize::try_from(v.clamp(1, MAX_CONNECTIONS_LIMIT)).unwrap_or(1);
        NetworkLimits {
            max_connections: count(max_connections),
            max_connections_per_ip: count(max_connections_per_ip),
            connections_max_idle: ms(connections_max_idle_ms),
            request_read_timeout: ms(request_read_timeout_ms),
        }
    }
}

#[derive(Debug)]
struct IpEntry {
    connections: usize,
    /// Token bucket of credential lookups; may go negative (lookups waiting).
    tokens: f64,
    refilled_at: Instant,
}

impl IpEntry {
    fn new(now: Instant) -> Self {
        IpEntry {
            connections: 0,
            tokens: SASL_LOOKUP_BURST,
            refilled_at: now,
        }
    }

    fn refill(&mut self, now: Instant) {
        let elapsed = now
            .saturating_duration_since(self.refilled_at)
            .as_secs_f64();
        self.tokens = (self.tokens + elapsed * SASL_LOOKUPS_PER_IP_PER_SEC).min(SASL_LOOKUP_BURST);
        self.refilled_at = now;
    }

    /// Nothing to remember: no connection, and the bucket is full again.
    fn idle(&mut self, now: Instant) -> bool {
        self.refill(now);
        self.connections == 0 && self.tokens >= SASL_LOOKUP_BURST
    }
}

/// Per-IP state of all listeners: live connection counts and the lookup token buckets.
#[derive(Debug)]
pub struct IpTracker {
    max_per_ip: usize,
    entries: Mutex<HashMap<IpAddr, IpEntry>>,
}

/// One admitted connection of an IP; dropping it frees the slot.
#[derive(Debug)]
pub struct IpPermit {
    tracker: Arc<IpTracker>,
    ip: IpAddr,
}

impl Drop for IpPermit {
    fn drop(&mut self) {
        let now = Instant::now();
        let mut entries = self.tracker.lock();
        if let Some(e) = entries.get_mut(&self.ip) {
            e.connections = e.connections.saturating_sub(1);
            if e.idle(now) {
                entries.remove(&self.ip);
            }
        }
    }
}

/// What to do before a credential lookup.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LookupAdmission {
    /// Look up now.
    Now,
    /// Wait this long first (the IP exceeded its sustained rate).
    After(Duration),
    /// The IP is far over its rate: do not look up; the connection waits out its
    /// pre-authentication deadline and is closed without an answer.
    Refused,
}

impl IpTracker {
    pub fn new(max_per_ip: usize) -> Arc<Self> {
        Arc::new(IpTracker {
            max_per_ip: max_per_ip.max(1),
            entries: Mutex::new(HashMap::new()),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<IpAddr, IpEntry>> {
        // The critical sections cannot panic; recover from poisoning anyway.
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Admit a new connection from `ip`, or `None` when the IP is at its cap.
    pub fn try_open(self: &Arc<Self>, ip: IpAddr) -> Option<IpPermit> {
        let now = Instant::now();
        let mut entries = self.lock();
        if entries.len() >= SWEEP_THRESHOLD && !entries.contains_key(&ip) {
            entries.retain(|_, e| !e.idle(now));
        }
        let entry = entries.entry(ip).or_insert_with(|| IpEntry::new(now));
        if entry.connections >= self.max_per_ip {
            return None;
        }
        entry.connections += 1;
        Some(IpPermit {
            tracker: Arc::clone(self),
            ip,
        })
    }

    /// Live connections of `ip`.
    #[cfg(test)]
    fn connections(&self, ip: IpAddr) -> usize {
        self.lock().get(&ip).map_or(0, |e| e.connections)
    }

    /// Entries in the table.
    #[cfg(test)]
    fn tracked(&self) -> usize {
        self.lock().len()
    }

    /// Take one credential-lookup token for `ip` (token bucket of
    /// [`SASL_LOOKUPS_PER_IP_PER_SEC`], burst [`SASL_LOOKUP_BURST`]).
    pub fn admit_lookup(&self, ip: IpAddr) -> LookupAdmission {
        self.admit_lookup_at(ip, Instant::now())
    }

    fn admit_lookup_at(&self, ip: IpAddr, now: Instant) -> LookupAdmission {
        let mut entries = self.lock();
        let entry = entries.entry(ip).or_insert_with(|| IpEntry::new(now));
        entry.refill(now);
        if entry.tokens <= -SASL_LOOKUP_BURST {
            return LookupAdmission::Refused;
        }
        entry.tokens -= 1.0;
        if entry.tokens >= 0.0 {
            LookupAdmission::Now
        } else {
            LookupAdmission::After(Duration::from_secs_f64(
                -entry.tokens / SASL_LOOKUPS_PER_IP_PER_SEC,
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    #[test]
    fn defaults_match_the_gucs() {
        let d = NetworkLimits::default();
        assert_eq!(d.max_connections, 1024);
        assert_eq!(d.max_connections_per_ip, 256);
        assert_eq!(d.connections_max_idle, Duration::from_secs(600));
        assert_eq!(d.request_read_timeout, Duration::from_secs(60));
        let c = NetworkLimits::from_raw(0, -5, 1, 0);
        assert_eq!(c.max_connections, 1);
        assert_eq!(c.max_connections_per_ip, 1);
        assert_eq!(c.connections_max_idle, Duration::from_millis(100));
    }

    #[test]
    fn per_ip_cap_is_per_address_and_freed_on_drop() {
        let t = IpTracker::new(2);
        let a1 = t.try_open(ip("10.0.0.1")).unwrap();
        let _a2 = t.try_open(ip("10.0.0.1")).unwrap();
        assert!(
            t.try_open(ip("10.0.0.1")).is_none(),
            "third from the same IP"
        );
        let _b = t
            .try_open(ip("10.0.0.2"))
            .expect("another IP is unaffected");
        assert_eq!(t.connections(ip("10.0.0.1")), 2);
        drop(a1);
        assert_eq!(t.connections(ip("10.0.0.1")), 1);
        assert!(t.try_open(ip("10.0.0.1")).is_some());
    }

    #[test]
    fn idle_entries_are_removed() {
        let t = IpTracker::new(4);
        let p = t.try_open(ip("::1")).unwrap();
        assert_eq!(t.tracked(), 1);
        drop(p);
        assert_eq!(t.tracked(), 0, "no connection and a full bucket: forgotten");
    }

    #[test]
    fn lookup_bucket_survives_reconnects() {
        // Closing and reopening connections must not reset the lookup bucket (otherwise
        // a client could get a fresh burst per connection).
        let t = IpTracker::new(4);
        let a = ip("192.0.2.7");
        let now = Instant::now();
        for _ in 0..SASL_LOOKUP_BURST_COUNT {
            let p = t.try_open(a).unwrap();
            assert_eq!(t.admit_lookup_at(a, now), LookupAdmission::Now);
            drop(p);
        }
        assert_eq!(t.tracked(), 1, "a drained bucket is remembered");
        match t.admit_lookup_at(a, now) {
            LookupAdmission::After(d) => {
                assert!(
                    d > Duration::ZERO && d <= Duration::from_millis(101),
                    "{d:?}"
                );
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn lookup_rate_waits_then_refuses_then_recovers() {
        let t = IpTracker::new(4);
        let a = ip("198.51.100.1");
        let now = Instant::now();
        let mut waits = Vec::new();
        for _ in 0..(2 * SASL_LOOKUP_BURST_COUNT) {
            match t.admit_lookup_at(a, now) {
                LookupAdmission::Now => {}
                LookupAdmission::After(d) => waits.push(d),
                LookupAdmission::Refused => panic!("refused too early"),
            }
        }
        assert_eq!(waits.len(), usize::from(SASL_LOOKUP_BURST_COUNT));
        assert!(
            waits.windows(2).all(|w| w[0] < w[1]),
            "queued lookups wait longer"
        );
        assert_eq!(t.admit_lookup_at(a, now), LookupAdmission::Refused);
        // Another address is unaffected.
        assert_eq!(
            t.admit_lookup_at(ip("198.51.100.2"), now),
            LookupAdmission::Now
        );
        // After enough time the bucket refills.
        let later = now + Duration::from_secs(10);
        assert_eq!(t.admit_lookup_at(a, later), LookupAdmission::Now);
    }
}
