# Listener Security: TLS and SASL

pg_kafka's Kafka listener can encrypt connections with TLS and authenticate clients
with SASL (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512) or TLS client certificates. All of it
is off by default: out of the box there is one PLAINTEXT listener on
`127.0.0.1:9092`, as before.

**Authorization is not implemented.** An authenticated client may do everything an
unauthenticated client on a PLAINTEXT listener can: read and write every topic, manage
groups, create and delete topics. There are no ACLs. The principal of every connection
is recorded (see [Principals](#principals)) so ACLs can be added later.

## Listeners

pg_kafka uses Kafka's listener security protocols:

| Protocol | Encryption | Client authentication |
|----------|-----------|-----------------------|
| `PLAINTEXT` | none | none |
| `SSL` | TLS | none, or a client certificate when `pg_kafka.ssl_ca_file` is set (mutual TLS) |
| `SASL_PLAINTEXT` | none | SASL |
| `SASL_SSL` | TLS | SASL (plus a client certificate if `pg_kafka.ssl_ca_file` is set) |

The primary listener is `pg_kafka.host`:`pg_kafka.port` and speaks
`pg_kafka.security_protocol`. More listeners can run at the same time, in Kafka's
`listeners` syntax:

```sql
ALTER SYSTEM SET pg_kafka.additional_listeners =
  'SASL_SSL://0.0.0.0:9093@broker.example.com:9093, SSL://:9094';  -- an empty host means pg_kafka.host
```

Each listener advertises its own address in Metadata and FindCoordinator responses, so a
client that bootstrapped on a listener keeps using it. This is Kafka's per-listener
`advertised.listeners`, written next to the listener it belongs to:

* `PROTOCOL://host:port@advertised_host:advertised_port` advertises the part after `@`
  (`advertised_port` defaults to the bind port; an IPv6 literal goes in brackets,
  `@[2001:db8::1]:9093`). For the primary listener the same `host[:port]` goes in
  `pg_kafka.advertised_address`.
* Without one, a listener advertises its bind host, except that an unspecified bind
  address (`0.0.0.0`, `::`) is not something a client can connect to and advertises
  `localhost`, which serves local clients only. The worker logs a WARNING at start for
  every listener that is reachable from other hosts but advertises a loopback address:
  remote clients would bootstrap and then fail to connect.

An advertised address must name a host (not `0.0.0.0` or `::`) and a port from 1 to
65535 (it may differ from the bind port, behind NAT or a load balancer); a bad one stops
the worker with an error naming the setting. A typical deployment keeps the primary
listener PLAINTEXT on loopback for local tools and exposes only a `SASL_SSL` listener
with its public name after `@`.

Why Kafka's vocabulary rather than one "TLS on/off" switch: Kafka clients are configured
with `security.protocol=SSL|SASL_SSL|...`, and running several listeners at once lets an
operator migrate clients one at a time (and lets the E2E suite keep using PLAINTEXT
9092 unchanged).

### Settings

| GUC | Default | Meaning |
|-----|---------|---------|
| `pg_kafka.security_protocol` | `PLAINTEXT` | Protocol of the primary listener |
| `pg_kafka.additional_listeners` | empty | Extra `PROTOCOL://host:port[@advertised_host[:port]]` listeners, comma separated |
| `pg_kafka.advertised_address` | empty | `host[:port]` the primary listener advertises (empty: `pg_kafka.host`, `localhost` for `0.0.0.0` / `::`) |
| `pg_kafka.ssl_cert_file` | empty | PEM certificate chain (required by any SSL / SASL_SSL listener) |
| `pg_kafka.ssl_key_file` | empty | PEM private key (PKCS#8, PKCS#1 or SEC1) |
| `pg_kafka.ssl_ca_file` | empty | PEM CA bundle; when set, TLS clients must present a certificate signed by it |
| `pg_kafka.sasl_enabled_mechanisms` | `PLAIN,SCRAM-SHA-256,SCRAM-SHA-512` | Mechanisms offered on SASL listeners |
| `pg_kafka.max_connections` | `1024` | Concurrent connections **per listener** (each listener has its own cap) |
| `pg_kafka.max_connections_per_ip` | `256` | Concurrent connections from one IP address, across all listeners (Kafka's `max.connections.per.ip`) |
| `pg_kafka.connections_max_idle_ms` | `600000` | Close a connection idle this long (nothing read, nothing written, nothing outstanding; Kafka's `connections.max.idle.ms`) |
| `pg_kafka.request_read_timeout_ms` | `60000` | A request frame must arrive completely within this long of its first byte |

All of them are read once, **when the background worker starts**. They are `SIGHUP`
settings so `ALTER SYSTEM` + `pg_reload_conf()` can change them, but the new values take
effect only after the `pg_kafka_listener` worker restarts:

```sql
SELECT pg_reload_conf();
SELECT pg_terminate_backend(pid) FROM pg_stat_activity
 WHERE backend_type = 'pg_kafka_listener';   -- the postmaster restarts it after 5 s
```

The worker logs a WARNING after a reload that changed them. A bad value (unknown
protocol, duplicate port, unreadable certificate or key, key that does not match the
certificate, TLS listener without a certificate) stops the worker with an ERROR that
names the setting and the file; the postmaster retries every 5 s, so fix the setting and
reload. The private key file must be readable by the server's OS user.

TLS uses rustls (TLS 1.2 and 1.3, rustls' safe default cipher suites, ring crypto); no
system TLS library is involved. Every socket has `TCP_NODELAY` set (Kafka does the
same), so pipelined responses are not held back by Nagle's algorithm.

**Pre-authentication deadline.** A client has **10 s from the moment its TCP connection
is accepted** to finish everything before authentication: the TLS handshake (SSL,
SASL_SSL) and, on a SASL listener, SASL authentication, including any wait for a
credential lookup, the lookup rate limit below, or a password-check slot. It is one
deadline (a SASL_SSL client does not get 10 s for TLS plus 10 s for SASL).

**Connection limits.** Each listener has its own `pg_kafka.max_connections` cap and each
IP address its own `pg_kafka.max_connections_per_ip` cap across all listeners;
connections over a cap are closed at accept (counted in `kafka.broker_metrics` as
rejected). Because the caps are per listener, a flood on an exposed listener cannot lock
clients out of another one: keep a loopback PLAINTEXT (or SASL) listener for local
administration and applications. A distributed flood from many addresses can still fill
the exposed listener's slots; use a firewall or a proxy in front of it for that.

## SASL

On a `SASL_PLAINTEXT` or `SASL_SSL` listener a client must authenticate before it can do
anything: until SaslAuthenticate succeeds, only ApiVersions, SaslHandshake and
SaslAuthenticate are processed, and any other request closes the connection without a
response (as Kafka does). Before authentication the network thread answers ApiVersions
itself, as Kafka's authenticator does, so an unauthenticated client never puts work on
the database thread; only its credential lookup does (rate-limited, below). After
authentication ApiVersions is served by the database thread like any other request.

* **SaslHandshake** v1. v0, after which tokens are sent unframed, is not supported: it
  is advertised (librdkafka enables SASL only when the broker advertises v0) but a v0
  handshake gets `UNSUPPORTED_VERSION` and the connection closes; every client since
  Kafka 1.0 uses v1 plus SaslAuthenticate. A mechanism that is not enabled
  gets `UNSUPPORTED_SASL_MECHANISM` with the enabled list, and the connection closes.
* **SaslAuthenticate** v0-v2. A failure gets `SASL_AUTHENTICATION_FAILED` with the
  message "Authentication failed: invalid credentials" after a 100 ms delay, then the
  connection closes, so each connection buys one attempt. The message is the same for a
  wrong password and an unknown user.
* Out-of-order requests (SaslAuthenticate before SaslHandshake, a second SaslHandshake)
  get `ILLEGAL_SASL_STATE` and the connection closes. After authentication, and on
  listeners without SASL, both APIs answer `ILLEGAL_SASL_STATE` and the connection stays
  open.
* `session_lifetime_ms` is 0: sessions are not re-authenticated (KIP-368 is not
  implemented), and dropping or changing a user does not close connections that already
  authenticated as it.
* SCRAM: channel binding (`p=`), mandatory extensions and delegation tokens
  (`tokenauth=true`) are rejected. An `a=` authorization id is accepted only if it
  equals the username. Passwords are used as their UTF-8 bytes without SASLprep, like
  Kafka and librdkafka.

The SASL state machine runs on the network thread, one per connection. Only the
credential lookup needs the database: it goes to the background worker over a dedicated
channel, and the worker answers it with one `SELECT` between request batches.

**Lookup rate limit.** Each SCRAM client-first message or PLAIN token costs the database
thread a `SELECT` and costs the client nothing, so each IP address may start at most 10
lookups per second (burst 20). Beyond that, lookups wait their turn; an address more than
20 lookups behind gets no lookup at all, and its connection is closed without an answer
at the pre-authentication deadline (a disconnect, which clients retry, rather than an
authentication failure, which the Java client treats as fatal).

## Users and credentials

Credentials live in `kafka.sasl_credentials`, one row per user and SCRAM mechanism,
stored the way Kafka stores them:

```
SaltedPassword = PBKDF2-HMAC-H(password, salt, iterations)   (32-byte random salt)
StoredKey      = H(HMAC(SaltedPassword, "Client Key"))
ServerKey      = HMAC(SaltedPassword, "Server Key")
```

No password is stored. SCRAM clients are checked against `StoredKey`; SASL/PLAIN
passwords are checked by recomputing `StoredKey` from the SCRAM-SHA-256 credential (or
the SCRAM-SHA-512 one if that is the only one).

```sql
-- Create (SCRAM-SHA-256 and SCRAM-SHA-512 credentials; iterations 4096..16384, default 4096)
SELECT pg_kafka_create_user('alice', 'correct horse battery staple');
SELECT pg_kafka_create_user('bob', 'another secret', 8192);
-- Change the password
SELECT pg_kafka_alter_user('alice', 'new password');
-- Install a verifier computed elsewhere: the plaintext never reaches the server.
-- SCRAM-SHA-256 uses PostgreSQL's format, e.g. from libpq PQencryptPasswordConn().
SELECT pg_kafka_set_user_verifier('carol', 'SCRAM-SHA-256$4096:<salt>$<StoredKey>:<ServerKey>');
-- Drop (returns false if the user did not exist)
SELECT pg_kafka_drop_user('bob');
-- List users (no secrets); readable by pgkafka_reader
SELECT * FROM kafka.sasl_users;
```

The functions are SECURITY INVOKER: they work for roles that may write
`kafka.sasl_credentials`, which by default is only superusers (the extension owner).
`pgkafka_reader` cannot read the table, only the `kafka.sasl_users` view;
`pgkafka_worker` can only `SELECT` it. The table is registered with
`pg_extension_config_dump`, so `pg_dump` includes the verifiers (protect dumps
accordingly).

`pg_kafka_create_user` / `pg_kafka_alter_user` receive the password as an SQL argument.
It is not stored, but it is visible in that session's `pg_stat_activity.query` while the
statement runs, and in the server log if `log_statement` logs it or the statement fails
(`log_min_error_statement`, e.g. "user already exists"). Passing the password as a bind
parameter (`SELECT pg_kafka_create_user($1, $2)` through a driver) keeps it out of the
logged statement text (`log_parameter_max_length_on_error` defaults to 0); otherwise use
`pg_kafka_set_user_verifier`, or a session with statement logging off.

## Principals

Each connection carries a principal in Kafka syntax:

* `User:<username>` after SASL authentication;
* `User:<certificate subject>` (RFC 2253, e.g. `User:CN=app1,O=Acme`) on an `SSL`
  listener with mutual TLS;
* `User:ANONYMOUS` otherwise.

It lives in the connection's `ConnectionAuth` (`src/kafka/listener/sasl.rs`) and is logged
when the connection authenticates and when it closes (with `pg_kafka.log_connections =
on`; the network thread logs through `tracing`, so also run with
`RUST_LOG=pg_kafka=info`). Failed authentications are logged as warnings with the peer
address and mechanism (never the password; not the username either, since users mistype
passwords into it). Nothing checks the principal yet.

## What is and is not protected

Protected, when configured:

* **Confidentiality and integrity on the wire** for `SSL` / `SASL_SSL` listeners
  (TLS 1.2+). `SASL_PLAINTEXT` encrypts nothing: SCRAM does not expose the password, but
  PLAIN sends it in clear text and all record data is readable. The worker warns about
  PLAIN on an exposed SASL_PLAINTEXT listener.
* **Who may connect** to a SASL listener (a known user with the right password) or a
  mutual-TLS listener (a certificate signed by the configured CA).
* **Stored credentials**: salted, iterated SCRAM verifiers only; a database reader with
  table access learns verifiers (which allow offline guessing and impersonating the
  server to a SCRAM client), not passwords. `pgkafka_reader` cannot read them.
* **Pre-authentication resources**: before SASL succeeds a connection may send frames
  of at most 512 KiB (Kafka's `sasl.server.max.receive.size`) and only the three APIs
  above, and it must finish TLS and SASL within 10 s of being accepted, so idle
  unauthenticated clients cannot hold connection slots. Connections are capped per
  listener and per IP address, and credential lookups are rate limited per IP address
  (see above).
* **Online guessing cost**: one attempt per connection, a 100 ms delay before every
  failure, connection caps, 10 lookups per second per address, and SASL/PLAIN's PBKDF2
  (at most 16384 iterations) runs on at most two blocking threads at a time, off the
  async runtime and off the database thread. SCRAM costs the server only a few HMACs per
  attempt.
* **User enumeration**: an unknown user's exchange runs to the end against a fake
  credential and fails exactly like a wrong password. Its salt is
  `HMAC(secret, mechanism:username)`, keyed by a random secret generated at `CREATE
  EXTENSION` and kept in `kafka.sasl_server_secret` (readable only by superusers and
  `pgkafka_worker`), so it is stable for a username across worker restarts, as a real
  user's salt is. Its iteration count and salt length are the most common ones among the
  stored credentials of that mechanism (re-read at most once a minute; 4096 and 32 bytes
  when there are none), and SASL/PLAIN runs the same PBKDF2 against it, so a PLAIN
  failure for an unknown user costs what one for a typical user costs.
  **Residual leak:** a real user whose iteration count or salt length differs from the
  most common one (for example one installed from a libpq verifier, 16-byte salt, among
  users created by `pg_kafka_create_user`, 32-byte salt) is recognizable as real, and so
  is the iteration count's effect on PLAIN timing. Create all users with the same
  parameters to avoid that. A dump/restore into a new installation gets a new secret, so
  fake salts change once then.
* **Timing**: proofs and password-derived keys are compared in constant time.

Not protected:

* **Authorization**: any authenticated (or, on PLAINTEXT/SSL-without-client-CA
  listeners, any connected) client can read and write every topic and run every admin
  API. There are no ACLs.
* **Any PLAINTEXT listener**, including the default `127.0.0.1:9092`, and SSL listeners
  without `pg_kafka.ssl_ca_file`: they accept everyone. Keep the PLAINTEXT listener on
  loopback (or firewall it) when you rely on SASL elsewhere. The worker warns about
  non-loopback listeners that do not authenticate.
* **Revocation**: dropping a user or changing a password does not disconnect existing
  sessions; restart the worker to cut them.
* **Certificate revocation** (CRLs/OCSP) for client certificates is not checked.
* **Secrets in memory**: passwords received over PLAIN are wiped from pg_kafka's own
  buffer after use, but copies made by the protocol decoder are not.
* PostgreSQL's own access (pg_hba.conf, roles) is unchanged and independent.

## Client configuration examples

```properties
# SASL_SSL with SCRAM-SHA-512 (librdkafka / Java)
bootstrap.servers=broker.example.com:9093
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.username=alice
sasl.password=correct horse battery staple
ssl.ca.location=/etc/pg_kafka/ca.pem          # Java: ssl.truststore.location
```

```properties
# Mutual TLS
security.protocol=SSL
ssl.ca.location=/etc/pg_kafka/ca.pem
ssl.certificate.location=/etc/pg_kafka/client.pem
ssl.key.location=/etc/pg_kafka/client.key
```

## Testing

* Unit tests: SCRAM against the RFC 5802 (SHA-1) and RFC 7677 (SHA-256) test vectors,
  SHA-512 round trips, malformed messages, PLAIN parsing, the session state machine,
  listener spec parsing, certificate subject rendering and TLS configuration errors
  (`cargo test --lib kafka::auth`).
* E2E: the `auth` category in `kafka_test` (`cargo run --release -- --category auth`).
  It generates a throwaway CA and certificates with the `openssl` CLI, adds
  `SASL_PLAINTEXT://127.0.0.1:19093`, `SSL://127.0.0.1:19094` and
  `SASL_SSL://127.0.0.1:19095` through `ALTER SYSTEM`, restarts the listener worker,
  runs real rdkafka clients (PLAIN, SCRAM-SHA-256/512, TLS, an untrusted CA, mutual TLS,
  SASL_SSL, wrong passwords, unauthenticated requests) and resets everything at the end.
  It must run on the database host as a superuser with `openssl` on PATH. The
  PLAINTEXT 9092 listener the rest of the suite uses is never changed.
* CI needs nothing new beyond what the root crate already needs: `libssl-dev` (the
  E2E crate's rdkafka builds with `ssl`, which also enables librdkafka's built-in
  PLAIN and SCRAM) and the `openssl` CLI for the E2E certificates. libsasl2 is not
  needed for these mechanisms (only for GSSAPI).
