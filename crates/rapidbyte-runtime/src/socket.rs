//! Host TCP socket helpers for plugin network I/O.

use std::net::{IpAddr, SocketAddr, TcpStream, ToSocketAddrs};

#[cfg(unix)]
use std::os::unix::io::AsRawFd;

/// Default milliseconds to wait for socket readiness before returning `WouldBlock`.
/// Override with `RAPIDBYTE_SOCKET_POLL_MS` env var for performance tuning.
pub const SOCKET_READY_POLL_MS: i32 = 1;

/// Initial optimistic `WouldBlock` events that return immediately.
pub const SOCKET_SPIN_THRESHOLD: u32 = 8;

/// Subsequent `WouldBlock` events that yield the current thread before polling.
pub const SOCKET_YIELD_THRESHOLD: u32 = 32;

/// Maximum adaptive readiness poll timeout multiplier.
pub const SOCKET_MAX_POLL_MULTIPLIER: i32 = 8;

/// Adaptive backpressure action for repeated socket `WouldBlock` results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SocketBackpressureAction {
    ReturnWouldBlock,
    Yield,
    Poll(i32),
}

/// Track consecutive `WouldBlock` events and choose the next host-side backpressure action.
///
#[must_use]
pub fn next_socket_backpressure_action(streak: &mut u32) -> SocketBackpressureAction {
    *streak = streak.saturating_add(1);
    if *streak <= SOCKET_SPIN_THRESHOLD {
        return SocketBackpressureAction::ReturnWouldBlock;
    }

    if *streak <= SOCKET_YIELD_THRESHOLD {
        return SocketBackpressureAction::Yield;
    }

    SocketBackpressureAction::Poll(adaptive_socket_poll_timeout_ms(*streak))
}

/// Interest direction for socket readiness polling.
#[cfg(unix)]
#[derive(Clone, Copy)]
pub enum SocketInterest {
    Read,
    Write,
}

/// Wait up to `timeout_ms` for a socket to become ready for the given interest.
///
/// Returns:
/// - `Ok(true)` if the socket is ready (including error/HUP conditions — the
///   caller should discover the specifics via the next I/O call).
/// - `Ok(false)` on a clean timeout (no events).
/// - `Err(io::Error)` on a non-EINTR poll failure (logged by caller for
///   observability, then treated as "ready" to let I/O surface the real error).
///
/// Handles EINTR by retrying. POLLERR/POLLHUP/POLLNVAL are returned as
/// `Ok(true)` so that the subsequent `read()`/`write()` surfaces the real error
/// through normal error handling.
///
/// # Errors
///
/// Returns an `io::Error` on a non-EINTR poll failure.
#[cfg(unix)]
pub fn wait_socket_ready(
    stream: &TcpStream,
    interest: SocketInterest,
    timeout_ms: i32,
) -> std::io::Result<bool> {
    let events = match interest {
        SocketInterest::Read => libc::POLLIN,
        SocketInterest::Write => libc::POLLOUT,
    };
    let mut pfd = libc::pollfd {
        fd: stream.as_raw_fd(),
        events,
        revents: 0,
    };
    loop {
        let ret = unsafe { libc::poll(&raw mut pfd, 1, timeout_ms) };
        if ret < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::Interrupted {
                continue; // EINTR: retry
            }
            return Err(err);
        }
        // ret == 0: clean timeout, socket not ready.
        // ret > 0: socket has events. This includes POLLERR, POLLHUP, POLLNVAL
        // — all of which mean the next I/O call will return the real error/EOF.
        return Ok(ret > 0);
    }
}

/// Read the socket poll timeout, allowing env override for perf tuning.
#[must_use]
pub fn socket_poll_timeout_ms() -> i32 {
    static CACHED: std::sync::OnceLock<i32> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| {
        std::env::var("RAPIDBYTE_SOCKET_POLL_MS")
            .ok()
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(SOCKET_READY_POLL_MS)
    })
}

/// Compute an adaptive readiness poll timeout for sustained backpressure.
#[must_use]
pub fn adaptive_socket_poll_timeout_ms(streak: u32) -> i32 {
    let base = socket_poll_timeout_ms().max(1);
    let multiplier = if streak <= SOCKET_YIELD_THRESHOLD + 32 {
        1
    } else if streak <= SOCKET_YIELD_THRESHOLD + 128 {
        2
    } else if streak <= SOCKET_YIELD_THRESHOLD + 512 {
        4
    } else {
        SOCKET_MAX_POLL_MULTIPLIER
    };
    base.saturating_mul(multiplier)
}

/// Per-socket state tracking the TCP stream and consecutive `WouldBlock` streaks.
pub struct SocketEntry {
    pub stream: TcpStream,
    pub read_would_block_streak: u32,
    pub write_would_block_streak: u32,
}

/// Resolve a hostname and port to a list of socket addresses.
///
/// # Errors
///
/// Returns an `io::Error` if DNS resolution fails or yields no addresses.
pub fn resolve_socket_addrs(host: &str, port: u16) -> std::io::Result<Vec<SocketAddr>> {
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(vec![SocketAddr::new(ip, port)]);
    }

    let addrs: Vec<SocketAddr> = (host, port).to_socket_addrs()?.collect();
    if addrs.is_empty() {
        Err(std::io::Error::new(
            std::io::ErrorKind::AddrNotAvailable,
            "No address",
        ))
    } else {
        Ok(addrs)
    }
}

pub enum SocketReadResult {
    Data(Vec<u8>),
    Eof,
    WouldBlock,
}

pub enum SocketWriteResult {
    Written(u64),
    WouldBlock,
}

#[cfg(test)]
mod tests {
    use super::{
        adaptive_socket_poll_timeout_ms, next_socket_backpressure_action, socket_poll_timeout_ms,
        SocketBackpressureAction, SOCKET_SPIN_THRESHOLD, SOCKET_YIELD_THRESHOLD,
    };

    #[test]
    fn socket_backpressure_action_stages_are_stable() {
        let mut streak = 0_u32;
        for _ in 0..SOCKET_SPIN_THRESHOLD {
            assert_eq!(
                next_socket_backpressure_action(&mut streak),
                SocketBackpressureAction::ReturnWouldBlock
            );
        }
        for _ in (SOCKET_SPIN_THRESHOLD + 1)..=SOCKET_YIELD_THRESHOLD {
            assert_eq!(
                next_socket_backpressure_action(&mut streak),
                SocketBackpressureAction::Yield
            );
        }
        assert!(matches!(
            next_socket_backpressure_action(&mut streak),
            SocketBackpressureAction::Poll(_)
        ));
    }

    #[test]
    fn adaptive_socket_poll_timeout_ms_ramps_and_caps() {
        let initial = adaptive_socket_poll_timeout_ms(SOCKET_YIELD_THRESHOLD + 1);
        let medium = adaptive_socket_poll_timeout_ms(SOCKET_YIELD_THRESHOLD + 200);
        let capped = adaptive_socket_poll_timeout_ms(SOCKET_YIELD_THRESHOLD + 10_000);

        assert!(initial >= 1);
        assert!(medium >= initial);
        assert!(capped >= medium);
        assert_eq!(
            capped,
            socket_poll_timeout_ms() * super::SOCKET_MAX_POLL_MULTIPLIER
        );
    }

    #[cfg(unix)]
    mod socket_poll_tests {
        use super::super::{wait_socket_ready, SocketInterest};

        #[test]
        fn wait_ready_returns_true_when_data_available() {
            use std::io::Write;

            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let client = std::net::TcpStream::connect(addr).unwrap();
            let (mut server, _) = listener.accept().unwrap();
            client.set_nonblocking(true).unwrap();

            server.write_all(b"hello").unwrap();
            // Generous sleep to ensure data arrives in kernel buffer (CI-safe)
            std::thread::sleep(std::time::Duration::from_millis(50));

            assert!(wait_socket_ready(&client, SocketInterest::Read, 500).unwrap());
        }

        #[test]
        fn wait_ready_returns_false_on_timeout() {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let client = std::net::TcpStream::connect(addr).unwrap();
            let _server = listener.accept().unwrap();
            client.set_nonblocking(true).unwrap();

            // No data written — poll should timeout
            assert!(!wait_socket_ready(&client, SocketInterest::Read, 1).unwrap());
        }

        #[test]
        fn wait_ready_writable_for_connected_socket() {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let client = std::net::TcpStream::connect(addr).unwrap();
            let _server = listener.accept().unwrap();
            client.set_nonblocking(true).unwrap();

            // Fresh connected socket with empty send buffer should be writable
            assert!(wait_socket_ready(&client, SocketInterest::Write, 500).unwrap());
        }

        #[test]
        fn wait_ready_detects_peer_close_as_ready() {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let client = std::net::TcpStream::connect(addr).unwrap();
            let (server, _) = listener.accept().unwrap();
            client.set_nonblocking(true).unwrap();

            // Close server side — client should see HUP/readability for EOF
            drop(server);
            std::thread::sleep(std::time::Duration::from_millis(50));

            assert!(wait_socket_ready(&client, SocketInterest::Read, 500).unwrap());
        }
    }
}
