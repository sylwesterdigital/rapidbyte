//! Host TCP socket helpers for plugin network I/O.

use std::net::{IpAddr, SocketAddr, TcpStream, ToSocketAddrs};

#[cfg(unix)]
use std::os::unix::io::AsRawFd;

/// Default milliseconds to wait for socket readiness before returning `WouldBlock`.
/// Override with `RAPIDBYTE_SOCKET_POLL_MS` env var for performance tuning.
pub const SOCKET_READY_POLL_MS: i32 = 1;

/// Number of consecutive `WouldBlock` events before activating poll(1ms).
/// At ~2M iterations/sec CPU speed, 1024 iterations is roughly 0.5ms of spinning.
/// This avoids adding 1ms poll overhead to transient `WouldBlock` results during active streaming.
pub const SOCKET_POLL_ACTIVATION_THRESHOLD: u32 = 1024;

/// Track consecutive `WouldBlock` events and decide when to activate host readiness polling.
///
/// Returns `true` when the caller should perform an actual socket readiness wait.
///
/// The streak remains at-or-above threshold until caller-side progress resets it.
#[must_use]
pub fn should_activate_socket_poll(streak: &mut u32) -> bool {
    *streak = streak.saturating_add(1);
    *streak >= SOCKET_POLL_ACTIVATION_THRESHOLD
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
    use super::{should_activate_socket_poll, SOCKET_POLL_ACTIVATION_THRESHOLD};

    #[test]
    fn socket_poll_activation_threshold_and_saturation_are_stable() {
        let mut streak = 0_u32;
        for _ in 0..(SOCKET_POLL_ACTIVATION_THRESHOLD - 1) {
            assert!(!should_activate_socket_poll(&mut streak));
        }
        assert!(should_activate_socket_poll(&mut streak));
        assert!(should_activate_socket_poll(&mut streak));
        assert!(streak >= SOCKET_POLL_ACTIVATION_THRESHOLD);
    }

    #[cfg(unix)]
    mod socket_poll_tests {
        use super::super::{wait_socket_ready, SocketInterest};

        fn sandbox_skip<T>(result: std::io::Result<T>, context: &str) -> Option<T> {
            match result {
                Ok(value) => Some(value),
                Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                    eprintln!("Skipping socket poll test during {context}: {error}");
                    None
                }
                Err(error) => panic!("{context} failed unexpectedly: {error}"),
            }
        }

        fn wait_ready_or_skip(
            stream: &std::net::TcpStream,
            interest: SocketInterest,
            timeout_ms: i32,
        ) -> Option<bool> {
            match wait_socket_ready(stream, interest, timeout_ms) {
                Ok(ready) => Some(ready),
                Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                    eprintln!("Skipping socket poll test: poll() not permitted in this sandbox");
                    None
                }
                Err(error) => panic!("wait_socket_ready failed unexpectedly: {error}"),
            }
        }

        fn connected_pair() -> Option<(std::net::TcpStream, std::net::TcpStream)> {
            let listener = sandbox_skip(
                std::net::TcpListener::bind("127.0.0.1:0"),
                "binding test listener",
            )?;
            let addr = listener.local_addr().unwrap();
            let client =
                sandbox_skip(std::net::TcpStream::connect(addr), "connecting test client")?;
            let (server, _) = sandbox_skip(listener.accept(), "accepting test client")?;
            sandbox_skip(
                client.set_nonblocking(true),
                "marking test client nonblocking",
            )?;
            Some((client, server))
        }

        #[test]
        fn wait_ready_returns_true_when_data_available() {
            use std::io::Write;

            let Some((client, mut server)) = connected_pair() else {
                return;
            };

            server.write_all(b"hello").unwrap();
            // Generous sleep to ensure data arrives in kernel buffer (CI-safe)
            std::thread::sleep(std::time::Duration::from_millis(50));

            let Some(ready) = wait_ready_or_skip(&client, SocketInterest::Read, 500) else {
                return;
            };
            assert!(ready);
        }

        #[test]
        fn wait_ready_returns_false_on_timeout() {
            let Some((client, _server)) = connected_pair() else {
                return;
            };

            // No data written — poll should timeout
            let Some(ready) = wait_ready_or_skip(&client, SocketInterest::Read, 1) else {
                return;
            };
            assert!(!ready);
        }

        #[test]
        fn wait_ready_writable_for_connected_socket() {
            let Some((client, _server)) = connected_pair() else {
                return;
            };

            // Fresh connected socket with empty send buffer should be writable
            let Some(ready) = wait_ready_or_skip(&client, SocketInterest::Write, 500) else {
                return;
            };
            assert!(ready);
        }

        #[test]
        fn wait_ready_detects_peer_close_as_ready() {
            let Some((client, server)) = connected_pair() else {
                return;
            };

            // Close server side — client should see HUP/readability for EOF
            drop(server);
            std::thread::sleep(std::time::Duration::from_millis(50));

            let Some(ready) = wait_ready_or_skip(&client, SocketInterest::Read, 500) else {
                return;
            };
            assert!(ready);
        }
    }
}
