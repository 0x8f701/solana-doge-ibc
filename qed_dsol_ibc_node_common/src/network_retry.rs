use std::{error::Error as StdError, future::Future, io, time::{Duration, Instant}};

use tokio::time::sleep;

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const DEFAULT_INITIAL_DELAY: Duration = Duration::from_millis(500);
const DEFAULT_MAX_DELAY: Duration = Duration::from_secs(10);
/// Bounded overall wall-clock budget for the default policy so production
/// retry loops always terminate instead of retrying forever.
const DEFAULT_OVERALL_DEADLINE: Duration = Duration::from_secs(600);
/// Bounded maximum number of attempts for the default policy.
const DEFAULT_MAX_ATTEMPTS: u64 = 20;

#[derive(Debug, Clone, Copy)]
pub(crate) struct NetworkRetryPolicy {
    request_timeout: Duration,
    initial_delay: Duration,
    max_delay: Duration,
    /// Wall-clock budget for the entire retry loop. When elapsed, the loop
    /// terminates and returns the last transient error instead of retrying.
    overall_deadline: Option<Duration>,
    /// Maximum number of attempts (including the first). When reached, the
    /// loop terminates and returns the last transient error.
    max_attempts: Option<u64>,
}

impl NetworkRetryPolicy {
    pub(crate) const fn new(
        request_timeout: Duration,
        initial_delay: Duration,
        max_delay: Duration,
    ) -> Self {
        Self {
            request_timeout,
            initial_delay,
            max_delay,
            overall_deadline: None,
            max_attempts: None,
        }
    }

    /// Bound the entire retry loop with an overall wall-clock deadline.
    pub(crate) const fn with_overall_deadline(mut self, deadline: Duration) -> Self {
        self.overall_deadline = Some(deadline);
        self
    }

    /// Cap the total number of attempts (the first attempt plus retries).
    pub(crate) const fn with_max_attempts(mut self, max_attempts: u64) -> Self {
        self.max_attempts = Some(max_attempts);
        self
    }

    pub(crate) const fn request_timeout(self) -> Duration {
        self.request_timeout
    }

    /// Whether this policy carries both an overall deadline and a max attempts
    /// cap, guaranteeing that a permanently-failing operation terminates.
    pub(crate) const fn is_bounded(self) -> bool {
        self.overall_deadline.is_some() && self.max_attempts.is_some()
    }

    pub(crate) async fn run<T, E, F, Fut>(
        self,
        operation_name: &'static str,
        mut operation: F,
    ) -> Result<T, E>
    where
        E: std::fmt::Display,
        F: FnMut() -> Fut,
        Fut: Future<Output = RetryAction<T, E>>,
    {
        let start = Instant::now();
        let mut attempt = 1u64;
        let mut delay = self.initial_delay;
        let mut last_error: Option<E> = None;
        loop {
            match operation().await {
                RetryAction::Success(value) => return Ok(value),
                RetryAction::Fatal(error) => return Err(error),
                RetryAction::Retry(error) => {
                    eprintln!(
                        "{operation_name} failed with a transient network error on attempt {attempt}: {error}; retrying in {} ms",
                        delay.as_millis(),
                    );
                    last_error = Some(error);

                    // Cap the total number of attempts. The just-finished
                    // attempt is counted, so reaching the cap terminates the
                    // loop and surfaces the last transient error.
                    if let Some(max_attempts) = self.max_attempts {
                        if attempt >= max_attempts {
                            return Err(last_error
                                .expect("a retry error is recorded before termination"));
                        }
                    }

                    // Honor the overall wall-clock budget: do not start a new
                    // attempt or sleep past the deadline, and clamp the backoff
                    // sleep to the remaining budget.
                    let sleep_duration = match self.overall_deadline {
                        Some(deadline) => {
                            let elapsed = start.elapsed();
                            if elapsed >= deadline {
                                return Err(last_error
                                    .expect("a retry error is recorded before termination"));
                            }
                            let remaining = deadline - elapsed;
                            delay.min(remaining)
                        }
                        None => delay,
                    };

                    sleep(sleep_duration).await;
                    attempt = attempt.saturating_add(1);
                    delay = delay.saturating_mul(2).min(self.max_delay);
                }
            }
        }
    }
}

impl Default for NetworkRetryPolicy {
    fn default() -> Self {
        Self::new(
            DEFAULT_REQUEST_TIMEOUT,
            DEFAULT_INITIAL_DELAY,
            DEFAULT_MAX_DELAY,
        )
        .with_overall_deadline(DEFAULT_OVERALL_DEADLINE)
        .with_max_attempts(DEFAULT_MAX_ATTEMPTS)
    }
}

pub(crate) enum RetryAction<T, E> {
    Success(T),
    Retry(E),
    Fatal(E),
}

pub(crate) fn is_retryable_http_status(status: u16) -> bool {
    matches!(status, 408 | 425 | 429 | 500 | 502 | 503 | 504)
}

pub(crate) fn is_retryable_io_kind(kind: io::ErrorKind) -> bool {
    matches!(
        kind,
        io::ErrorKind::BrokenPipe
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::Interrupted
            | io::ErrorKind::NotConnected
            | io::ErrorKind::TimedOut
            | io::ErrorKind::UnexpectedEof
            | io::ErrorKind::WouldBlock
    )
}

pub(crate) fn has_retryable_io_source(error: &(dyn StdError + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(source) = current {
        if let Some(io_error) = source.downcast_ref::<io::Error>() {
            if is_retryable_io_kind(io_error.kind()) {
                return true;
            }
        }
        current = source.source();
    }
    false
}

pub(crate) fn is_retryable_reqwest(error: &reqwest::Error) -> bool {
    if error.is_builder() || error.is_redirect() {
        return false;
    }
    if error.is_timeout()
        || error.is_connect()
        || error.is_request()
        || error.is_body()
        || has_retryable_io_source(error)
    {
        return true;
    }
    error
        .status()
        .is_some_and(|status| is_retryable_http_status(status.as_u16()))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[tokio::test]
    async fn retries_transient_failures_until_success() {
        let attempts = AtomicUsize::new(0);
        let policy = NetworkRetryPolicy::new(
            Duration::from_secs(1),
            Duration::from_millis(1),
            Duration::from_millis(2),
        );

        let result = policy
            .run("test operation", || async {
                if attempts.fetch_add(1, Ordering::SeqCst) < 2 {
                    RetryAction::Retry("temporary")
                } else {
                    RetryAction::Success(42)
                }
            })
            .await;

        assert_eq!(result, Ok(42));
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn returns_non_retryable_failures_immediately() {
        let attempts = AtomicUsize::new(0);
        let result: Result<(), &str> = NetworkRetryPolicy::default()
            .run("test operation", || async {
                attempts.fetch_add(1, Ordering::SeqCst);
                RetryAction::Fatal("invalid response")
            })
            .await;

        assert_eq!(result, Err("invalid response"));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn default_policy_is_bounded_so_production_retries_terminate() {
        // The production default must carry an overall deadline and a max
        // attempts cap so a permanently-failing operation terminates instead
        // of retrying forever. initialize()/poll_once() use this default.
        assert!(NetworkRetryPolicy::default().is_bounded());
        // The low-level `new` constructor is intentionally unbounded; callers
        // must opt into bounds via the builders.
        assert!(!NetworkRetryPolicy::new(
            Duration::from_secs(1),
            Duration::from_millis(1),
            Duration::from_millis(2)
        )
        .is_bounded());
    }

    #[tokio::test]
    async fn always_retry_terminates_at_max_attempts() {
        let attempts = AtomicUsize::new(0);
        let policy = NetworkRetryPolicy::new(
            Duration::from_secs(1),
            Duration::from_millis(1),
            Duration::from_millis(2),
        )
        .with_max_attempts(5);

        let result: Result<(), &str> = policy
            .run("always retry", || async {
                attempts.fetch_add(1, Ordering::SeqCst);
                RetryAction::Retry("still transient")
            })
            .await;

        assert_eq!(result, Err("still transient"));
        // The first attempt plus four retries, then termination.
        assert_eq!(attempts.load(Ordering::SeqCst), 5);
    }

    #[tokio::test]
    async fn always_retry_terminates_at_overall_deadline() {
        let attempts = AtomicUsize::new(0);
        let policy = NetworkRetryPolicy::new(
            Duration::from_secs(1),
            Duration::from_millis(20),
            Duration::from_millis(40),
        )
        .with_overall_deadline(Duration::from_millis(60));

        let result: Result<(), &str> = policy
            .run("deadline bounded", || async {
                attempts.fetch_add(1, Ordering::SeqCst);
                RetryAction::Retry("still transient")
            })
            .await;

        assert_eq!(result, Err("still transient"));
        // The deadline guarantees termination without relying on max attempts.
        assert!(attempts.load(Ordering::SeqCst) >= 2);
    }

    #[test]
    fn retryable_statuses_are_limited_to_transient_failures() {
        for status in [408, 425, 429, 500, 502, 503, 504] {
            assert!(is_retryable_http_status(status), "HTTP {status} should retry");
        }
        for status in [400, 401, 403, 404, 409, 422] {
            assert!(!is_retryable_http_status(status), "HTTP {status} must fail closed");
        }
    }

    #[test]
    fn connection_reset_and_eof_are_retryable() {
        assert!(is_retryable_io_kind(io::ErrorKind::ConnectionReset));
        assert!(is_retryable_io_kind(io::ErrorKind::UnexpectedEof));
        assert!(!is_retryable_io_kind(io::ErrorKind::InvalidData));
    }
}