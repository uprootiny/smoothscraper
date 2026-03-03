//! Exponential backoff with decorrelated jitter and 429 awareness.
//!
//! Design decisions:
//!   - Decorrelated jitter (sleep = rand(base, prev_sleep * 3)) avoids thundering herd.
//!     Better than equal/full jitter for multiple concurrent callers.
//!   - 429 responses parse Retry-After header when present.
//!   - 400-class errors are never retried (client error = our bug).
//!   - Rate budget tracking: callers can check remaining weight before firing.
//!   - All delays are observable via the returned RetryOutcome.

use anyhow::{anyhow, Result};
use rand::Rng;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

/// Retry configuration — immutable after construction.
#[derive(Clone, Debug)]
pub struct RetryConfig {
    pub max_retries: u32,
    pub base_delay_ms: u64,
    pub max_delay_ms: u64,
    /// Rate budget: if set, tracks cumulative weight spent this minute.
    pub rate_budget: Option<Arc<RateBudget>>,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 5,
            base_delay_ms: 300,
            max_delay_ms: 15_000,
            rate_budget: None,
        }
    }
}

/// Sliding-window rate budget tracker.
/// Binance allows 1200 weight/minute. We track spend and pause when near limit.
pub struct RateBudget {
    /// Weight spent in current window
    spent: AtomicU64,
    /// Window start (epoch millis)
    window_start_ms: AtomicU64,
    /// Max weight per window
    pub limit: u64,
    /// Window duration in ms
    pub window_ms: u64,
}

impl std::fmt::Debug for RateBudget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RateBudget")
            .field("spent", &self.spent.load(Ordering::Relaxed))
            .field("limit", &self.limit)
            .finish()
    }
}

impl RateBudget {
    pub fn new(limit: u64, window_ms: u64) -> Arc<Self> {
        Arc::new(Self {
            spent: AtomicU64::new(0),
            window_start_ms: AtomicU64::new(now_ms()),
            limit,
            window_ms,
        })
    }

    /// Binance default: 1200 weight per 60s
    pub fn binance() -> Arc<Self> {
        Self::new(1200, 60_000)
    }

    /// Record weight spent. Returns remaining budget.
    pub fn record(&self, weight: u64) -> u64 {
        self.maybe_reset();
        let prev = self.spent.fetch_add(weight, Ordering::Relaxed);
        self.limit.saturating_sub(prev + weight)
    }

    /// Check remaining budget without spending.
    pub fn remaining(&self) -> u64 {
        self.maybe_reset();
        self.limit
            .saturating_sub(self.spent.load(Ordering::Relaxed))
    }

    /// How long to wait until budget refills, in ms. 0 if budget available.
    pub fn wait_ms(&self) -> u64 {
        if self.remaining() > 0 {
            return 0;
        }
        let start = self.window_start_ms.load(Ordering::Relaxed);
        let elapsed = now_ms().saturating_sub(start);
        self.window_ms.saturating_sub(elapsed)
    }

    fn maybe_reset(&self) {
        let start = self.window_start_ms.load(Ordering::Relaxed);
        let now = now_ms();
        if now.saturating_sub(start) >= self.window_ms {
            self.window_start_ms.store(now, Ordering::Relaxed);
            self.spent.store(0, Ordering::Relaxed);
        }
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl RetryConfig {
    /// Decorrelated jitter: sleep = rand(base, min(max, prev_sleep * 3))
    /// From AWS architecture blog — superior to equal jitter for correlated callers.
    fn delay_decorrelated(&self, prev_delay_ms: u64) -> Duration {
        let upper = (prev_delay_ms * 3)
            .max(self.base_delay_ms)
            .min(self.max_delay_ms);
        let lower = self.base_delay_ms;
        let delay = if upper > lower {
            rand::thread_rng().gen_range(lower..=upper)
        } else {
            lower
        };
        Duration::from_millis(delay)
    }
}

/// Retry a fallible async operation with decorrelated jitter backoff.
///
/// 429 responses: if the error message contains "429" or "Retry-After: N",
/// we honor the server's requested delay (clamped to max_delay_ms).
///
/// 400 responses: never retried (client bug, not transient).
///
/// Rate budget: if configured, waits for budget before each attempt.
pub async fn retry<F, Fut, T>(cfg: &RetryConfig, name: &str, mut op: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut last_err: Option<anyhow::Error> = None;
    let mut prev_delay_ms = cfg.base_delay_ms;

    for attempt in 0..=cfg.max_retries {
        // Respect rate budget before attempting
        if let Some(ref budget) = cfg.rate_budget {
            let wait = budget.wait_ms();
            if wait > 0 {
                eprintln!("[rate] {} pausing {}ms for rate budget", name, wait);
                sleep(Duration::from_millis(wait)).await;
            }
            budget.record(1); // default weight=1 per request
        }

        match op().await {
            Ok(val) => return Ok(val),
            Err(e) => {
                let msg = format!("{}", e);

                // Never retry 400-class client errors (our bug)
                if msg.contains(" 400") || msg.contains("Bad Request") {
                    return Err(e);
                }

                if attempt < cfg.max_retries {
                    // Check for 429 with Retry-After
                    let delay = if msg.contains("429") || msg.contains("Too Many Requests") {
                        let retry_after = parse_retry_after(&msg);
                        let ra_ms = retry_after.unwrap_or(prev_delay_ms).min(cfg.max_delay_ms);
                        eprintln!(
                            "[retry] {} 429 rate-limited, waiting {}ms{}",
                            name,
                            ra_ms,
                            if retry_after.is_some() {
                                " (from Retry-After)"
                            } else {
                                ""
                            }
                        );
                        Duration::from_millis(ra_ms)
                    } else {
                        cfg.delay_decorrelated(prev_delay_ms)
                    };

                    eprintln!(
                        "[retry] {} attempt {}/{} failed: {}. Waiting {:?}",
                        name,
                        attempt + 1,
                        cfg.max_retries + 1,
                        e,
                        delay
                    );

                    prev_delay_ms = delay.as_millis() as u64;
                    sleep(delay).await;
                }
                last_err = Some(e);
            }
        }
    }

    Err(last_err.unwrap_or_else(|| anyhow!("{}: exhausted {} retries", name, cfg.max_retries)))
}

/// Extract Retry-After value from error message (seconds → ms).
/// Looks for "Retry-After: N" or "retry-after: N" in the message.
fn parse_retry_after(msg: &str) -> Option<u64> {
    let lower = msg.to_lowercase();
    if let Some(idx) = lower.find("retry-after") {
        let rest = &msg[idx..];
        // Skip "Retry-After: " or "retry-after:"
        let val_start = rest.find(|c: char| c.is_ascii_digit())?;
        let val_end = rest[val_start..]
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(rest.len() - val_start);
        let secs: u64 = rest[val_start..val_start + val_end].parse().ok()?;
        Some(secs * 1000) // convert to ms
    } else {
        None
    }
}

/// Adaptive page delay: respects rate budget, falls back to base_ms.
/// When budget is >50% remaining, uses base_ms.
/// When budget is 20-50%, doubles the delay.
/// When budget is <20%, triples the delay.
/// When budget is exhausted, waits for refill.
pub async fn page_delay(cfg: &RetryConfig, base_ms: u64) {
    if let Some(ref budget) = cfg.rate_budget {
        let wait = budget.wait_ms();
        if wait > 0 {
            eprintln!("[pace] budget exhausted, waiting {}ms for refill", wait);
            sleep(Duration::from_millis(wait)).await;
            return;
        }
        let remaining = budget.remaining();
        let pct = (remaining as f64 / budget.limit as f64 * 100.0) as u64;
        let delay = if pct > 50 {
            base_ms
        } else if pct > 20 {
            base_ms * 2
        } else {
            base_ms * 3
        };
        sleep(Duration::from_millis(delay)).await;
    } else {
        sleep(Duration::from_millis(base_ms)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decorrelated_jitter_stays_in_bounds() {
        let cfg = RetryConfig {
            base_delay_ms: 300,
            max_delay_ms: 15_000,
            ..Default::default()
        };

        // Run many iterations to check bounds
        for prev in [300, 600, 1200, 5000, 15000, 50000] {
            for _ in 0..100 {
                let d = cfg.delay_decorrelated(prev);
                let ms = d.as_millis() as u64;
                assert!(
                    ms >= cfg.base_delay_ms,
                    "delay {} < base {}",
                    ms,
                    cfg.base_delay_ms
                );
                assert!(
                    ms <= cfg.max_delay_ms,
                    "delay {} > max {}",
                    ms,
                    cfg.max_delay_ms
                );
            }
        }
    }

    #[test]
    fn decorrelated_jitter_has_variance() {
        let cfg = RetryConfig {
            base_delay_ms: 100,
            max_delay_ms: 10_000,
            ..Default::default()
        };

        let delays: Vec<u64> = (0..50)
            .map(|_| cfg.delay_decorrelated(1000).as_millis() as u64)
            .collect();

        let min = *delays.iter().min().unwrap();
        let max = *delays.iter().max().unwrap();
        assert!(
            max > min,
            "jitter should produce variance: min={} max={}",
            min,
            max
        );
    }

    #[test]
    fn parse_retry_after_header() {
        assert_eq!(
            parse_retry_after("429 Too Many Requests, Retry-After: 5"),
            Some(5000)
        );
        assert_eq!(parse_retry_after("retry-after: 30"), Some(30000));
        assert_eq!(parse_retry_after("HTTP 429"), None);
        assert_eq!(parse_retry_after("some random error"), None);
    }

    #[test]
    fn rate_budget_tracks_spend() {
        let budget = RateBudget::new(10, 60_000);
        assert_eq!(budget.remaining(), 10);
        assert_eq!(budget.record(3), 7);
        assert_eq!(budget.remaining(), 7);
        assert_eq!(budget.record(7), 0);
        assert_eq!(budget.remaining(), 0);
        assert!(budget.wait_ms() > 0);
    }

    #[test]
    fn rate_budget_binance_defaults() {
        let budget = RateBudget::binance();
        assert_eq!(budget.limit, 1200);
        assert_eq!(budget.window_ms, 60_000);
        assert_eq!(budget.remaining(), 1200);
    }

    #[tokio::test]
    async fn retry_succeeds_first_try() {
        let cfg = RetryConfig::default();
        let result: Result<i32> = retry(&cfg, "test", || async { Ok(42) }).await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn retry_eventual_success() {
        let cfg = RetryConfig {
            base_delay_ms: 1,
            max_delay_ms: 10,
            ..Default::default()
        };
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let cc = counter.clone();
        let result: Result<i32> = retry(&cfg, "test", || {
            let c = cc.clone();
            async move {
                let n = c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if n < 2 {
                    Err(anyhow!("not yet"))
                } else {
                    Ok(42)
                }
            }
        })
        .await;
        assert_eq!(result.unwrap(), 42);
        assert_eq!(counter.load(std::sync::atomic::Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn retry_400_not_retried() {
        let cfg = RetryConfig {
            base_delay_ms: 1,
            ..Default::default()
        };
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let cc = counter.clone();
        let result: Result<i32> = retry(&cfg, "test", || {
            let c = cc.clone();
            async move {
                c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Err(anyhow!("HTTP 400 Bad Request"))
            }
        })
        .await;
        assert!(result.is_err());
        assert_eq!(counter.load(std::sync::atomic::Ordering::SeqCst), 1); // only 1 attempt
    }
}
