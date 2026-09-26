//! Token-bucket rate limiting with partial-token grants so large network chunks
//! are throttled smoothly instead of stalling the pipeline.

use std::sync::Mutex;
use std::time::{Duration, Instant};

pub struct TokenBucket {
    inner: Mutex<BucketState>,
    rate_bps: std::sync::atomic::AtomicU64,
}

struct BucketState {
    tokens: f64,
    last_refill: Instant,
}

impl TokenBucket {
    /// A bucket refilled continuously at `rate_bps` with a one-second burst capacity.
    pub fn new(rate_bps: u64) -> Self {
        Self {
            inner: Mutex::new(BucketState {
                tokens: rate_bps as f64,
                last_refill: Instant::now(),
            }),
            rate_bps: std::sync::atomic::AtomicU64::new(rate_bps),
        }
    }

    pub fn rate_bps(&self) -> u64 {
        self.rate_bps.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Change the rate live (bandwidth scheduler).
    pub fn set_rate_bps(&self, rate_bps: u64) {
        self.rate_bps
            .store(rate_bps, std::sync::atomic::Ordering::Relaxed);
    }

    fn refill(&self, state: &mut BucketState) {
        let now = Instant::now();
        let dt = now.duration_since(state.last_refill).as_secs_f64();
        if dt <= 0.0 {
            return;
        }
        state.last_refill = now;
        let rate = self.rate_bps();
        // Token capacity is capped at one second of bandwidth to allow small bursts.
        state.tokens = (state.tokens + dt * rate as f64).min(rate.max(16384) as f64);
    }

    /// Asynchronously acquire `bytes` worth of bandwidth. Grants partial tokens
    /// immediately when available and waits for the refill otherwise, so callers
    /// holding large chunks release them in ~64KB sub-units without stalling.
    pub async fn acquire(&self, bytes: usize) {
        let mut remaining = bytes as f64;
        while remaining > 0.0 {
            let granted = {
                let mut state = self.inner.lock().expect("rate limiter poisoned");
                self.refill(&mut state);
                let grant = state.tokens.min(remaining).min(65_536.0);
                if grant > 0.0 {
                    state.tokens -= grant;
                    remaining -= grant;
                }
                grant
            };
            // Only sleep when the bucket is effectively drained; otherwise keep
            // pulling at memory speed (unlimited / large-burst regimes).
            if remaining > 0.0 && granted < 16_384.0 {
                let wait_for = 65_536.0_f64.min(remaining).max(4096.0);
                let rate = self.rate_bps().max(1) as f64;
                let sleep_ms = (wait_for / rate * 1000.0).clamp(1.0, 25.0);
                tokio::time::sleep(Duration::from_millis(sleep_ms as u64)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn limits_bandwidth() {
        // 1 MB/s bucket with a one-second burst capacity; acquiring 4 MB must
        // take ~4 s (1 MB burst + 3 MB at rate).
        let bucket = TokenBucket::new(1 << 20);
        let start = Instant::now();
        bucket.acquire(4 << 20).await;
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(2500),
            "acquire completed too fast: {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_secs(12),
            "acquire too slow: {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn unlimited_via_huge_rate() {
        let bucket = TokenBucket::new(u64::MAX / 2);
        let start = Instant::now();
        bucket.acquire(1 << 20).await;
        assert!(start.elapsed() < Duration::from_millis(500));
    }
}
