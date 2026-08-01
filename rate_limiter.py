"""
Rate limiting algorithms, implemented from scratch (no external libraries).

Two strategies are provided:
  - TokenBucket:     O(1) memory per client, allows short bursts.
  - SlidingWindowLog: exact counting over a trailing window, O(limit) memory.

Both are thread-safe and designed to be called from many connection-handling
threads concurrently inside the load balancer.
"""

import time
import threading
from collections import deque


class TokenBucket:
    """Classic token bucket: tokens refill continuously at `refill_rate`
    tokens/sec, up to `capacity`. Each request costs one token. This allows
    short bursts up to `capacity` while enforcing a long-run average rate.
    """

    def __init__(self, capacity: int, refill_rate: float):
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.tokens = float(capacity)
        self.last_refill = time.monotonic()
        self.lock = threading.Lock()

    def allow(self, cost: float = 1.0) -> bool:
        with self.lock:
            now = time.monotonic()
            elapsed = now - self.last_refill
            self.last_refill = now
            self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
            if self.tokens >= cost:
                self.tokens -= cost
                return True
            return False


class SlidingWindowLog:
    """Sliding window log: keeps an exact timestamp log of accepted requests
    within the trailing `window_seconds`. More accurate than fixed-window
    counters (no boundary burst problem) at the cost of O(limit) memory per
    client.
    """

    def __init__(self, limit: int, window_seconds: float):
        self.limit = limit
        self.window = window_seconds
        self.log = deque()
        self.lock = threading.Lock()

    def allow(self) -> bool:
        with self.lock:
            now = time.monotonic()
            cutoff = now - self.window
            while self.log and self.log[0] < cutoff:
                self.log.popleft()
            if len(self.log) < self.limit:
                self.log.append(now)
                return True
            return False


class RateLimiterRegistry:
    """Lazily creates and owns one rate limiter per client key (e.g. source
    IP), so every client gets an independent budget.
    """

    def __init__(self, algorithm="token_bucket", **kwargs):
        self.algorithm = algorithm
        self.kwargs = kwargs
        self.clients = {}
        self.lock = threading.Lock()

    def _new_limiter(self):
        if self.algorithm == "token_bucket":
            return TokenBucket(
                capacity=self.kwargs.get("capacity", 20),
                refill_rate=self.kwargs.get("refill_rate", 10),
            )
        elif self.algorithm == "sliding_window":
            return SlidingWindowLog(
                limit=self.kwargs.get("limit", 20),
                window_seconds=self.kwargs.get("window_seconds", 1.0),
            )
        raise ValueError(f"unknown algorithm: {self.algorithm}")

    def allow(self, client_key: str) -> bool:
        with self.lock:
            limiter = self.clients.get(client_key)
            if limiter is None:
                limiter = self._new_limiter()
                self.clients[client_key] = limiter
        return limiter.allow()
