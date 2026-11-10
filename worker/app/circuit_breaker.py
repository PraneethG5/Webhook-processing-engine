import time
import logging
from typing import Dict
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

class CircuitBreakerOpenException(Exception):
    """Raised when request is dropped because destination circuit breaker is OPEN."""
    pass

class HostCircuitBreaker:
    STATE_CLOSED = "CLOSED"
    STATE_OPEN = "OPEN"
    STATE_HALF_OPEN = "HALF_OPEN"

    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 30.0):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.state = self.STATE_CLOSED
        self.consecutive_failures = 0
        self.last_failure_time = 0.0

    def can_attempt(self) -> bool:
        now = time.time()
        if self.state == self.STATE_CLOSED:
            return True

        if self.state == self.STATE_OPEN:
            if now - self.last_failure_time >= self.recovery_timeout:
                logger.info("Circuit transitioning from OPEN to HALF_OPEN (probing)")
                self.state = self.STATE_HALF_OPEN
                return True
            return False

        if self.state == self.STATE_HALF_OPEN:
            # In half-open, allow single probe attempt
            return True

        return True

    def record_success(self):
        if self.state != self.STATE_CLOSED:
            logger.info("Probe succeeded. Circuit reset to CLOSED.")
        self.state = self.STATE_CLOSED
        self.consecutive_failures = 0

    def record_failure(self):
        self.last_failure_time = time.time()
        self.consecutive_failures += 1

        if self.state == self.STATE_HALF_OPEN:
            logger.warning("Probe failed in HALF_OPEN state. Tripping circuit back to OPEN.")
            self.state = self.STATE_OPEN
        elif self.consecutive_failures >= self.failure_threshold:
            logger.warning(
                f"Consecutive failures ({self.consecutive_failures}) reached threshold ({self.failure_threshold}). "
                f"Tripping circuit to OPEN for {self.recovery_timeout}s."
            )
            self.state = self.STATE_OPEN

class CircuitBreakerRegistry:
    def __init__(self):
        self._breakers: Dict[str, HostCircuitBreaker] = {}

    def get_breaker(self, url: str) -> HostCircuitBreaker:
        parsed = urlparse(url)
        host = parsed.netloc or parsed.path
        if host not in self._breakers:
            self._breakers[host] = HostCircuitBreaker()
        return self._breakers[host]
