"""Exponential backoff retry policy for Kafka message handling."""

import logging
from pydantic import ValidationError

logger = logging.getLogger("fastapi_ha.kafka.retry")

NON_RETRYABLE_EXCEPTIONS = (
    ValidationError,
    ValueError,
    KeyError,
    TypeError,
)


class RetryPolicy:
    """Exponential backoff retry policy for Kafka consumer message handling."""

    def __init__(
        self,
        max_retries: int = 3,
        base_backoff_seconds: float = 5.0,
        max_backoff_seconds: float = 60.0,
    ) -> None:
        self.max_retries = max_retries
        self.base_backoff_seconds = base_backoff_seconds
        self.max_backoff_seconds = max_backoff_seconds

    def is_retryable(self, exception: Exception) -> bool:
        """Determines whether the error is transient and worthy of retry.

        Validation and type errors are permanent and route immediately to DLQ.
        """
        if isinstance(exception, NON_RETRYABLE_EXCEPTIONS):
            return False
        return True

    def calculate_backoff(self, attempt: int) -> float:
        """Calculates exponential backoff delay: base * 2^(attempt - 1)."""
        delay = self.base_backoff_seconds * (2 ** max(0, attempt - 1))
        return min(delay, self.max_backoff_seconds)


__all__ = ["RetryPolicy", "NON_RETRYABLE_EXCEPTIONS"]
