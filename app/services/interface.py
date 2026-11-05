"""Cache interface and NullCache implementation."""

from app.core.cache_interface import ICacheService, NullCache

__all__ = [
    "ICacheService",
    "NullCache",
]
