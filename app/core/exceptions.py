"""Custom application domain exceptions.

Provides explicit, structured exception types that decouple business logic
from HTTP status codes and prevent leaking internal implementation details.
"""

from typing import Any, Optional


class AppException(Exception):
    """Base exception for all domain-specific application errors."""

    status_code: int = 500
    code: str = "INTERNAL_ERROR"

    def __init__(
        self,
        message: str,
        code: Optional[str] = None,
        status_code: Optional[int] = None,
        details: Optional[Any] = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        if code is not None:
            self.code = code
        if status_code is not None:
            self.status_code = status_code
        self.details = details


class NotFoundError(AppException):
    """Raised when a requested resource is not found."""

    status_code = 404
    code = "not_found"

    def __init__(self, message: str = "Resource not found", code: str = "not_found") -> None:
        super().__init__(message=message, code=code, status_code=404)


class ConflictError(AppException):
    """Raised when an operation violates a unique constraint or business conflict."""

    status_code = 409
    code = "conflict"

    def __init__(self, message: str = "Resource conflict", code: str = "conflict") -> None:
        super().__init__(message=message, code=code, status_code=409)


class DecryptionError(AppException):
    """Raised when ciphertext cannot be decrypted or authentication tag fails."""

    status_code = 400
    code = "decryption_failed"

    def __init__(
        self,
        message: str = "Unable to decrypt data or authentication tag mismatch.",
        code: str = "decryption_failed",
    ) -> None:
        super().__init__(message=message, code=code, status_code=400)


__all__ = [
    "AppException",
    "NotFoundError",
    "ConflictError",
    "DecryptionError",
]
