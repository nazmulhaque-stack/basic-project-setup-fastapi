"""Custom application domain exceptions.

Provides explicit, structured exception types that decouple business logic
from HTTP status codes and prevent leaking internal implementation details.
Conforms to both Clean Architecture and fastapi-repo-pattern error handling.
"""

from typing import Any, Optional
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse


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


# Alias for fastapi-repo-pattern compatibility
AppError = AppException


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


class BusinessRuleError(AppException):
    """Raised when request breaks a business rule (e.g. references missing parent)."""

    status_code = 422
    code = "business_rule_violation"

    def __init__(self, message: str = "Business rule violation", code: str = "business_rule_violation") -> None:
        super().__init__(message=message, code=code, status_code=422)


class InvalidQueryError(AppException):
    """Raised when an unknown filter or sort field reaches the repository."""

    status_code = 400
    code = "invalid_query"

    def __init__(self, message: str = "Invalid query parameter", code: str = "invalid_query") -> None:
        super().__init__(message=message, code=code, status_code=400)


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


async def app_error_handler(_: Request, exc: AppException) -> JSONResponse:
    """Standard error response handler."""
    body: dict[str, Any] = {"detail": exc.message, "code": exc.code}
    if exc.details is not None:
        body["details"] = exc.details
    return JSONResponse(status_code=exc.status_code, content=body)


def register_exception_handlers(app: FastAPI) -> None:
    """Registers exception handlers on the FastAPI application."""
    app.add_exception_handler(AppException, app_error_handler)  # type: ignore[arg-type]


__all__ = [
    "AppException",
    "AppError",
    "NotFoundError",
    "ConflictError",
    "BusinessRuleError",
    "InvalidQueryError",
    "DecryptionError",
    "app_error_handler",
    "register_exception_handlers",
]
