"""Domain exceptions.

Repositories and services raise these; they never import FastAPI or HTTPException.
One handler in main.py turns them into JSON responses, so HTTP details stay in the
API layer (Single Responsibility + Dependency Inversion).
"""

from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse


class AppError(Exception):
    status_code: int = 400
    code: str = "app_error"

    def __init__(self, message: str, *, details: Any = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details


class NotFoundError(AppError):
    status_code = 404
    code = "not_found"


class ConflictError(AppError):
    """Unique / foreign-key constraint violations."""

    status_code = 409
    code = "conflict"


class BusinessRuleError(AppError):
    """Valid JSON, but breaks a business rule (e.g. references a missing parent)."""

    status_code = 422
    code = "business_rule_violation"


class InvalidQueryError(AppError):
    """Unknown filter / sort field reached the repository."""

    status_code = 400
    code = "invalid_query"


async def app_error_handler(_: Request, exc: AppError) -> JSONResponse:
    body: dict[str, Any] = {"detail": exc.message, "code": exc.code}
    if exc.details is not None:
        body["details"] = exc.details
    return JSONResponse(status_code=exc.status_code, content=body)


def register_exception_handlers(app: FastAPI) -> None:
    app.add_exception_handler(AppError, app_error_handler)  # type: ignore[arg-type]
