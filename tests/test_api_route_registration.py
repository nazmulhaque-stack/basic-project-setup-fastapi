"""Tests for application-level API route registration."""

from collections import Counter

from fastapi.routing import APIRoute

from app.main import app


def test_api_routes_are_mounted_once_under_v1() -> None:
    """Ensures domain routers are not also mounted at the application root."""
    routes = [route for route in app.routes if isinstance(route, APIRoute)]
    route_methods = [
        (route.path, method)
        for route in routes
        for method in route.methods or ()
    ]

    assert all(
        path.startswith("/api/v1/") or path in {"/healthz", "/readyz"}
        for path, _ in route_methods
    )
    assert all(count == 1 for count in Counter(route_methods).values())


def test_openapi_operation_ids_are_unique() -> None:
    """Ensures the published OpenAPI schema has no duplicate operations."""
    operation_ids = [
        operation["operationId"]
        for path_item in app.openapi()["paths"].values()
        for operation in path_item.values()
        if isinstance(operation, dict) and "operationId" in operation
    ]

    assert len(operation_ids) == len(set(operation_ids))
