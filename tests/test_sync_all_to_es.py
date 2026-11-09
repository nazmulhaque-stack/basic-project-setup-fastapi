"""Tests for choosing a complete or explicitly limited Elasticsearch backfill."""

import pytest

from scripts import sync_all_to_es
from scripts.sync_all_to_es import resolve_sync_target


def test_sync_target_defaults_to_all_database_users() -> None:
    assert resolve_sync_target(total_users=10_000_000) == 10_000_000


def test_sync_target_respects_only_explicit_limit() -> None:
    assert resolve_sync_target(total_users=10_000_000, limit=100_000) == 100_000


def test_sync_target_does_not_exceed_database_total() -> None:
    assert resolve_sync_target(total_users=100_000, limit=200_000) == 100_000


def test_sync_target_rejects_negative_limit() -> None:
    with pytest.raises(ValueError, match="cannot be negative"):
        resolve_sync_target(total_users=10_000_000, limit=-1)


@pytest.mark.asyncio
async def test_sync_connection_disables_asyncpg_statement_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    call_args: dict[str, object] = {}

    class FakeConnection:
        closed = False

        async def close(self) -> None:
            self.closed = True

    fake_connection = FakeConnection()

    async def fake_connect(dsn: str, **kwargs: object) -> FakeConnection:
        call_args["dsn"] = dsn
        call_args.update(kwargs)
        return fake_connection

    monkeypatch.setattr(sync_all_to_es.asyncpg, "connect", fake_connect)

    async with sync_all_to_es.db_connection("postgresql+asyncpg://localhost/app_db") as conn:
        assert conn is fake_connection

    assert call_args == {
        "dsn": "postgresql://localhost/app_db",
        "statement_cache_size": 0,
    }
    assert fake_connection.closed


@pytest.mark.asyncio
async def test_es_request_retries_transient_read_error(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts = 0

    class FakeResponse:
        status_code = 200

    class FakeClient:
        async def request(self, method: str, url: str, **kwargs: object) -> FakeResponse:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise sync_all_to_es.httpx.ReadError("connection closed")
            return FakeResponse()

    async def no_sleep(_: float) -> None:
        return None

    monkeypatch.setattr(sync_all_to_es.asyncio, "sleep", no_sleep)

    response = await sync_all_to_es.request_with_retry(FakeClient(), "POST", "/_bulk", content="ndjson")

    assert response.status_code == 200
    assert attempts == 2
