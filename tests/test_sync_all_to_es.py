"""Tests for choosing a complete or explicitly limited Elasticsearch backfill."""

import pytest

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
