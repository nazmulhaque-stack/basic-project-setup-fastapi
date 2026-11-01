"""Tests for seeder CLI and UserSeeder."""

from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from app.database.seeders.__main__ import build_parser, cmd_list, main
from app.database.seeders.registry import get_registered_seeders
from app.database.seeders.runner import load_seeders


def test_seeders_cli_parser():
    parser = build_parser()
    list_args = parser.parse_args(["list"])
    assert list_args.command == "list"

    run_args = parser.parse_args(["run", "user_seeder"])
    assert run_args.command == "run"
    assert run_args.seeder_name == "user_seeder"


def test_seeders_list_command(capsys):
    load_seeders()
    code = cmd_list()
    assert code == 0
    captured = capsys.readouterr()
    assert "category_seeder" in captured.out
    assert "user_seeder" in captured.out


def test_user_seeder_registered():
    load_seeders()
    seeders = get_registered_seeders()
    assert "user_seeder" in seeders
    assert "category_seeder" in seeders
    assert seeders["user_seeder"].dependencies == ("category_seeder",)
