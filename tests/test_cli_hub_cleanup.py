import json

from typer.testing import CliRunner

from codex_autorunner.cli import app

runner = CliRunner()


def test_hub_cleanup_dry_run_json(hub_root_only) -> None:
    hub_root = hub_root_only
    result = runner.invoke(
        app,
        ["hub", "cleanup", "--path", str(hub_root), "--dry-run"],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output.strip())
    assert payload["status"] == "ok"
    assert payload["dry_run"] is True
    assert "message" in payload
    assert "threads" in payload
    assert "worktrees" in payload
    assert "flow_runs" in payload


def test_hub_cleanup_no_json_text(hub_root_only) -> None:
    hub_root = hub_root_only
    result = runner.invoke(
        app,
        ["hub", "cleanup", "--path", str(hub_root), "--dry-run", "--no-json"],
    )
    assert result.exit_code == 0, result.output
    assert result.output.strip()


def test_hub_cleanup_pretty_json(hub_root_only) -> None:
    hub_root = hub_root_only
    result = runner.invoke(
        app,
        ["hub", "cleanup", "--path", str(hub_root), "--dry-run", "--pretty"],
    )
    assert result.exit_code == 0, result.output
    assert "\n" in result.output
