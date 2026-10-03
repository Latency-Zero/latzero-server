import asyncio

import pytest

from latzero_server import cli


def test_pod_count_flag_defaults_to_single_daemon():
    parser = cli.build_parser()
    assert parser.parse_args([]).pods == 1
    assert parser.parse_args(["--pods", "4"]).pods == 4
    assert "--pod-child" not in parser.format_help()


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [0, -1, 65])
async def test_invalid_pod_count_fails_before_opening_storage_or_network(count, monkeypatch):
    args = cli.build_parser().parse_args(["--pods", str(count)])
    monkeypatch.setattr(cli, "LatZeroServer", lambda **kwargs: pytest.fail("No daemon should be constructed"))
    with pytest.raises(ValueError, match="pods"):
        await cli._run(args)


@pytest.mark.asyncio
async def test_multi_pod_tui_is_explicitly_rejected():
    args = cli.build_parser().parse_args(["--pods", "2", "--tui"])
    with pytest.raises(ValueError, match="--tui"):
        await cli._run(args)


@pytest.mark.asyncio
async def test_multi_pod_cli_starts_supervisor_and_cleans_it_on_cancellation(tmp_path, monkeypatch, capsys):
    import latzero_server.pods as pods

    calls = []
    started = asyncio.Event()

    class Supervisor:
        def __init__(self, config, count):
            calls.append(("construct", count, config.data_dir, config.websocket_enabled))
            self.tcp_port = 19999

        async def start(self):
            calls.append(("start",))

        async def serve_forever(self):
            started.set()
            await asyncio.Event().wait()

        async def stop(self):
            calls.append(("stop",))

    monkeypatch.setattr(pods, "PodSupervisor", Supervisor)
    monkeypatch.setattr(cli, "LatZeroServer", lambda **kwargs: pytest.fail("Multi-pod CLI must not create a classic daemon"))
    args = cli.build_parser().parse_args(["--pods", "4", "--port", "0", "--no-ws", "--data-dir", str(tmp_path)])
    task = asyncio.create_task(cli._run(args))
    await asyncio.wait_for(started.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert calls == [("construct", 4, tmp_path, False), ("start",), ("stop",)]
    assert "127.0.0.1:19999" in capsys.readouterr().out


def test_frozen_hidden_child_dispatch_does_not_enter_normal_cli(monkeypatch):
    import latzero_server.pods as pods

    monkeypatch.setattr(pods, "child_main", lambda: 17)
    monkeypatch.setattr(cli, "_run", lambda args: pytest.fail("Child dispatch must not construct the public supervisor"))
    assert cli.main(["--pod-child"]) == 17


@pytest.mark.asyncio
async def test_pod_cli_rejects_remote_bind_before_start(tmp_path):
    args = cli.build_parser().parse_args(["--pods", "2", "--host", "0.0.0.0", "--data-dir", str(tmp_path)])
    with pytest.raises(ValueError, match="127.0.0.1"):
        await cli._run(args)
    assert not (tmp_path / ".latzero.lock").exists()
