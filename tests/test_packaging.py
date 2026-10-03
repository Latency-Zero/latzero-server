"""Startup and packaging regressions without network listeners or build installs."""

import ast
import asyncio
import builtins
import importlib.util
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import textwrap
from types import ModuleType, SimpleNamespace

import pytest

from latzero_server import cli


ROOT = Path(__file__).resolve().parents[1]


def run_isolated_python(source, tmp_path):
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT)
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(source)],
        cwd=str(tmp_path),
        env=env,
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_base_and_cli_import_without_tui_dependencies(tmp_path):
    run_isolated_python(
        """
        import builtins
        import sys

        original_import = builtins.__import__
        def without_tui(name, globals=None, locals=None, fromlist=(), level=0):
            if name.split('.')[0] in ('prompt_toolkit', 'PIL'):
                raise AssertionError('Optional TUI dependency was imported: ' + name)
            return original_import(name, globals, locals, fromlist, level)
        builtins.__import__ = without_tui

        import latzero_server
        from latzero_server import LatZeroServer, ServerConfig
        import latzero_server.cli
        assert 'latzero_server.tui' not in sys.modules
        assert 'ServerDashboard' in latzero_server.__all__
        assert latzero_server.cli.build_parser().parse_args([]).tui is False
        try:
            latzero_server.unknown_attribute
        except AttributeError:
            pass
        else:
            raise AssertionError('Unknown package attribute must raise AttributeError')
        """,
        tmp_path,
    )


def test_public_dashboard_export_preserves_real_class(tmp_path):
    if importlib.util.find_spec("prompt_toolkit") is None:
        pytest.skip("Optional TUI dependency is not installed")
    run_isolated_python(
        """
        import sys
        import latzero_server
        assert 'latzero_server.tui' not in sys.modules
        from latzero_server import ServerDashboard
        from latzero_server.tui import ServerDashboard as implementation
        assert ServerDashboard is implementation
        assert latzero_server.ServerDashboard is implementation
        """,
        tmp_path,
    )


@pytest.fixture
def daemon(monkeypatch):
    state = SimpleNamespace(events=[], servers=[])

    class Server:
        def __init__(self, config):
            self.config = config
            state.servers.append(self)

        async def start(self):
            state.events.append("start")

        async def serve_forever(self):
            state.events.append("serve_forever")

        async def stop(self):
            state.events.append("stop")

    monkeypatch.setattr(cli, "LatZeroServer", Server)
    return state


@pytest.mark.parametrize("mode", [[], ["--headless"]])
def test_default_and_headless_run_daemon_with_windows_diagnostics(mode, daemon, monkeypatch, capsys, tmp_path):
    def cannot_detach():
        raise AssertionError("Headless mode must not detach the Windows console")

    monkeypatch.setitem(
        sys.modules,
        "ctypes",
        SimpleNamespace(windll=SimpleNamespace(kernel32=SimpleNamespace(FreeConsole=cannot_detach))),
    )
    monkeypatch.setattr(sys, "platform", "win32")
    stdout, stderr = sys.stdout, sys.stderr
    args = cli.build_parser().parse_args(mode + ["--data-dir", str(tmp_path)])
    asyncio.run(cli._run(args))
    assert daemon.events == ["start", "serve_forever", "stop"]
    assert sys.stdout is stdout
    assert sys.stderr is stderr
    output = capsys.readouterr()
    assert "127.0.0.1:14130" in output.out
    assert str(tmp_path) in output.out


def test_tui_flag_selects_dashboard_and_stops_server(daemon, monkeypatch, tmp_path):
    module = ModuleType("latzero_server.tui")

    class Dashboard:
        def __init__(self, server):
            assert server is daemon.servers[0]
            daemon.events.append("dashboard")

        async def run(self):
            daemon.events.append("tui")

    module.ServerDashboard = Dashboard
    monkeypatch.setitem(sys.modules, module.__name__, module)
    assert cli.main(["--tui", "--data-dir", str(tmp_path)]) == 0
    assert daemon.events == ["start", "dashboard", "tui", "stop"]


@pytest.mark.parametrize("missing", ["prompt_toolkit.application", "unrelated_dependency"])
def test_failed_tui_import_reports_cause_before_start(missing, daemon, monkeypatch, capsys, tmp_path):
    original_import = builtins.__import__

    def without_dashboard(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "tui" and level == 1:
            raise ModuleNotFoundError("No module named " + missing, name=missing)
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", without_dashboard)
    assert cli.main(["--tui", "--data-dir", str(tmp_path)]) == 1
    assert not daemon.servers
    error = capsys.readouterr().err
    if missing.startswith("prompt_toolkit"):
        assert 'python -m pip install "latzero-server[tui]"' in error
    else:
        assert missing in error
        assert "TUI extra" not in error


def test_startup_error_reports_diagnostic_and_attempts_cleanup(daemon, monkeypatch, capsys, tmp_path):
    async def fail_start(self):
        daemon.events.append("start")
        raise OSError("address already in use")

    monkeypatch.setattr(cli.LatZeroServer, "start", fail_start)
    assert cli.main(["--headless", "--data-dir", str(tmp_path)]) == 1
    assert daemon.events == ["start", "stop"]
    assert "latzero-server: address already in use" in capsys.readouterr().err


def test_keyboard_interrupt_stops_server_without_traceback(daemon, monkeypatch, capsys, tmp_path):
    async def interrupt(self):
        daemon.events.append("serve_forever")
        raise KeyboardInterrupt

    monkeypatch.setattr(cli.LatZeroServer, "serve_forever", interrupt)
    assert cli.main(["--data-dir", str(tmp_path)]) == 0
    assert daemon.events == ["start", "serve_forever", "stop"]
    assert "Traceback" not in capsys.readouterr().err


def test_tui_and_headless_are_mutually_exclusive():
    with pytest.raises(SystemExit) as error:
        cli.build_parser().parse_args(["--tui", "--headless"])
    assert error.value.code == 2


@pytest.mark.parametrize(
    "options, enabled, port, origins",
    [
        ([], True, None, [None]),
        (["--no-ws"], False, None, [None]),
        (["--ws-port", "15001", "--ws-origin", "http://localhost:8080", "--ws-origin", "null"],
         True, 15001, [None, "http://localhost:8080", "null"]),
    ],
)
def test_websocket_cli_controls_preserve_explicit_origins(options, enabled, port, origins, daemon, tmp_path):
    assert cli.main(options + ["--data-dir", str(tmp_path)]) == 0
    config = daemon.servers[0].config
    assert config.websocket_enabled is enabled
    assert config.websocket_port == port
    assert config.websocket_origins == origins


def load_build_module():
    spec = importlib.util.spec_from_file_location("latzero_build_test", ROOT / "build.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def build_env(monkeypatch, tmp_path):
    module = load_build_module()
    root = tmp_path / "checkout with spaces"
    root.mkdir()
    for name in ("entry.py", "lat.png", "logo.png", "README.md", "pyproject.toml", "lat.ico", "latzero-server.spec"):
        (root / name).write_text("checkout asset: " + name, encoding="utf-8")
    for name, value in {
        "HERE": root,
        "ENTRY": root / "entry.py",
        "LOGO": root / "lat.png",
        "DIST_DIR": root / "dist",
        "BUILD_DIR": root / "build",
    }.items():
        monkeypatch.setattr(module, name, value)
    for folder in (module.BUILD_DIR, module.DIST_DIR):
        folder.mkdir()
        (folder / "old-artifact").write_text("old", encoding="utf-8")

    state = SimpleNamespace(module=module, root=root, calls=[], icons=[], failure=None, create_output=True)
    state.check_dependencies = module.ensure_deps
    monkeypatch.setattr(module, "ensure_deps", lambda: None)
    monkeypatch.setattr(module.platform, "system", lambda: "Windows")
    original_find_spec = importlib.util.find_spec
    monkeypatch.setattr(
        importlib.util,
        "find_spec",
        lambda name: None if name == "psutil" else original_find_spec(name),
    )

    def make_icon(source, target):
        assert source == module.LOGO
        assert target.parent.is_dir()
        assert target.parent != root
        state.icons.append(target)
        target.write_bytes(b"temporary icon")
        return target

    monkeypatch.setattr(module, "make_ico", make_icon)
    package = ModuleType("PyInstaller")
    package.__path__ = []
    entry = ModuleType("PyInstaller.__main__")

    def run(args):
        state.calls.append(list(args))
        spec_dir = Path(args[args.index("--specpath") + 1])
        (spec_dir / "latzero-server.spec").write_text("generated spec", encoding="utf-8")
        work_dir = Path(args[args.index("--workpath") + 1])
        work_dir.mkdir()
        (work_dir / "analysis").write_text("build work", encoding="utf-8")
        if state.failure:
            raise state.failure
        if state.create_output:
            binary = module.DIST_DIR / "latzero-server.exe"
            if "--onedir" in args:
                binary = module.DIST_DIR / "latzero-server" / "latzero-server.exe"
            binary.parent.mkdir(parents=True, exist_ok=True)
            binary.write_bytes(b"built executable")

    entry.run = run
    package.__main__ = entry
    monkeypatch.setitem(sys.modules, package.__name__, package)
    monkeypatch.setitem(sys.modules, entry.__name__, entry)
    return state


@pytest.mark.parametrize("onedir", [False, True])
def test_build_uses_unique_assets_and_preserves_checkout_files(build_env, monkeypatch, tmp_path, onedir):
    state = build_env
    outside = tmp_path / "outside checkout"
    outside.mkdir()
    monkeypatch.chdir(outside)
    first = state.module.build(console=False, onedir=onedir, clean=True)
    second = state.module.build(console=True, onedir=onedir, clean=False)
    assert first == second
    assert first.is_file()
    assert first.name == "latzero-server.exe"
    assert first.parent == (state.module.DIST_DIR / "latzero-server" if onedir else state.module.DIST_DIR)
    assert not (state.module.BUILD_DIR / "old-artifact").exists()
    assert not (state.module.DIST_DIR / "old-artifact").exists()
    assert (state.root / "lat.ico").read_text(encoding="utf-8") == "checkout asset: lat.ico"
    assert (state.root / "latzero-server.spec").read_text(encoding="utf-8") == "checkout asset: latzero-server.spec"
    assert state.icons[0] != state.icons[1]

    for args in state.calls:
        temporary = Path(args[args.index("--specpath") + 1])
        assert temporary.parent == state.module.BUILD_DIR
        assert not temporary.exists()
        assert Path(args[args.index("--workpath") + 1]).parent == temporary
        assert Path(args[args.index("--icon") + 1]).parent == temporary
        assert args[0] == str(state.module.ENTRY)
        assert "--console" in args
        assert "--noconsole" not in args
        resources = [args[index + 1] for index, value in enumerate(args) if value == "--add-data"]
        for name in ("lat.png", "logo.png", "README.md", "pyproject.toml"):
            assert str(state.root / name) + os.pathsep + "." in resources
        assert "latzero_server.tui" in args
        assert "websockets.legacy.server" in args
        assert "--copy-metadata" in args


def test_failed_build_cleans_temporary_assets_only(build_env):
    state = build_env
    state.failure = RuntimeError("simulated bundler failure")
    with pytest.raises(RuntimeError, match="simulated bundler failure"):
        state.module.build(console=False, onedir=False, clean=False)
    temporary = Path(state.calls[0][state.calls[0].index("--specpath") + 1])
    assert not temporary.exists()
    assert not state.icons[0].exists()
    assert (state.root / "lat.ico").is_file()
    assert (state.root / "latzero-server.spec").is_file()
    assert (state.module.BUILD_DIR / "old-artifact").is_file()


def test_missing_build_dependencies_do_not_install_or_clean(build_env, monkeypatch):
    state = build_env
    monkeypatch.setattr(state.module, "ensure_deps", state.check_dependencies)
    monkeypatch.setattr(importlib.util, "find_spec", lambda name: None)

    def cannot_install(*args, **kwargs):
        raise AssertionError("Builds must not invoke a dependency installer")

    monkeypatch.setattr(subprocess, "run", cannot_install)
    with pytest.raises(RuntimeError, match="Missing build dependencies: PyInstaller, websockets, Pillow, prompt_toolkit"):
        state.module.build(console=False, onedir=False, clean=True)
    assert not state.calls
    assert (state.module.BUILD_DIR / "old-artifact").is_file()
    assert (state.module.DIST_DIR / "old-artifact").is_file()
    assert (state.root / "latzero-server.spec").is_file()


def test_missing_entry_is_detected_before_clean(build_env):
    state = build_env
    state.module.ENTRY.unlink()
    with pytest.raises(RuntimeError, match="Entry point not found"):
        state.module.build(console=False, onedir=False, clean=True)
    assert not state.calls
    assert (state.module.BUILD_DIR / "old-artifact").is_file()
    assert (state.module.DIST_DIR / "old-artifact").is_file()


def test_build_without_expected_executable_fails(build_env, capsys):
    state = build_env
    state.create_output = False
    with pytest.raises(RuntimeError, match="did not create the expected executable"):
        state.module.build(console=False, onedir=False, clean=True)
    temporary = Path(state.calls[0][state.calls[0].index("--specpath") + 1])
    assert not temporary.exists()
    assert "[build] Executable:" not in capsys.readouterr().out


def test_checked_in_spec_is_relocatable_and_bundles_resources(build_env, monkeypatch, tmp_path):
    state = build_env
    utils = ModuleType("PyInstaller.utils")
    utils.__path__ = []
    hooks = ModuleType("PyInstaller.utils.hooks")
    metadata = []

    def copy_metadata(distribution):
        metadata.append(distribution)
        return [(distribution + ".dist-info", distribution + ".dist-info")]

    hooks.copy_metadata = copy_metadata
    monkeypatch.setitem(sys.modules, utils.__name__, utils)
    monkeypatch.setitem(sys.modules, hooks.__name__, hooks)
    monkeypatch.chdir(tmp_path)
    options = {}

    def analysis(scripts, **kwargs):
        options["scripts"] = scripts
        options.update(kwargs)
        return SimpleNamespace(pure=[], scripts=scripts, binaries=[], datas=kwargs["datas"])

    def executable(*args, **kwargs):
        options["exe"] = kwargs

    exec(
        compile((ROOT / "latzero-server.spec").read_text(encoding="utf-8"), "latzero-server.spec", "exec"),
        {"SPECPATH": str(state.root), "Analysis": analysis, "PYZ": lambda pure: pure, "EXE": executable},
    )
    assert options["scripts"] == [str(state.root / "entry.py")]
    assert options["pathex"] == [str(state.root)]
    for name in ("lat.png", "logo.png", "README.md", "pyproject.toml"):
        assert (str(state.root / name), ".") in options["datas"]
    assert metadata == ["websockets", "prompt_toolkit", "Pillow"]
    assert options["exe"]["icon"] == str(state.root / "lat.png")
    assert options["exe"]["console"] is True


def test_owned_sources_keep_python_38_syntax_and_tui_extra():
    for name in ("latzero_server/__init__.py", "latzero_server/cli.py", "build.py", "latzero-server.spec"):
        ast.parse((ROOT / name).read_text(encoding="utf-8"), filename=name, feature_version=8)
    project = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert 'requires-python = ">=3.8"' in project
    extra = ast.literal_eval(re.search(r"(?m)^tui\s*=\s*(\[.*\])$", project).group(1))
    assert "prompt_toolkit>=3.0" in extra
    assert "Pillow>=9.0" in extra


def test_runtime_sources_preserve_python_38_grammar_and_executor_api():
    for source in (ROOT / "latzero_server").glob("*.py"):
        contents = source.read_text(encoding="utf-8")
        ast.parse(contents, filename=str(source), feature_version=8)
        assert "asyncio.to_thread(" not in contents


def test_source_distribution_includes_isolated_fixture_helpers_and_resources():
    manifest = (ROOT / "MANIFEST.in").read_text(encoding="utf-8")
    assert "include lat.png logo.png" in manifest
    assert "recursive-include examples *.py" in manifest
    assert "recursive-include tests *.py *.cjs" in manifest


def test_real_dashboard_logo_supports_current_pillow_resampling():
    pytest.importorskip("PIL")
    pytest.importorskip("prompt_toolkit")
    from latzero_server.tui import _build_logo_art

    art = _build_logo_art(str(ROOT / "logo.png"), target_cols=12, target_rows=3)
    assert art is not None
    assert sum(text == "\n" for _, text in art) == 3
    assert len(art) == 39


def test_ignore_patterns_cover_secret_files_without_hiding_examples():
    if shutil.which("git") is None:
        pytest.skip("git is needed to evaluate ignore patterns")
    secrets = [".env", ".env.local", ".env.production", "nested/.env.development", "nested/.env.test.local"]
    examples = [".env.example", "nested/.env.production.example"]
    result = subprocess.run(
        ["git", "check-ignore", "--no-index", "--stdin", "-z"],
        input=("\0".join(secrets + examples) + "\0").encode("utf-8"),
        cwd=str(ROOT),
        capture_output=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.decode("utf-8").rstrip("\0").split("\0") == secrets
