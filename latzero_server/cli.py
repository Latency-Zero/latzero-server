"""
Command-line entrypoint for latzero-server.
"""

import argparse
import asyncio
from pathlib import Path
import sys

from .config import ServerConfig
from .server import LatZeroServer


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the local LatZero TCP/WebSocket daemon")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host")
    parser.add_argument("--port", type=int, default=14130, help="Bind port")
    parser.add_argument("--no-ws", action="store_true", help="Disable the WebSocket listener")
    parser.add_argument("--ws-port", type=int, default=None, help="WebSocket port (default: TCP port + 1)")
    parser.add_argument(
        "--ws-origin",
        action="append",
        default=None,
        help="Allow this browser Origin in addition to native clients (repeatable; null permits file pages)",
    )
    parser.add_argument(
        "--data-dir",
        default=None,
        help="Directory for persistent pool snapshots",
    )
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument(
        "--tui",
        action="store_true",
        help="Launch the interactive terminal dashboard",
    )
    modes.add_argument(
        "--headless",
        action="store_true",
        help="Run without TUI (the default; console diagnostics remain visible)",
    )
    # --- Load-handling knobs ---
    parser.add_argument(
        "--max-connections",
        type=int,
        default=None,
        help="Maximum simultaneous connections (default: 5000)",
    )
    parser.add_argument(
        "--min-workers",
        type=int,
        default=None,
        help="Minimum worker pool size (default: 4)",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=None,
        help="Maximum worker pool size (default: 512)",
    )
    return parser


async def _run(args: argparse.Namespace) -> None:
    if args.tui:
        try:
            from .tui import ServerDashboard
        except ModuleNotFoundError as exc:
            if exc.name and exc.name.split(".")[0] == "prompt_toolkit":
                raise ImportError(
                    'The dashboard requires the TUI extra. Install it with '
                    'python -m pip install "latzero-server[tui]".'
                ) from exc
            raise

    defaults = ServerConfig()
    config = ServerConfig(
        host=args.host,
        port=args.port,
        data_dir=Path(args.data_dir) if args.data_dir else defaults.data_dir,
        max_connections=args.max_connections if args.max_connections is not None else defaults.max_connections,
        min_workers=args.min_workers if args.min_workers is not None else defaults.min_workers,
        max_workers=args.max_workers if args.max_workers is not None else defaults.max_workers,
        websocket_enabled=defaults.websocket_enabled and not args.no_ws,
        websocket_port=args.ws_port if args.ws_port is not None else defaults.websocket_port,
        websocket_origins=defaults.websocket_origins + (args.ws_origin or []),
    )
    server = LatZeroServer(config=config)
    try:
        await server.start()

        if not args.tui:
            print(
                f"latzero-server listening on {config.host}:{config.port} "
                f"(data-dir: {config.data_dir})",
                file=sys.stdout,
                flush=True,
            )
            await server.serve_forever()
        else:
            dashboard = ServerDashboard(server)
            await dashboard.run()
    finally:
        await server.stop()


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        asyncio.run(_run(args))
    except KeyboardInterrupt:
        return 0
    except (ImportError, OSError, ValueError) as exc:
        print(f"latzero-server: {exc}", file=sys.stderr, flush=True)
        return 1
    return 0
