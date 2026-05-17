"""
Command-line entrypoint for latzero-server.
"""

import argparse
import asyncio
from pathlib import Path
import sys

from .config import ServerConfig
from .server import LatZeroServer
from .tui import ServerDashboard


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the local LatZero TCP server")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host")
    parser.add_argument("--port", type=int, default=14130, help="Bind port")
    parser.add_argument(
        "--data-dir",
        default=None,
        help="Directory for persistent pool snapshots",
    )
    parser.add_argument(
        "--tui",
        action="store_true",
        help="Launch the interactive terminal dashboard",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run without TUI (headless mode)",
    )
    return parser


async def _run(args: argparse.Namespace) -> None:
    config = ServerConfig(
        host=args.host,
        port=args.port,
        data_dir=Path(args.data_dir) if args.data_dir else ServerConfig().data_dir,
    )
    server = LatZeroServer(config=config)
    try:
        await server.start()
        
        if args.headless:
            # Hide the console completely if on Windows and running headless
            if sys.platform == "win32":
                import ctypes
                import os
                ctypes.windll.kernel32.FreeConsole()
                sys.stdout = open(os.devnull, 'w')
                sys.stderr = open(os.devnull, 'w')
                
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
    asyncio.run(_run(args))
    return 0
