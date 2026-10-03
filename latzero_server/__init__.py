"""
latzero_server - Local TCP server for LatZero server mode.
"""

from .config import ServerConfig
from .server import LatZeroServer

__all__ = ["LatZeroServer", "ServerConfig", "ServerDashboard"]


def __getattr__(name):
    if name == "ServerDashboard":
        from .tui import ServerDashboard

        globals()[name] = ServerDashboard
        return ServerDashboard
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
