"""
Standalone entry point for the PyInstaller binary.
Uses absolute imports so PyInstaller can resolve the package correctly.
"""
from latzero_server.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
