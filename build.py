"""
Create a standalone latzero-server binary using an already prepared environment.

Usage:
    python build.py              # one-file binary with console diagnostics
    python build.py --console    # equivalent to the default
    python build.py --onedir     # output a folder instead of a single executable
    python build.py --clean      # delete only build/ and dist/ first

Output: dist/latzero-server.exe (Windows), dist/latzero-server (Linux / macOS).
"""

import argparse
import importlib.util
import os
import platform
import shutil
import tempfile
from pathlib import Path


HERE = Path(__file__).resolve().parent
ENTRY = HERE / "entry.py"
LOGO = HERE / "lat.png"
DIST_DIR = HERE / "dist"
BUILD_DIR = HERE / "build"
BINARY_NAME = "latzero-server"


def make_ico(png_path: Path, ico_path: Path) -> Path:
    """Convert a PNG to an explicitly owned temporary icon path."""
    from PIL import Image, ImageFilter

    with Image.open(png_path) as source:
        img = source.convert("RGBA")
    resampling = getattr(Image, "Resampling", Image)
    if img.width < 512 or img.height < 512:
        scale = 512 / max(img.width, img.height)
        img = img.resize(
            (int(img.width * scale), int(img.height * scale)), resampling.LANCZOS
        )

    frames = []
    for size in (16, 32, 48, 64, 128, 256):
        scale = size / min(img.size)
        width = max(size, int(img.width * scale))
        height = max(size, int(img.height * scale))
        resized = img.resize((width, height), resampling.LANCZOS)
        left = (width - size) // 2
        top = (height - size) // 2
        frame = resized.crop((left, top, left + size, top + size))
        if size <= 48:
            frame = frame.filter(
                ImageFilter.UnsharpMask(radius=0.6, percent=180, threshold=2)
            )
        frames.append(frame)

    frames[0].save(
        ico_path,
        format="ICO",
        sizes=[(frame.width, frame.height) for frame in frames],
        append_images=frames[1:],
    )
    print(f"[build] Icon written: {ico_path}")
    return ico_path


def ensure_deps():
    """Fail before changing artifacts; never install into the caller's environment."""
    required = {
        "PyInstaller": "PyInstaller",
        "websockets": "websockets",
        "PIL": "Pillow",
        "prompt_toolkit": "prompt_toolkit",
    }
    missing = [
        distribution
        for module, distribution in required.items()
        if importlib.util.find_spec(module) is None
    ]
    if missing:
        raise RuntimeError(
            f"Missing build dependencies: {', '.join(missing)}. "
            'Prepare an isolated environment explicitly with '
            'python -m pip install ".[tui]" pyinstaller before building.'
        )


def build(console: bool, onedir: bool, clean: bool) -> Path:
    ensure_deps()
    import PyInstaller.__main__ as pyi

    if not ENTRY.is_file():
        raise RuntimeError(f"Entry point not found: {ENTRY}")
    if clean:
        for path in (DIST_DIR, BUILD_DIR):
            if path.exists():
                print(f"[build] Removing {path}")
                shutil.rmtree(path)

    BUILD_DIR.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=f"{BINARY_NAME}-", dir=str(BUILD_DIR)) as temporary:
        temporary_path = Path(temporary)
        # Console output is required for both daemon diagnostics and the opt-in TUI.
        args = [
            str(ENTRY),
            "--name", BINARY_NAME,
            "--paths", str(HERE),
            "--distpath", str(DIST_DIR),
            "--workpath", str(temporary_path / "work"),
            "--specpath", str(temporary_path),
            "--noconfirm",
            "--console",
            "--onedir" if onedir else "--onefile",
        ]

        if LOGO.is_file():
            icon = make_ico(LOGO, temporary_path / "latzero-server.ico")
            args += ["--icon", str(icon)]
        else:
            print(f"[build] WARNING: {LOGO.name} not found; using the default icon.")

        for name in ("lat.png", "logo.png", "README.md", "pyproject.toml", "LICENSE", "LICENSE.txt"):
            resource = HERE / name
            if resource.is_file():
                args += ["--add-data", f"{resource}{os.pathsep}."]

        hidden = [
            "latzero_server.tui",
            "prompt_toolkit",
            "prompt_toolkit.application",
            "prompt_toolkit.layout",
            "prompt_toolkit.layout.containers",
            "prompt_toolkit.layout.controls",
            "prompt_toolkit.layout.dimension",
            "prompt_toolkit.layout.processors",
            "prompt_toolkit.widgets",
            "prompt_toolkit.styles",
            "prompt_toolkit.key_binding",
            "prompt_toolkit.formatted_text",
            "prompt_toolkit.output.win32",
            "prompt_toolkit.output.vt100",
            "prompt_toolkit.input.win32",
            "prompt_toolkit.input.vt100",
            "websockets",
            "websockets.legacy.server",
            "PIL",
            "PIL.Image",
            "asyncio",
        ]
        distributions = ["websockets", "prompt_toolkit", "Pillow"]
        if importlib.util.find_spec("psutil") is not None:
            hidden.append("psutil")
            distributions.append("psutil")
        for module in hidden:
            args += ["--hidden-import", module]
        for distribution in distributions:
            args += ["--copy-metadata", distribution]
        for module in ("tkinter", "matplotlib", "numpy", "scipy", "pandas", "IPython", "jupyter", "notebook"):
            args += ["--exclude-module", module]

        print("[build] Starting PyInstaller...")
        pyi.run(args)

    suffix = ".exe" if platform.system() == "Windows" else ""
    binary = DIST_DIR / BINARY_NAME
    if onedir:
        binary /= BINARY_NAME + suffix
    else:
        binary = binary.with_suffix(suffix)
    if not binary.is_file():
        raise RuntimeError(f"PyInstaller did not create the expected executable: {binary}")
    size_mb = binary.stat().st_size / 1024 / 1024
    print(f"[build] Executable: {binary} ({size_mb:.1f} MB)")
    return binary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build latzero-server binary without installing dependencies")
    parser.add_argument("--console", action="store_true", help="Keep console diagnostics (always enabled)")
    parser.add_argument("--onedir", action="store_true", help="Output a folder instead of a single executable")
    parser.add_argument("--clean", action="store_true", help="Delete only build/ and dist/ before building")
    args = parser.parse_args()
    try:
        build(console=args.console, onedir=args.onedir, clean=args.clean)
    except (ImportError, OSError, RuntimeError) as exc:
        parser.exit(1, f"[build] ERROR: {exc}\n")
