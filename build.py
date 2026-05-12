"""
build.py — Creates a standalone binary for latzero-server.

Usage:
    python build.py              # one-file binary, no console window
    python build.py --console    # keep console window (useful for TUI)
    python build.py --onedir     # output a folder instead of single exe
    python build.py --clean      # delete build artifacts first

Output: dist/latzero-server.exe  (Windows)
         dist/latzero-server      (Linux / macOS)
"""

import argparse
import os
import platform
import shutil
import subprocess
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

HERE        = Path(__file__).parent.resolve()
ENTRY       = HERE / "entry.py"
LOGO        = HERE / "lat.png"
DIST_DIR    = HERE / "dist"
BUILD_DIR   = HERE / "build"
SPEC_FILE   = HERE / "latzero-server.spec"

BINARY_NAME = "latzero-server"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def run(*cmd, **kw):
    print(f"\n>>> {' '.join(str(c) for c in cmd)}\n")
    subprocess.run(cmd, check=True, **kw)


def make_ico(png_path: Path) -> Path:
    """Convert a PNG to a multi-resolution .ico next to it. Returns the .ico path."""
    from PIL import Image, ImageFilter
    ico_path = png_path.with_suffix(".ico")
    img = Image.open(png_path).convert("RGBA")

    # Work from the largest possible source — upsample if source is small
    MAX_SRC = 512
    if img.width < MAX_SRC or img.height < MAX_SRC:
        scale = MAX_SRC / max(img.width, img.height)
        img = img.resize(
            (int(img.width * scale), int(img.height * scale)),
            Image.LANCZOS,
        )
    sizes = [16, 32, 48, 64, 128, 256]
    frames = []
    for s in sizes:
        # Cover-fit: scale so the shortest side fills the square, then centre-crop.
        # This avoids letterboxing that wastes pixels and causes blur.
        src_w, src_h = img.size
        scale = s / min(src_w, src_h)
        new_w = max(s, int(src_w * scale))
        new_h = max(s, int(src_h * scale))
        resized = img.resize((new_w, new_h), Image.LANCZOS)
        # Centre-crop to exactly s×s
        left = (new_w - s) // 2
        top  = (new_h - s) // 2
        frame = resized.crop((left, top, left + s, top + s))

        # For small frames, apply unsharp mask to recover crisp edges
        if s <= 48:
            frame = frame.filter(ImageFilter.UnsharpMask(radius=0.6, percent=180, threshold=2))

        frames.append(frame)

    frames[0].save(
        ico_path, format="ICO",
        sizes=[(f.width, f.height) for f in frames],
        append_images=frames[1:],
    )
    print(f"[build] Icon written → {ico_path}  ({len(frames)} sizes)")
    return ico_path



def ensure_pyinstaller():
    try:
        import PyInstaller  # noqa: F401
        print(f"[build] PyInstaller already installed.")
    except ImportError:
        print("[build] PyInstaller not found — installing...")
        run(sys.executable, "-m", "pip", "install", "pyinstaller")


def ensure_deps():
    """Make sure runtime deps are present before bundling."""
    deps = ["websockets", "Pillow", "psutil", "prompt_toolkit"]
    for dep in deps:
        try:
            __import__(dep.split("[")[0].replace("-", "_").lower())
        except ImportError:
            print(f"[build] Installing {dep}...")
            run(sys.executable, "-m", "pip", "install", dep)


# ---------------------------------------------------------------------------
# Main build
# ---------------------------------------------------------------------------

def build(console: bool, onedir: bool, clean: bool):
    ensure_deps()
    ensure_pyinstaller()

    import PyInstaller.__main__ as pyi

    if clean:
        for p in (DIST_DIR, BUILD_DIR, SPEC_FILE):
            if p.exists():
                print(f"[build] Removing {p}")
                shutil.rmtree(p) if p.is_dir() else p.unlink()

    if not ENTRY.exists():
        sys.exit(f"[build] ERROR: entry point not found: {ENTRY}")

    ico_path = None
    if not LOGO.exists():
        print(f"[build] WARNING: {LOGO.name} not found — using default PyInstaller icon.")
        icon_args = []
        logo_data = []
    else:
        ico_path = make_ico(LOGO)
        icon_args = ["--icon", str(ico_path)]
        # Also bundle the original PNG so the TUI can load it at runtime
        sep = ";" if platform.system() == "Windows" else ":"
        logo_data = ["--add-data", f"{LOGO}{sep}."]

    # Hidden imports that PyInstaller misses for dynamic imports
    hidden = [
        # prompt_toolkit internals
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
        # websockets
        "websockets",
        "websockets.server",
        "websockets.legacy",
        "websockets.legacy.server",
        # PIL
        "PIL",
        "PIL.Image",
        # psutil
        "psutil",
        # asyncio
        "asyncio",
    ]

    hidden_args = []
    for h in hidden:
        hidden_args += ["--hidden-import", h]

    args = [
        str(ENTRY),
        "--name",       BINARY_NAME,
        "--distpath",   str(DIST_DIR),
        "--workpath",   str(BUILD_DIR),
        "--specpath",   str(HERE),
        "--noconfirm",
    ]

    # Window / console mode
    if console:
        args.append("--console")
    else:
        args.append("--console")   # TUI always needs a console; keeping it even for "release"

    # One-file vs one-dir
    if onedir:
        args.append("--onedir")
    else:
        args.append("--onefile")

    args += logo_data
    args += icon_args
    args += hidden_args

    # Exclude big packages not needed at runtime
    args += [
        "--exclude-module", "tkinter",
        "--exclude-module", "matplotlib",
        "--exclude-module", "numpy",
        "--exclude-module", "scipy",
        "--exclude-module", "pandas",
        "--exclude-module", "IPython",
        "--exclude-module", "jupyter",
        "--exclude-module", "notebook",
    ]

    print(f"\n[build] Starting PyInstaller...\n")
    try:
        pyi.run(args)
    finally:
        # Clean up temp .ico regardless of build outcome
        if ico_path and ico_path.exists():
            ico_path.unlink()
            print(f"[build] Cleaned up temp icon.")

    binary = DIST_DIR / BINARY_NAME
    if platform.system() == "Windows" and not onedir:
        binary = binary.with_suffix(".exe")

    if binary.exists():
        size_mb = binary.stat().st_size / 1024 / 1024
        print(f"\n[build] ✓ Done!  →  {binary}  ({size_mb:.1f} MB)")
    else:
        # onedir puts it one level deeper
        inner = DIST_DIR / BINARY_NAME / (BINARY_NAME + (".exe" if platform.system() == "Windows" else ""))
        if inner.exists():
            size_mb = inner.stat().st_size / 1024 / 1024
            print(f"\n[build] ✓ Done!  →  {inner}  ({size_mb:.1f} MB)")
        else:
            print("\n[build] Build completed (output path may differ — check dist/)")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Build latzero-server binary")
    p.add_argument("--console", action="store_true",
                   help="Keep terminal console window visible")
    p.add_argument("--onedir",  action="store_true",
                   help="Output a folder instead of a single executable")
    p.add_argument("--clean",   action="store_true",
                   help="Delete build/ and dist/ before building")
    a = p.parse_args()

    build(console=a.console, onedir=a.onedir, clean=a.clean)
