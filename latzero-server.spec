# -*- mode: python ; coding: utf-8 -*-

from pathlib import Path
from PyInstaller.utils.hooks import copy_metadata

root = Path(SPECPATH).resolve()
datas = [
    (str(root / name), '.')
    for name in ('lat.png', 'logo.png', 'README.md', 'pyproject.toml', 'LICENSE', 'LICENSE.txt')
    if (root / name).is_file()
]
for distribution in ('websockets', 'prompt_toolkit', 'Pillow'):
    datas += copy_metadata(distribution)

a = Analysis(
    [str(root / 'entry.py')],
    pathex=[str(root)],
    binaries=[],
    datas=datas,
    hiddenimports=['latzero_server.tui', 'latzero_server.pods', 'latzero_server.directory_lock', 'prompt_toolkit', 'prompt_toolkit.application', 'prompt_toolkit.layout', 'prompt_toolkit.layout.containers', 'prompt_toolkit.layout.controls', 'prompt_toolkit.layout.dimension', 'prompt_toolkit.layout.processors', 'prompt_toolkit.widgets', 'prompt_toolkit.styles', 'prompt_toolkit.key_binding', 'prompt_toolkit.formatted_text', 'prompt_toolkit.output.win32', 'prompt_toolkit.output.vt100', 'prompt_toolkit.input.win32', 'prompt_toolkit.input.vt100', 'websockets', 'websockets.legacy.server', 'PIL', 'PIL.Image', 'asyncio'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=['tkinter', 'matplotlib', 'numpy', 'scipy', 'pandas', 'IPython', 'jupyter', 'notebook'],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='latzero-server',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=str(root / 'lat.png') if (root / 'lat.png').is_file() else None,
)
