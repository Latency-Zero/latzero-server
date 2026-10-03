"""Diagnostic only: report live real pod workers from the current checkout."""

import json
from pathlib import Path

import psutil

remaining = []
for process in psutil.process_iter(["name", "cmdline"]):
    arguments = process.info["cmdline"] or []
    module_child = "latzero_server.pods" in arguments and "--child" in arguments
    frozen_child = "--pod-child" in arguments and any("latzero" in Path(arg).name.lower() for arg in arguments)
    if module_child or frozen_child:
        remaining.append({"pid": process.pid, "name": process.info["name"], "command": arguments})
print(json.dumps({"remaining_pod_workers": remaining}))
if remaining:
    raise SystemExit(1)
