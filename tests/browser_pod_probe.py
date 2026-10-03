"""Explicit temporary fixture for the browser SDK's native-WS integration gate."""

import asyncio
import json
import os
from pathlib import Path
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "latzero-server"))

from latzero_server.config import ServerConfig
from latzero_server.pods import PodSupervisor, pool_owner


async def main():
    with tempfile.TemporaryDirectory(prefix="latzero-browser-pods-", dir=r"C:\Users\kstya\AppData\Local\Temp\kilo") as data:
        cluster = PodSupervisor(ServerConfig(port=0, websocket_port=0, data_dir=Path(data)), 4)
        await cluster.start()
        pools = {}
        index = 0
        while len(pools) < 2:
            name = "browser-probe-" + str(index)
            pools.setdefault(pool_owner(name, 4), name)
            index += 1
        names = list(pools.values())
        environment = os.environ.copy()
        environment["LATZERO_WEB_POD_TEST"] = json.dumps({
            "host": "127.0.0.1", "port": cluster.tcp_port, "wsPort": cluster.ws_port,
            "pool": names[0], "switchPool": names[1], "timeout": 10000,
        })
        child = None
        try:
            child = await asyncio.create_subprocess_exec("node", "--unhandled-rejections=strict", "--test",
                str(ROOT / "web-client" / "latzero-client.integration.test.js"), env=environment)
            code = await asyncio.wait_for(child.wait(), 25)
            if code:
                raise RuntimeError("Browser native-WS pod test failed with code " + str(code))
        finally:
            if child and child.returncode is None:
                child.kill()
                await child.wait()
            await asyncio.wait_for(cluster.stop(), 30)


if __name__ == "__main__":
    asyncio.run(main())
