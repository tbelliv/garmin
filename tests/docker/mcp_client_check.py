"""Connect to an MCP server command over stdio, as Claude Code does, and call tools.

    python tests/docker/mcp_client_check.py -- docker compose ... run --rm -T --no-deps garmin-mcp
"""

import asyncio
import json
import os
import sys

from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client


async def main(cmd: list[str]) -> int:
    params = StdioServerParameters(command=cmd[0], args=cmd[1:], env=dict(os.environ))
    async with stdio_client(params) as (read, write), ClientSession(read, write) as session:
        await session.initialize()
        tools = sorted(t.name for t in (await session.list_tools()).tools)
        print("tools:", ", ".join(tools))
        checks = {
            "list_activities": {"limit": 10},
            "data_coverage": {},
            "get_training_metrics": {},
            "search_activities": {"source": "coros"},
        }
        results = {}
        for name, args in checks.items():
            res = await session.call_tool(name, args)
            results[name] = json.loads(res.content[0].text)
        acts = results["list_activities"]["activities"]
        key = acts[0]["activity_key"]
        stream = json.loads((await session.call_tool(
            "get_activity_stream", {"activity_key": key, "max_points": 10})).content[0].text)
        cov = results["data_coverage"]["report"]
        print("activities:", results["list_activities"]["count"],
              "| coros:", results["search_activities"]["count"],
              "| stream rows:", len(stream["rows"]),
              "| backfill_complete:", cov["backfill_complete"])
        assert len(tools) == 13
        assert results["list_activities"]["count"] >= 4
        assert results["search_activities"]["count"] == 1
        assert stream["rows"]
        assert cov["backfill_complete"] is True, cov["blocking"]
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main(sys.argv[sys.argv.index("--") + 1:])))
