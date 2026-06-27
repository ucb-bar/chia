from __future__ import annotations

from contextlib import AsyncExitStack, asynccontextmanager
from typing import TYPE_CHECKING, List

if TYPE_CHECKING:
    from fastapi import FastAPI
    from mcp.server.fastmcp import FastMCP

def make_router_lifespan(mcp_instances: List[FastMCP]):
    """A lifespan which runs all MCP session managers concurrently.

    This is useful when multiple MCP ASGI apps are mounted under one FastAPI
    ingress app (e.g. /gem5, /chia). Mounting should be done outside the
    lifespan; this only manages session manager lifetimes.
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        async with AsyncExitStack() as stack:
            for mcp in mcp_instances:
                await stack.enter_async_context(mcp.session_manager.run())
            yield

    return lifespan