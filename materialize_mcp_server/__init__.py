"""
Materialize MCP Server

A  server that exposes Materialize indexes as "tools" over the Model Context
Protocol (MCP).  Each Materialize index that the connected role is allowed to
`SELECT` from (and whose cluster it can `USAGE`) is surfaced as a tool whose
inputs correspond to the indexed columns and whose output is the remaining
columns of the underlying view.

The server supports two transports:

* stdio – lines of JSON over stdin/stdout (handy for local CLIs)
* sse   – server‑sent events suitable for web browsers

---------------

1.  ``list_tools`` executes a catalog query to derive the list of exposable
    indexes; the result is translated into MCP ``Tool`` objects.
2.  ``call_tool`` validates the requested tool, switches the session to the
    appropriate cluster, executes a parameterised ``SELECT`` against the
    indexed view, and returns the first matching row (minus any columns whose
    values were supplied as inputs).
"""

import asyncio
from dataclasses import dataclass
import logging
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Sequence, AsyncIterator

from .mz_client import MzClient
from .config import load_config, Config
from mcp.server import FastMCP
from mcp.types import Tool, TextContent, ImageContent, EmbeddedResource
from psycopg_pool import AsyncConnectionPool


logger = logging.getLogger("mz_mcp_server")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


@dataclass
class AppContext:
    mz_client: MzClient


def get_lifespan(cfg):
    @asynccontextmanager
    async def lifespan(server) -> AsyncIterator[MzClient]:
        logger.info("Initializing database connection pool for dsn {}".format(cfg.dsn))

        async def configure(conn):
            await conn.set_autocommit(True)

        async with AsyncConnectionPool(
            conninfo=cfg.dsn,
            min_size=cfg.pool_min_size,
            max_size=cfg.pool_max_size,
            kwargs={"application_name": "materialize_mcp_server"},
            configure=configure,
        ) as pool:
            try:
                yield MzClient(pool=pool)
            finally:
                await pool.close()

    return lifespan


class MaterializeMCP(FastMCP):
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        logger.setLevel(cfg.log_level)
        super().__init__(
            name="Materialize MCP Server",
            lifespan=get_lifespan(cfg),
            host=cfg.host,
            port=cfg.port,
            log_level=cfg.log_level,
        )

    async def list_tools(self) -> List[Tool]:
        return (
            self.get_context().request_context.lifespan_context.mz_client.list_tools()
        )

    async def call_tool(
        self, name: str, arguments: Dict[str, Any]
    ) -> Sequence[TextContent | ImageContent | EmbeddedResource]:
        logger.debug(f"Calling tool {name} with args {arguments}")
        return self.get_context().request_context.lifespan_context.mz_client.call_tool(
            name, arguments
        )


async def run():
    cfg = load_config()
    server = MaterializeMCP(cfg)

    match cfg.transport:
        case "stdio":
            await server.run_stdio_async()
        case "sse":
            await server.run_sse_async()
        case t:
            raise ValueError(f"Unknown transport: {t}")


def main():
    """Synchronous wrapper for the async main function."""
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("Shutting down …")


if __name__ == "__main__":
    run()
