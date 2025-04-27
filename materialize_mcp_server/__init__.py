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
import logging
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Sequence, AsyncIterator

import uvicorn
from mcp import stdio_server
from mcp.server.sse import SseServerTransport
from psycopg.rows import dict_row

from .mz_client import MzClient
from .config import load_config
from mcp.server import Server
from mcp.types import Tool, TextContent, ImageContent, EmbeddedResource
from psycopg_pool import AsyncConnectionPool


logger = logging.getLogger("mz_mcp_server")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


def get_lifespan(cfg):
    @asynccontextmanager
    async def lifespan(server) -> AsyncIterator[MzClient]:
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
                async with pool.connection() as conn:
                    await conn.set_autocommit(True)
                    async with conn.cursor(row_factory=dict_row) as cur:
                        await cur.execute(
                            "SELECT mz_environment_id() AS env, current_role AS role;"
                        )
                        meta = await cur.fetchone()
                        logger.info(
                            f"starting server for env {meta['env']} with user {meta['role']} ..."
                        )
                yield MzClient(pool=pool)
            finally:
                await pool.close()

    return lifespan


async def run():
    cfg = load_config()
    server = Server("materialize_mcp_server", lifespan=get_lifespan(cfg))

    @server.list_tools()
    async def list_tools() -> List[Tool]:
        return await server.request_context.lifespan_context.list_tools()

    @server.call_tool()
    async def call_tool(
        name: str, arguments: Dict[str, Any]
    ) -> Sequence[TextContent | ImageContent | EmbeddedResource]:
        return await server.request_context.lifespan_context.call_tool(name, arguments)

    match cfg.transport:
        case "stdio":
            async with stdio_server() as (read_stream, write_stream):
                await server.run(
                    read_stream,
                    write_stream,
                    server.create_initialization_options(),
                )
        case "sse":
            from starlette.applications import Starlette
            from starlette.routing import Mount, Route

            sse = SseServerTransport("/messages/")

            async def handle_sse(request):
                async with sse.connect_sse(
                    request.scope, request.receive, request._send
                ) as streams:
                    await server.run(
                        streams[0],
                        streams[1],
                        server.create_initialization_options(),
                    )

            starlette_app = Starlette(
                routes=[
                    Route("/sse", endpoint=handle_sse),
                    Mount("/messages/", app=sse.handle_post_message),
                ],
            )

            config = uvicorn.Config(
                starlette_app,
                host=cfg.host,
                port=cfg.port,
                log_level=cfg.log_level.upper(),
            )
            server = uvicorn.Server(config)
            await server.serve()
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
