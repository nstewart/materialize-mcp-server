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
import json
import logging
import sys
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Sequence, AsyncIterator

import uvicorn
from mcp import stdio_server
from mcp.server.sse import SseServerTransport
from psycopg.rows import dict_row

from .mz_client import MzClient, MissingTool, json_serial
from .config import load_config
from mcp.server import Server, NotificationOptions
from mcp.types import Tool, TextContent, ImageContent, EmbeddedResource
from psycopg_pool import AsyncConnectionPool


logger = logging.getLogger("mz_mcp_server")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stderr,
)


def get_lifespan(cfg):
    @asynccontextmanager
    async def lifespan(server) -> AsyncIterator[MzClient]:
        logger.info(
            f"Initializing connection pool with min_size={cfg.pool_min_size}, max_size={cfg.pool_max_size}"
        )
        logger.info(f"Connecting to Materialize at: {cfg.dsn}")
        print(f"DEBUG: Starting lifespan function", file=sys.stderr)

        async def configure(conn):
            await conn.set_autocommit(True)
            logger.debug("Configured new database connection")

        try:
            logger.debug("Creating connection pool...")
            print(f"DEBUG: About to create connection pool", file=sys.stderr)
            async with AsyncConnectionPool(
                conninfo=cfg.dsn,
                min_size=cfg.pool_min_size,
                max_size=cfg.pool_max_size,
                kwargs={"application_name": "materialize_mcp_server"},
                configure=configure,
            ) as pool:
                try:
                    logger.debug("Testing database connection...")
                    async with pool.connection() as conn:
                        await conn.set_autocommit(True)
                        async with conn.cursor(row_factory=dict_row) as cur:
                            await cur.execute(
                                "SELECT mz_environment_id() AS env, current_role AS role;"
                            )
                            meta = await cur.fetchone()
                            logger.info(
                                f"Successfully connected to Materialize environment {meta['env']} as user {meta['role']}"
                            )
                    logger.debug("Connection pool initialized successfully")
                    yield MzClient(pool=pool)
                except Exception as e:
                    logger.error(f"Failed to initialize connection pool: {str(e)}")
                    import traceback
                    logger.error(f"Connection error traceback: {traceback.format_exc()}")
                    raise
                finally:
                    logger.info("Closing connection pool...")
                    await pool.close()
        except Exception as e:
            logger.error(f"Failed to create connection pool: {str(e)}")
            import traceback
            logger.error(f"Pool creation error traceback: {traceback.format_exc()}")
            raise

    return lifespan


async def run():
    print("DEBUG: Starting run function", file=sys.stderr)
    cfg = load_config()
    print(f"DEBUG: Config loaded: transport={cfg.transport}, dsn={cfg.dsn}", file=sys.stderr)
    server = Server("materialize_mcp_server", lifespan=get_lifespan(cfg))
    print("DEBUG: Server created", file=sys.stderr)

    @server.list_tools()
    async def list_tools() -> List[Tool]:
        logger.debug("Listing available tools...")
        tools = await server.request_context.lifespan_context.list_tools()
        
        # Add the list_objects tool
        list_objects_tool = Tool(
            name="list_objects",
            description="List all queryable objects in the Materialize database including sources, tables, views, materialized views, and indexed views",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        )
        tools.append(list_objects_tool)
        
        # Add the list_clusters tool
        list_clusters_tool = Tool(
            name="list_clusters",
            description="List all clusters in the Materialize instance.",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        )
        tools.append(list_clusters_tool)
        
        # Add the create_cluster tool
        create_cluster_tool = Tool(
            name="create_cluster",
            description="Create a new cluster in Materialize with the specified name and size.",
            inputSchema={
                "type": "object",
                "properties": {
                    "cluster_name": {
                        "type": "string",
                        "description": "Name of the cluster to create"
                    },
                    "size": {
                        "type": "string", 
                        "description": "Size specification (e.g., '100cc', '400cc', 'medium', etc.)"
                    }
                },
                "required": ["cluster_name", "size"]
            }
        )
        tools.append(create_cluster_tool)
        
        # Add the run_sql_transaction tool
        run_sql_transaction_tool = Tool(
            name="run_sql_transaction",
            description="Execute one or more SQL statements within a single transaction on a specified cluster.",
            inputSchema={
                "type": "object",
                "properties": {
                    "cluster_name": {
                        "type": "string",
                        "description": "Name of the cluster to execute the transaction on"
                    },
                    "sql_statements": {
                        "type": "array",
                        "items": {
                            "type": "string"
                        },
                        "description": "List of SQL statements to execute within the transaction"
                    },
                    "isolation_level": {
                        "type": "string",
                        "description": "Optional isolation level to set (e.g., 'strict serializable', 'serializable', etc.)"
                    }
                },
                "required": ["cluster_name", "sql_statements"]
            }
        )
        tools.append(run_sql_transaction_tool)
        
        return tools

    @server.call_tool()
    async def call_tool(
        name: str, arguments: Dict[str, Any]
    ) -> Sequence[TextContent | ImageContent | EmbeddedResource]:
        logger.debug(f"Calling tool '{name}' with arguments: {arguments}")
        
        # Handle the list_objects tool
        if name == "list_objects":
            try:
                objects = await server.request_context.lifespan_context.list_objects()
                result_text = json.dumps(objects, default=json_serial, indent=2)
                logger.debug(f"list_objects executed successfully, found {len(objects)} objects")
                return [TextContent(text=result_text, type="text")]
            except Exception as e:
                logger.error(f"Error executing list_objects: {str(e)}")
                raise
        
        # Handle the list_clusters tool
        if name == "list_clusters":
            try:
                clusters = await server.request_context.lifespan_context.list_clusters()
                result_text = json.dumps(clusters, default=json_serial, indent=2)
                logger.debug(f"list_clusters executed successfully, found {len(clusters)} clusters")
                return [TextContent(text=result_text, type="text")]
            except Exception as e:
                logger.error(f"Error executing list_clusters: {str(e)}")
                raise
        
        # Handle the create_cluster tool
        if name == "create_cluster":
            try:
                cluster_name = arguments.get("cluster_name")
                size = arguments.get("size")
                if not cluster_name or not size:
                    raise ValueError("Both cluster_name and size are required")
                
                result = await server.request_context.lifespan_context.create_cluster(cluster_name, size)
                result_text = json.dumps(result, default=json_serial, indent=2)
                logger.debug(f"create_cluster executed successfully: {result['message']}")
                return [TextContent(text=result_text, type="text")]
            except Exception as e:
                logger.error(f"Error executing create_cluster: {str(e)}")
                raise
        
        # Handle the run_sql_transaction tool
        if name == "run_sql_transaction":
            try:
                cluster_name = arguments.get("cluster_name")
                sql_statements = arguments.get("sql_statements")
                isolation_level = arguments.get("isolation_level")
                
                if not cluster_name or not sql_statements:
                    raise ValueError("Both cluster_name and sql_statements are required")
                
                if not isinstance(sql_statements, list):
                    raise ValueError("sql_statements must be a list of strings")
                
                result = await server.request_context.lifespan_context.run_sql_transaction(
                    cluster_name, sql_statements, isolation_level
                )
                result_text = json.dumps(result, default=json_serial, indent=2)
                logger.debug(f"run_sql_transaction executed successfully: {result['message']}")
                return [TextContent(text=result_text, type="text")]
            except Exception as e:
                logger.error(f"Error executing run_sql_transaction: {str(e)}")
                raise
        
        # Handle regular indexed view tools
        try:
            result = await server.request_context.lifespan_context.call_tool(
                name, arguments
            )
            logger.debug(f"Tool '{name}' executed successfully")
            return result
        except MissingTool:
            logger.error(f"Tool not found: {name}")
            await server.request_context.session.send_tool_list_changed()
            raise
        except Exception as e:
            logger.error(f"Error executing tool '{name}': {str(e)}")
            raise

    options = server.create_initialization_options(
        notification_options=NotificationOptions(tools_changed=True)
    )
    if cfg.transport == "stdio":
        logger.info("Starting server in stdio mode...")
        logger.info(f"Server initialization options: {options}")
        async with stdio_server() as (read_stream, write_stream):
            logger.info("stdio transport established, starting server...")
            try:
                await server.run(
                    read_stream,
                    write_stream,
                    options,
                )
            except Exception as e:
                logger.error(f"Error during server.run: {str(e)}")
                import traceback
                logger.error(f"Server run error traceback: {traceback.format_exc()}")
                raise
    elif cfg.transport == "sse":
        logger.info(f"Starting SSE server on {cfg.host}:{cfg.port}...")
        from starlette.applications import Starlette
        from starlette.routing import Mount, Route

        sse = SseServerTransport("/messages/")

        async def handle_sse(request):
            logger.debug(
                f"New SSE connection from {request.client.host if request.client else 'unknown'}"
            )
            try:
                async with sse.connect_sse(
                    request.scope, request.receive, request._send
                ) as streams:
                    await server.run(
                        streams[0],
                        streams[1],
                        options,
                    )
            except Exception as e:
                logger.error(f"Error handling SSE connection: {str(e)}")
                raise

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
    else:
        raise ValueError(f"Unknown transport: {cfg.transport}")


def main():
    """Synchronous wrapper for the async main function."""
    try:
        print("DEBUG: Starting main function", file=sys.stderr)
        logger.info("Starting Materialize MCP Server...")
        print("DEBUG: About to call asyncio.run(run())", file=sys.stderr)
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("Shutting down …")
    except Exception as e:
        logger.error(f"Fatal error in main: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        print(f"DEBUG: Exception in main: {str(e)}", file=sys.stderr)
        print(f"DEBUG: Traceback: {traceback.format_exc()}", file=sys.stderr)
        raise


if __name__ == "__main__":
    main()
