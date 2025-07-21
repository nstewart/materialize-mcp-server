"""
Materialize MCP Server

A server that provides static tools for managing and querying Materialize databases
over the Model Context Protocol (MCP). The server exposes tools for listing objects,
managing clusters, executing SQL transactions, and monitoring query performance.

The server supports two transports:

* stdio – lines of JSON over stdin/stdout (handy for local CLIs)
* sse   – server‑sent events suitable for web browsers

Available Tools:

1.  ``list_objects`` - Lists all queryable objects in the database
2.  ``list_clusters`` - Lists all clusters in the Materialize instance
3.  ``create_cluster`` - Creates a new cluster with specified name and size
4.  ``run_sql_transaction`` - Executes SQL statements within a transaction
5.  ``list_slow_queries`` - Lists queries with execution time above threshold
6.  ``list_schemas`` - Lists schemas, optionally filtered by database name
7.  ``list_indexes`` - Lists indexes, optionally filtered by schema and/or cluster
8.  ``create_index`` - Creates a default index on a source, view, or materialized view
9.  ``drop_index`` - Drops an index with optional CASCADE support
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
        # Only expose static tools
        tools = []
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
        # Add the list_slow_queries tool
        list_slow_queries_tool = Tool(
            name="list_slow_queries",
            description="List slow queries from recent activity log with execution time above the given threshold (ms).",
            inputSchema={
                "type": "object",
                "properties": {
                    "threshold_ms": {
                        "type": "integer",
                        "description": "Minimum execution time in milliseconds to consider a query slow"
                    }
                },
                "required": ["threshold_ms"]
            }
        )
        tools.append(list_slow_queries_tool)
        # Add the list_schemas tool
        list_schemas_tool = Tool(
            name="list_schemas",
            description="List schemas in Materialize, optionally filtered by database name.",
            inputSchema={
                "type": "object",
                "properties": {
                    "database": {
                        "type": "string",
                        "description": "Optional database name to filter schemas"
                    }
                },
                "required": []
            }
        )
        tools.append(list_schemas_tool)
        # Add the list_indexes tool
        list_indexes_tool = Tool(
            name="list_indexes",
            description="List indexes in Materialize, optionally filtered by schema and/or cluster.",
            inputSchema={
                "type": "object",
                "properties": {
                    "schema": {
                        "type": "string",
                        "description": "Optional schema name to filter indexes"
                    },
                    "cluster": {
                        "type": "string",
                        "description": "Optional cluster name to filter indexes"
                    }
                },
                "required": []
            }
        )
        tools.append(list_indexes_tool)
        # Add the create_index tool
        create_index_tool = Tool(
            name="create_index",
            description="Create a default index on a source, view, or materialized view in a specified cluster.",
            inputSchema={
                "type": "object",
                "properties": {
                    "index_name": {
                        "type": "string",
                        "description": "Name of the index to create"
                    },
                    "cluster_name": {
                        "type": "string",
                        "description": "Name of the cluster to maintain this index"
                    },
                    "object_name": {
                        "type": "string",
                        "description": "Name of the source, view, or materialized view to index"
                    }
                },
                "required": ["index_name", "cluster_name", "object_name"]
            }
        )
        tools.append(create_index_tool)
        # Add the drop_index tool
        drop_index_tool = Tool(
            name="drop_index",
            description="Drop an index from Materialize with optional CASCADE.",
            inputSchema={
                "type": "object",
                "properties": {
                    "index_name": {
                        "type": "string",
                        "description": "Name of the index to drop"
                    },
                    "cascade": {
                        "type": "boolean",
                        "description": "Whether to use CASCADE option (default: false)"
                    }
                },
                "required": ["index_name"]
            }
        )
        tools.append(drop_index_tool)
        return tools

    @server.call_tool()
    async def call_tool(
        name: str, arguments: Dict[str, Any]
    ) -> Sequence[TextContent | ImageContent | EmbeddedResource]:
        logger.debug(f"Calling tool '{name}' with arguments: {arguments}")
        # Only handle static tools
        if name == "list_objects":
            try:
                objects = await server.request_context.lifespan_context.list_objects()
                result_text = json.dumps(objects, default=json_serial, indent=2)
                logger.debug(f"list_objects executed successfully, found {len(objects)} objects")
                return [TextContent(text=result_text, type="text")]
            except Exception as e:
                logger.error(f"Error executing list_objects: {str(e)}")
                raise
        if name == "list_clusters":
            try:
                clusters = await server.request_context.lifespan_context.list_clusters()
                result_text = json.dumps(clusters, default=json_serial, indent=2)
                logger.debug(f"list_clusters executed successfully, found {len(clusters)} clusters")
                return [TextContent(text=result_text, type="text")]
            except Exception as e:
                logger.error(f"Error executing list_clusters: {str(e)}")
                raise
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
        if name == "list_slow_queries":
            try:
                threshold_ms = arguments.get("threshold_ms")
                if threshold_ms is None:
                    raise ValueError("threshold_ms is required")
                result = await server.request_context.lifespan_context.list_slow_queries(threshold_ms)
                result_text = json.dumps(result, default=json_serial, indent=2)
                logger.debug(f"list_slow_queries executed successfully, found {len(result)} slow queries")
                return [TextContent(text=result_text, type="text")]
            except Exception as e:
                logger.error(f"Error executing list_slow_queries: {str(e)}")
                raise
        if name == "list_schemas":
            try:
                database = arguments.get("database")
                result = await server.request_context.lifespan_context.list_schemas(database)
                result_text = json.dumps(result, default=json_serial, indent=2)
                logger.debug(f"list_schemas executed successfully, found {len(result)} schemas")
                return [TextContent(text=result_text, type="text")]
            except Exception as e:
                logger.error(f"Error executing list_schemas: {str(e)}")
                raise
        if name == "list_indexes":
            try:
                schema = arguments.get("schema")
                cluster = arguments.get("cluster")
                result = await server.request_context.lifespan_context.show_indexes(schema, cluster)
                result_text = json.dumps(result, default=json_serial, indent=2)
                logger.debug(f"list_indexes executed successfully, found {len(result)} indexes")
                return [TextContent(text=result_text, type="text")]
            except Exception as e:
                logger.error(f"Error executing list_indexes: {str(e)}")
                raise
        if name == "create_index":
            try:
                index_name = arguments.get("index_name")
                cluster_name = arguments.get("cluster_name")
                object_name = arguments.get("object_name")
                if not index_name or not cluster_name or not object_name:
                    raise ValueError("index_name, cluster_name, and object_name are required")
                result = await server.request_context.lifespan_context.create_index(index_name, cluster_name, object_name)
                result_text = json.dumps(result, default=json_serial, indent=2)
                logger.debug(f"create_index executed successfully: {result['message']}")
                return [TextContent(text=result_text, type="text")]
            except Exception as e:
                logger.error(f"Error executing create_index: {str(e)}")
                raise
        if name == "drop_index":
            try:
                index_name = arguments.get("index_name")
                cascade = arguments.get("cascade", False)
                if not index_name:
                    raise ValueError("index_name is required")
                result = await server.request_context.lifespan_context.drop_index(index_name, cascade)
                result_text = json.dumps(result, default=json_serial, indent=2)
                logger.debug(f"drop_index executed successfully: {result['message']}")
                return [TextContent(text=result_text, type="text")]
            except Exception as e:
                logger.error(f"Error executing drop_index: {str(e)}")
                raise
        # If not a static tool, raise error
        logger.error(f"Tool not found: {name}")
        raise MissingTool(f"Tool not found: {name}")

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
