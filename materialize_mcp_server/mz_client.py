import base64
import decimal
from typing import List, Dict, Any, Sequence
from uuid import UUID

from mcp import Tool
from mcp.types import TextContent, ImageContent, EmbeddedResource
from psycopg import sql
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
import json


TOOL_QUERY = base = sql.SQL(
    """
        WITH tools AS (
            SELECT
                op.database,
                op.schema,
                op.name AS object_name,
                i.name AS index_name,
                c.name AS cluster,
                cts.comment AS description,
                jsonb_build_object(
                    'type', 'object',
                    'required', jsonb_agg(distinct ccol.name) FILTER (WHERE ccol.position = ic.on_position),
                    'properties', jsonb_strip_nulls(jsonb_object_agg(
                        ccol.name,
                        CASE 
                            WHEN ccol.type IN (
                                'uint2', 'uint4','uint8', 'int', 'integer', 'smallint',
                                'double', 'double precision', 'bigint', 'float', 'numeric', 'real'
                            ) THEN jsonb_build_object('type', 'number', 'description', cts_col.comment)
                            WHEN ccol.type = 'boolean' THEN jsonb_build_object('type', 'boolean', 'description', cts_col.comment)
                            WHEN ccol.type = 'bytea' THEN jsonb_build_object(
                                'type', 'string',
                                'description', cts_col.comment,
                                'contentEncoding', 'base64',
                                'contentMediaType', 'application/octet-stream'
                            )
                            WHEN ccol.type = 'date' THEN jsonb_build_object('type', 'string', 'format', 'date', 'description', cts_col.comment)
                            WHEN ccol.type = 'time' THEN jsonb_build_object('type', 'string', 'format', 'time', 'description', cts_col.comment)
                            WHEN ccol.type ilike 'timestamp%%' THEN jsonb_build_object('type', 'string', 'format', 'date-time', 'description', cts_col.comment)
                            WHEN ccol.type = 'jsonb' THEN jsonb_build_object('type', 'object', 'description', cts_col.comment)
                            WHEN ccol.type = 'uuid' THEN jsonb_build_object('type', 'string', 'format', 'uuid', 'description', cts_col.comment)
                            ELSE jsonb_build_object('type', 'string', 'description', cts_col.comment)
                        END
                    ) FILTER (WHERE ccol.position = ic.on_position))
                ) AS input_schema,
                array_agg(distinct ccol.name) FILTER (WHERE ccol.position <> ic.on_position) AS output_columns
            FROM mz_internal.mz_show_my_object_privileges op
            JOIN mz_objects o ON op.name = o.name AND op.object_type = o.type
            JOIN mz_schemas s ON s.name = op.schema AND s.id = o.schema_id
            JOIN mz_databases d ON d.name = op.database AND d.id = s.database_id
            JOIN mz_indexes i ON i.on_id = o.id
            JOIN mz_index_columns ic ON i.id = ic.index_id
            JOIN mz_columns ccol ON ccol.id = o.id
            JOIN mz_clusters c ON c.id = i.cluster_id
            JOIN mz_internal.mz_show_my_cluster_privileges cp ON cp.name = c.name
            JOIN mz_internal.mz_comments cts ON cts.id = o.id AND cts.object_sub_id IS NULL
            LEFT JOIN mz_internal.mz_comments cts_col ON cts_col.id = o.id AND cts_col.object_sub_id = ccol.position
            WHERE op.privilege_type = 'SELECT'
              AND cp.privilege_type = 'USAGE'
            GROUP BY 1,2,3,4,5,6
        )
        SELECT * FROM tools
        """
)

OBJECTS_QUERY = sql.SQL(
    """
    SELECT 
        op.database,
        op.schema,
        op.name AS object_name,
        op.object_type,
        cts.comment AS description,
        CASE 
            WHEN op.object_type = 'source' THEN 'source'
            WHEN op.object_type = 'table' THEN 'table'
            WHEN op.object_type = 'view' THEN 'view'
            WHEN op.object_type = 'materialized-view' THEN 'materialized_view'
            WHEN op.object_type = 'index' THEN 'indexed_view'
            ELSE op.object_type
        END AS object_category
    FROM mz_internal.mz_show_my_object_privileges op
    JOIN mz_objects o ON op.name = o.name AND op.object_type = o.type
    JOIN mz_schemas s ON s.name = op.schema AND s.id = o.schema_id
    JOIN mz_databases d ON d.name = op.database AND d.id = s.database_id
    LEFT JOIN mz_internal.mz_comments cts ON cts.id = o.id AND cts.object_sub_id IS NULL
    WHERE op.privilege_type = 'SELECT'
      AND op.object_type IN ('source', 'table', 'view', 'materialized-view', 'index')
    ORDER BY op.database, op.schema, op.object_type, op.name
    """
)


class MissingTool(Exception):
    def __init__(self, message):
        super().__init__(message)


class MzClient:
    def __init__(self, pool: AsyncConnectionPool) -> None:
        self.pool = pool

    async def list_tools(self) -> List[Tool]:
        """
        Return the catalog of available tools.

        A tool is essentially an indexed view that the current role can query.

        TODO: The server could subscribe to the database catalog
        TODO: and notify the client whenever a new tool is created.
        """
        pool = self.pool
        tools: List[Tool] = []
        async with pool.connection() as conn:
            await conn.set_autocommit(True)
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(TOOL_QUERY)
                async for row in cur:
                    tools.append(
                        Tool(
                            name=row["index_name"],
                            description=row["description"],
                            inputSchema=row["input_schema"],
                        )
                    )
        return tools

    async def list_objects(self) -> List[Dict[str, Any]]:
        """
        Return the catalog of all queryable objects.

        This includes sources, tables, views, materialized views, and indexes.
        """
        pool = self.pool
        objects: List[Dict[str, Any]] = []
        async with pool.connection() as conn:
            await conn.set_autocommit(True)
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(OBJECTS_QUERY)
                async for row in cur:
                    objects.append(row)
        return objects

    async def call_tool(
        self, name: str, arguments: Dict[str, Any]
    ) -> Sequence[TextContent | ImageContent | EmbeddedResource]:
        pool = self.pool
        async with pool.connection() as conn:
            await conn.set_autocommit(True)
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    TOOL_QUERY + sql.SQL("WHERE index_name = %s"), (name,)
                )
                meta = await cur.fetchone()

        if not meta:
            raise MissingTool(f"Tool not found: {name}")

        async with pool.connection() as conn:
            await conn.set_autocommit(True)
            async with conn.cursor() as cur:
                await cur.execute(
                    sql.SQL("SET cluster TO {};").format(
                        sql.Identifier(meta["cluster"])
                    )
                )

                await cur.execute(
                    sql.SQL("SELECT {} FROM {} WHERE {};").format(
                        sql.SQL("count(*) > 0 AS exists")
                        if not meta["output_columns"]
                        else sql.SQL(",").join(
                            sql.Identifier(col) for col in meta["output_columns"]
                        ),
                        sql.Identifier(
                            meta["database"], meta["schema"], meta["object_name"]
                        ),
                        sql.SQL(" AND ").join(
                            [
                                sql.SQL("{} = {}").format(
                                    sql.Identifier(k), sql.Placeholder()
                                )
                                for k in arguments.keys()
                            ]
                        ),
                    ),
                    list(arguments.values()),
                )
                rows = await cur.fetchall()
                columns = [desc.name for desc in cur.description]

                result = [
                    {k: v for k, v in dict(zip(columns, row)).items()} for row in rows
                ]

                if len(result) == 0:
                    return []
                elif len(result) == 1:
                    text = json.dumps(result[0], default=json_serial)
                else:
                    text = json.dumps(result, default=json_serial)

                return [TextContent(text=text, type="text")]


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    from datetime import datetime, date, time, timedelta

    if isinstance(obj, (datetime, date, time)):
        return obj.isoformat()
    elif isinstance(obj, timedelta):
        return obj.total_seconds()
    elif isinstance(obj, bytes):
        return base64.b64encode(obj).decode("ascii")
    elif isinstance(obj, decimal.Decimal):
        return str(obj)
    elif isinstance(obj, UUID):
        return str(obj)
    else:
        raise TypeError("Type %s not serializable. This is a bug." % type(obj))
