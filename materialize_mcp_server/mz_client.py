from typing import Optional, Tuple, List, Dict, Any, Sequence

from mcp import Tool
from mcp.types import TextContent, ImageContent, EmbeddedResource
from psycopg import sql
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
import simplejson as json


class MzClient:
    def __init__(self, pool: AsyncConnectionPool) -> None:
        self.pool = pool

    async def list_tools(self) -> List[Tool]:
        """
        Return the catalog of available tools.

        A tool is essentially an indexed view that the current role can query.
        The metadata is discovered via :func:`get_tool_query`.

        TODO: The server could subscribe to the database catalog
        TODO: and notify the client whenever a new tool is created.
        """
        pool = self.pool
        tools: List[Tool] = []
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                query, params = get_tool_query()
                await cur.execute(query, params)
                async for row in cur:
                    tools.append(
                        Tool(
                            name=row["index_name"],
                            description=row.get("description"),
                            inputSchema=row["input_schema"],
                        )
                    )
        return tools

    async def call_tool(
        self, name: str, arguments: Dict[str, Any]
    ) -> Sequence[TextContent | ImageContent | EmbeddedResource]:
        pool = self.pool
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                q_meta, p_meta = get_tool_query(name)
                await cur.execute(q_meta, p_meta)
                meta = await cur.fetchone()

        if not meta:
            raise RuntimeError(f"Tool not found: {name}")

        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    sql.SQL("SET cluster TO {};").format(
                        sql.Identifier(meta["cluster"])
                    )
                )
                await cur.execute(
                    sql.SQL("SELECT * FROM {} WHERE {};").format(
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
                row = await cur.fetchone()
                if not row:
                    return []

                # TODO push projection pushdown into the database
                columns = [desc.name for desc in cur.description]
                result = {
                    k: v
                    for k, v in dict(zip(columns, row)).items()
                    if k not in arguments
                }
                return [
                    TextContent(
                        text=json.dumps(result, default=json_serial), type="text"
                    )
                ]


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    # TODO support all materialize types
    from datetime import datetime, date, time, timedelta
    from psycopg.types.range import Range

    if isinstance(obj, (datetime, date, time)):
        return obj.isoformat()
    elif isinstance(obj, timedelta):
        return obj.total_seconds()
    elif isinstance(obj, Range):
        return {"lower": obj.lower, "upper": obj.upper, "bounds": obj.bounds}
    elif hasattr(obj, "__dict__"):
        return obj.__dict__


def get_tool_query(tool: Optional[str] = None) -> Tuple[sql.Composed | sql.SQL, Tuple]:
    """
    Construct the SQL query that enumerates eligible indexes.

    When *tool* is supplied, the query is narrowed to the given *index_name*.
    The function returns a pair ``(query, params)`` ready to be passed to
    :py:meth:`cursor.execute`.

    The query inspects Materializes system catalog to:

    * find every object the connected role can `SELECT`
    * ensure the role can `USAGE` the cluster that object lives in
    * bundle the indexed columns into a JSON schema representing tool inputs
    """
    base = sql.SQL(
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
                    'required', jsonb_agg(ccol.name),
                    'properties', jsonb_object_agg(
                        ccol.name,
                        jsonb_build_object(
                            'type',
                            CASE
                                WHEN ccol.name IN ('int', 'bigint', 'float', 'numeric') THEN 'number'
                                WHEN ccol.name = 'bool' THEN 'boolean'
                                ELSE 'text'
                            END
                        )
                    )
                ) AS input_schema
            FROM mz_internal.mz_show_my_object_privileges op
            JOIN mz_objects o ON op.name = o.name AND op.object_type = o.type
            JOIN mz_schemas s ON s.name = op.schema AND s.id = o.schema_id
            JOIN mz_databases d ON d.name = op.database AND d.id = s.database_id
            JOIN mz_indexes i ON i.on_id = o.id
            JOIN mz_index_columns ic ON i.id = ic.index_id
            JOIN mz_columns ccol ON ccol.id = o.id AND ccol.position = ic.on_position
            JOIN mz_clusters c ON c.id = i.cluster_id
            JOIN mz_internal.mz_show_my_cluster_privileges cp ON cp.name = c.name
            JOIN mz_internal.mz_comments cts ON cts.id = o.id AND cts.object_sub_id IS NULL
            WHERE op.privilege_type = 'SELECT'
              AND cp.privilege_type = 'USAGE'
            GROUP BY 1,2,3,4,5,6
        )
        SELECT * FROM tools
        WHERE 1 = 1
        """
    )
    if tool is not None:
        base += sql.SQL(" AND index_name = %s")
        return base, (tool,)
    return base, ()
