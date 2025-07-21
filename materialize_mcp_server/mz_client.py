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


# REMOVE TOOL_QUERY and all dynamic tool logic
# Remove TOOL_QUERY definition
# Remove list_tools method
# Remove call_tool method

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

    async def list_clusters(self) -> List[Dict[str, Any]]:
        """
        Return the list of clusters in Materialize.
        """
        pool = self.pool
        clusters: List[Dict[str, Any]] = []
        async with pool.connection() as conn:
            await conn.set_autocommit(True)
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("SHOW CLUSTERS")
                async for row in cur:
                    clusters.append(row)
        return clusters

    async def create_cluster(self, cluster_name: str, size: str) -> Dict[str, Any]:
        """
        Create a new cluster in Materialize with the specified name and size.
        
        Args:
            cluster_name: Name of the cluster to create
            size: Size specification (e.g., '100cc', '400cc', 'medium', etc.)
            
        Returns:
            Dictionary with the result of the cluster creation
        """
        pool = self.pool
        async with pool.connection() as conn:
            await conn.set_autocommit(True)
            async with conn.cursor(row_factory=dict_row) as cur:
                # Execute CREATE CLUSTER statement
                create_sql = f"CREATE CLUSTER {cluster_name} (SIZE = '{size}')"
                await cur.execute(create_sql)
                
                # Return success result
                return {
                    "status": "success",
                    "message": f"Cluster '{cluster_name}' created successfully with size '{size}'",
                    "cluster_name": cluster_name,
                    "size": size
                }

    async def run_sql_transaction(
        self, 
        cluster_name: str, 
        sql_statements: List[str], 
        isolation_level: str = None
    ) -> Dict[str, Any]:
        """
        Execute one or more SQL statements within a single transaction.
        
        Args:
            cluster_name: Name of the cluster to execute the transaction on
            sql_statements: List of SQL statements to execute
            isolation_level: Optional isolation level to set (e.g., 'strict serializable', 'serializable', etc.)
            
        Returns:
            Dictionary with the results of the transaction execution
        """
        pool = self.pool
        results = []
        
        async with pool.connection() as conn:
            await conn.set_autocommit(False)  # Start transaction mode
            try:
                async with conn.cursor(row_factory=dict_row) as cur:
                    # Set the cluster
                    await cur.execute(f"SET cluster = '{cluster_name}'")
                    
                    # Set isolation level if provided
                    if isolation_level:
                        await cur.execute(f"SET transaction_isolation = '{isolation_level}'")
                    
                    # Execute each SQL statement
                    for i, sql in enumerate(sql_statements):
                        if not sql.strip():
                            continue
                            
                        await cur.execute(sql)
                        
                        # Try to fetch results if it's a SELECT statement
                        try:
                            rows = await cur.fetchall()
                            columns = [desc.name for desc in cur.description] if cur.description else []
                            
                            statement_result = {
                                "statement_index": i,
                                "sql": sql,
                                "row_count": len(rows),
                                "columns": columns,
                                "rows": rows if rows else []
                            }
                        except Exception:
                            # Not a SELECT statement or no results
                            statement_result = {
                                "statement_index": i,
                                "sql": sql,
                                "row_count": 0,
                                "columns": [],
                                "rows": []
                            }
                        
                        results.append(statement_result)
                    
                    # Commit the transaction
                    await conn.commit()
                    
                    return {
                        "status": "success",
                        "message": f"Transaction executed successfully on cluster '{cluster_name}'",
                        "cluster_name": cluster_name,
                        "isolation_level": isolation_level,
                        "statements_executed": len(sql_statements),
                        "results": results
                    }
                    
            except Exception as e:
                # Rollback on error
                await conn.rollback()
                raise e
            finally:
                await conn.set_autocommit(True)  # Reset to autocommit mode

    async def list_slow_queries(self, threshold_ms: int) -> list[dict]:
        """
        List slow queries from mz_internal.mz_recent_activity_log with execution_time_ms above the threshold.
        Args:
            threshold_ms: Minimum execution time in milliseconds to consider a query slow
        Returns:
            List of slow query records
        """
        pool = self.pool
        slow_queries = []
        async with pool.connection() as conn:
            await conn.set_autocommit(True)
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    SELECT 
                        sql_text,
                        execution_time_ms,
                        began_at,
                        finished_at,
                        cluster_name
                    FROM mz_internal.mz_recent_activity_log 
                    WHERE finished_at IS NOT NULL 
                        AND execution_time_ms > %s
                    ORDER BY execution_time_ms DESC
                    LIMIT 20;
                    """,
                    (threshold_ms,)
                )
                async for row in cur:
                    slow_queries.append(row)
        return slow_queries

    async def show_indexes(self, schema: str = None, cluster: str = None) -> list[dict]:
        """
        Show indexes, optionally filtered by schema and/or cluster.
        Args:
            schema: Optional schema name to filter indexes
            cluster: Optional cluster name to filter indexes
        Returns:
            List of index records
        """
        pool = self.pool
        indexes = []
        async with pool.connection() as conn:
            await conn.set_autocommit(True)
            async with conn.cursor(row_factory=dict_row) as cur:
                sql_parts = ["SHOW INDEXES"]
                if schema:
                    sql_parts.append(f"FROM {schema}")
                if cluster:
                    sql_parts.append(f"IN CLUSTER {cluster}")
                sql_stmt = " ".join(sql_parts)
                await cur.execute(sql_stmt)
                async for row in cur:
                    indexes.append(row)
        return indexes

    async def list_schemas(self, database: str = None) -> list[dict]:
        """
        List schemas, optionally filtered by database name.
        Args:
            database: Optional database name to filter schemas
        Returns:
            List of schema records
        """
        pool = self.pool
        schemas = []
        async with pool.connection() as conn:
            await conn.set_autocommit(True)
            async with conn.cursor(row_factory=dict_row) as cur:
                if database:
                    sql_stmt = f"SHOW SCHEMAS FROM {database}"
                else:
                    sql_stmt = "SHOW SCHEMAS"
                await cur.execute(sql_stmt)
                async for row in cur:
                    schemas.append(row)
        return schemas


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
