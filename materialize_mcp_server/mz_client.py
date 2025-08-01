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

from .validation import validate_and_sanitize_args, ValidationError



OBJECTS_QUERY = sql.SQL(
    """
    SELECT 
        d.name AS database,
        s.name AS schema,
        o.name AS object_name,
        o.type AS object_type,
        cts.comment AS description,
        CASE 
            WHEN o.type = 'source' THEN 'source'
            WHEN o.type = 'table' THEN 'table'
            WHEN o.type = 'view' THEN 'view'
            WHEN o.type = 'materialized-view' THEN 'materialized_view'
            WHEN o.type = 'index' THEN 'indexed_view'
            ELSE o.type
        END AS object_category
    FROM mz_objects o
    JOIN mz_schemas s ON s.id = o.schema_id
    JOIN mz_databases d ON d.id = s.database_id
    LEFT JOIN mz_internal.mz_comments cts ON cts.id = o.id AND cts.object_sub_id IS NULL
    WHERE o.type IN ('source', 'table', 'view', 'materialized-view', 'index')
    ORDER BY d.name, s.name, o.type, o.name
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
        # Validate and sanitize inputs
        args = validate_and_sanitize_args("create_cluster", {
            "cluster_name": cluster_name,
            "size": size
        })
        
        pool = self.pool
        async with pool.connection() as conn:
            await conn.set_autocommit(True)
            async with conn.cursor(row_factory=dict_row) as cur:
                # Execute CREATE CLUSTER statement with proper SQL construction
                create_sql = sql.SQL("CREATE CLUSTER {} (SIZE = {})").format(
                    sql.Identifier(cluster_name),  # Use original name for identifier
                    sql.Literal(size)
                )
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
        # Validate and sanitize inputs
        args = validate_and_sanitize_args("run_sql_transaction", {
            "cluster_name": cluster_name,
            "sql_statements": sql_statements,
            "isolation_level": isolation_level
        })
        
        pool = self.pool
        results = []
        
        async with pool.connection() as conn:
            await conn.set_autocommit(False)  # Start transaction mode
            try:
                async with conn.cursor(row_factory=dict_row) as cur:
                    # Set the cluster using proper parameterization
                    await cur.execute(
                        sql.SQL("SET cluster = {}").format(sql.Identifier(cluster_name))
                    )
                    
                    # Set isolation level if provided
                    if isolation_level:
                        await cur.execute(
                            sql.SQL("SET transaction_isolation = {}").format(sql.Literal(isolation_level))
                        )
                    
                    # Execute each SQL statement
                    for i, sql_stmt in enumerate(sql_statements):
                        if not sql_stmt.strip():
                            continue
                            
                        await cur.execute(sql_stmt)
                        
                        # Try to fetch results if it's a SELECT statement
                        try:
                            rows = await cur.fetchall()
                            columns = [desc.name for desc in cur.description] if cur.description else []
                            
                            statement_result = {
                                "statement_index": i,
                                "sql": sql_stmt,
                                "row_count": len(rows),
                                "columns": columns,
                                "rows": rows if rows else []
                            }
                        except Exception:
                            # Not a SELECT statement or no results
                            statement_result = {
                                "statement_index": i,
                                "sql": sql_stmt,
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

    async def create_index(self, cluster_name: str, object_name: str) -> Dict[str, Any]:
        """
        Create a default index on a source, view, or materialized view.
        
        Args:
            cluster_name: Name of the cluster to maintain this index
            object_name: Name of the source, view, or materialized view to index
            
        Returns:
            Dictionary with the result of the index creation
        """
        pool = self.pool
        async with pool.connection() as conn:
            await conn.set_autocommit(True)
            async with conn.cursor(row_factory=dict_row) as cur:
                # Execute CREATE DEFAULT INDEX statement with proper SQL construction
                create_sql = sql.SQL("CREATE DEFAULT INDEX IN CLUSTER {} ON {}").format(
                    sql.Identifier(cluster_name),
                    sql.Identifier(object_name)
                )
                await cur.execute(create_sql)
                
                # Return success result
                return {
                    "status": "success",
                    "message": f"Default index created successfully on '{object_name}' in cluster '{cluster_name}'",
                    "object_name": object_name,
                    "cluster_name": cluster_name
                }

    async def drop_index(self, index_name: str, cascade: bool = False) -> Dict[str, Any]:
        """
        Drop an index from Materialize.
        
        Args:
            index_name: Name of the index to drop
            cascade: Whether to use CASCADE option (default: False)
            
        Returns:
            Dictionary with the result of the index deletion
        """
        pool = self.pool
        async with pool.connection() as conn:
            await conn.set_autocommit(True)
            async with conn.cursor(row_factory=dict_row) as cur:
                # Execute DROP INDEX statement with proper SQL construction
                if cascade:
                    drop_sql = sql.SQL("DROP INDEX {} CASCADE").format(
                        sql.Identifier(index_name)
                    )
                else:
                    drop_sql = sql.SQL("DROP INDEX {}").format(
                        sql.Identifier(index_name)
                    )
                await cur.execute(drop_sql)
                
                # Return success result
                return {
                    "status": "success",
                    "message": f"Index '{index_name}' dropped successfully",
                    "index_name": index_name,
                    "cascade": cascade
                }

    async def create_view(self, view_name: str, sql_query: str) -> Dict[str, Any]:
        """
        Create a view with the specified name and SQL query.
        
        Args:
            view_name: Name of the view to create
            sql_query: The SELECT statement to embed in the view
            
        Returns:
            Dictionary with the result of the view creation
        """
        pool = self.pool
        async with pool.connection() as conn:
            await conn.set_autocommit(True)
            async with conn.cursor(row_factory=dict_row) as cur:
                # Execute CREATE VIEW statement with proper SQL construction
                # Note: sql_query cannot be parameterized as it's a SQL statement, not a value
                create_sql = sql.SQL("CREATE VIEW {} AS {}").format(
                    sql.Identifier(view_name),
                    sql.SQL(sql_query)  # Treat as raw SQL
                )
                await cur.execute(create_sql)
                
                # Return success result
                return {
                    "status": "success",
                    "message": f"View '{view_name}' created successfully",
                    "view_name": view_name,
                    "sql_query": sql_query
                }

    async def create_materialized_view(
        self, 
        view_name: str,
        cluster_name: str, 
        sql_query: str
    ) -> Dict[str, Any]:
        """
        Create a materialized view with the specified name, cluster, and SQL query.
        
        Args:
            view_name: Name of the materialized view to create
            cluster_name: Name of the cluster to create the materialized view in
            sql_query: The SELECT statement to embed in the materialized view
            
        Returns:
            Dictionary with the result of the materialized view creation
        """
        pool = self.pool
        async with pool.connection() as conn:
            await conn.set_autocommit(True)
            async with conn.cursor(row_factory=dict_row) as cur:
                # Execute CREATE MATERIALIZED VIEW statement with proper SQL construction
                create_sql = sql.SQL("CREATE MATERIALIZED VIEW {} IN CLUSTER {} AS {}").format(
                    sql.Identifier(view_name),
                    sql.Identifier(cluster_name),
                    sql.SQL(sql_query)  # Treat as raw SQL
                )
                await cur.execute(create_sql)
                
                # Return success result
                return {
                    "status": "success",
                    "message": f"Materialized view '{view_name}' created successfully in cluster '{cluster_name}'",
                    "view_name": view_name,
                    "cluster_name": cluster_name,
                    "sql_query": sql_query
                }

    async def show_sources(self, schema: str = None, cluster: str = None) -> list[dict]:
        """
        Show sources, optionally filtered by schema and/or cluster.
        
        Args:
            schema: Optional schema name to filter sources
            cluster: Optional cluster name to filter sources
            
        Returns:
            List of source records
        """
        pool = self.pool
        sources = []
        async with pool.connection() as conn:
            await conn.set_autocommit(True)
            async with conn.cursor(row_factory=dict_row) as cur:
                sql_parts = ["SHOW SOURCES"]
                if schema:
                    sql_parts.append(f"FROM {schema}")
                if cluster:
                    sql_parts.append(f"IN CLUSTER {cluster}")
                sql_stmt = " ".join(sql_parts)
                await cur.execute(sql_stmt)
                async for row in cur:
                    sources.append(row)
        return sources

    async def create_postgres_connection(
        self, 
        connection_name: str,
        host: str, 
        database: str, 
        password_secret: str,
        port: int = 5432,
        username: str = "materialize",
        ssl_mode: str = "require"
    ) -> Dict[str, Any]:
        """
        Create a PostgreSQL connection in Materialize.
        
        Args:
            connection_name: Name of the connection to create
            host: PostgreSQL host address
            database: Database name
            password_secret: Name of the secret containing the password
            port: PostgreSQL port (default: 5432)
            username: PostgreSQL username (default: "materialize")
            ssl_mode: SSL mode (default: "require")
            
        Returns:
            Dictionary with the result of the connection creation
        """
        pool = self.pool
        async with pool.connection() as conn:
            await conn.set_autocommit(True)
            async with conn.cursor(row_factory=dict_row) as cur:
                # Execute CREATE CONNECTION statement
                # Note: password_secret should NOT be quoted as it's an identifier, not a string literal
                # Use sql.SQL and sql.Identifier for safe SQL construction
                create_sql = sql.SQL("""
                CREATE CONNECTION {} TO POSTGRES (
                    HOST {},
                    PORT {},
                    USER {},
                    PASSWORD SECRET {},
                    SSL MODE {},
                    DATABASE {}
                );
                """).format(
                    sql.Identifier(connection_name),
                    sql.Literal(host),
                    sql.Literal(port),
                    sql.Literal(username),
                    sql.Identifier(password_secret),  # password_secret is an identifier, not a literal
                    sql.Literal(ssl_mode),
                    sql.Literal(database)
                )
                await cur.execute(create_sql)
                
                # Return success result
                return {
                    "status": "success",
                    "message": f"PostgreSQL connection '{connection_name}' created successfully",
                    "connection_name": connection_name,
                    "host": host,
                    "port": port,
                    "username": username,
                    "database": database,
                    "ssl_mode": ssl_mode,
                    "password_secret": password_secret
                }

    async def create_postgres_source(
        self, 
        source_name: str,
        cluster_name: str, 
        connection_name: str, 
        publication_name: str,
        for_all_tables: bool = True,
        for_schemas: List[str] = None,
        for_tables: List[str] = None
    ) -> Dict[str, Any]:
        """
        Create a PostgreSQL source in Materialize.
        
        Args:
            source_name: Name of the source to create
            cluster_name: Name of the cluster to create the source in
            connection_name: Name of the PostgreSQL connection to use
            publication_name: Name of the PostgreSQL publication
            for_all_tables: Whether to ingest all tables from the publication (default: True)
            for_schemas: Optional list of schema names to ingest (mutually exclusive with for_all_tables)
            for_tables: Optional list of table names to ingest (mutually exclusive with for_all_tables)
            
        Returns:
            Dictionary with the result of the source creation
        """
        pool = self.pool
        async with pool.connection() as conn:
            await conn.set_autocommit(True)
            async with conn.cursor(row_factory=dict_row) as cur:
                # Build the CREATE SOURCE statement with proper SQL construction
                base_sql = sql.SQL("CREATE SOURCE {} IN CLUSTER {} FROM POSTGRES CONNECTION {} (PUBLICATION {})").format(
                    sql.Identifier(source_name),
                    sql.Identifier(cluster_name),
                    sql.Identifier(connection_name),
                    sql.Literal(publication_name)
                )
                
                # Add the FOR clause based on parameters
                if for_all_tables:
                    create_sql = sql.SQL("{} FOR ALL TABLES").format(base_sql)
                elif for_schemas:
                    schema_list = sql.SQL(", ").join(sql.Literal(schema) for schema in for_schemas)
                    create_sql = sql.SQL("{} FOR SCHEMAS ({})").format(base_sql, schema_list)
                elif for_tables:
                    table_list = sql.SQL(", ").join(sql.Literal(table) for table in for_tables)
                    create_sql = sql.SQL("{} FOR TABLES ({})").format(base_sql, table_list)
                else:
                    # Default to FOR ALL TABLES if no specific option is provided
                    create_sql = sql.SQL("{} FOR ALL TABLES").format(base_sql)
                
                await cur.execute(create_sql)
                
                # Return success result
                return {
                    "status": "success",
                    "message": f"PostgreSQL source '{source_name}' created successfully in cluster '{cluster_name}'",
                    "source_name": source_name,
                    "cluster_name": cluster_name,
                    "connection_name": connection_name,
                    "publication_name": publication_name,
                    "for_all_tables": for_all_tables,
                    "for_schemas": for_schemas,
                    "for_tables": for_tables
                }

    async def list_materialized_views(self, schema: str = None, cluster: str = None) -> list[dict]:
        """
        List materialized views in Materialize, optionally filtered by schema and/or cluster.
        
        Args:
            schema: Optional schema name to filter materialized views
            cluster: Optional cluster name to filter materialized views
            
        Returns:
            List of materialized view records
        """
        pool = self.pool
        materialized_views = []
        async with pool.connection() as conn:
            await conn.set_autocommit(True)
            async with conn.cursor(row_factory=dict_row) as cur:
                sql_parts = ["SHOW MATERIALIZED VIEWS"]
                if schema:
                    sql_parts.append(f"FROM {schema}")
                if cluster:
                    sql_parts.append(f"IN {cluster}")
                sql_stmt = " ".join(sql_parts)
                await cur.execute(sql_stmt)
                async for row in cur:
                    materialized_views.append(row)
        return materialized_views

    async def monitor_data_freshness(self, threshold_seconds: float = 3.0, schema: str = None, cluster: str = None) -> dict:
        """
        Monitor data freshness with dependency-aware analysis. Shows lagging objects and their dependency chains
        to help identify root causes of freshness issues.
        
        Args:
            threshold_seconds: Freshness threshold in seconds (supports fractional values, default: 3.0)
            schema: Optional schema name to filter objects
            cluster: Optional cluster name to filter objects
            
        Returns:
            Dictionary with three categories:
            - lagging_objects: Objects that exceed the freshness threshold
            - dependency_chains: Full dependency chains for each lagging object  
            - critical_paths: Dependency edges that introduce delay, scored by lag amount
        """
        pool = self.pool
        result = {
            'lagging_objects': [],
            'dependency_chains': [],
            'critical_paths': []
        }
        
        async with pool.connection() as conn:
            await conn.set_autocommit(True)
            async with conn.cursor(row_factory=dict_row) as cur:
                
                # 1. Get basic lagging objects info
                lagging_query = """
                SELECT 
                    f.object_id,
                    o.name as object_name,
                    s.name as schema_name,
                    o.type as object_type,
                    c.name as cluster_name,
                    f.write_frontier,
                    to_timestamp(f.write_frontier::text::numeric / 1000) as write_frontier_time,
                    EXTRACT(EPOCH FROM (mz_now()::timestamp - to_timestamp(f.write_frontier::text::numeric / 1000))) as lag_seconds
                FROM mz_internal.mz_frontiers f
                JOIN mz_objects o ON f.object_id = o.id
                JOIN mz_schemas s ON o.schema_id = s.id
                LEFT JOIN mz_clusters c ON o.cluster_id = c.id
                WHERE f.write_frontier > 0
                  AND mz_now() > to_timestamp(f.write_frontier::text::numeric / 1000) + INTERVAL '1 second' * %s
                  AND f.object_id LIKE 'u%%'
                """
                
                params = [threshold_seconds]
                if schema:
                    lagging_query += " AND s.name = %s"
                    params.append(schema)
                if cluster:
                    lagging_query += " AND c.name = %s"
                    params.append(cluster)
                    
                lagging_query += " ORDER BY lag_seconds DESC"
                
                await cur.execute(lagging_query, params)
                async for row in cur:
                    lag_ms = row['lag_seconds'] * 1000 if row['lag_seconds'] else None
                    result['lagging_objects'].append({
                        'object_id': row['object_id'],
                        'object_name': row['object_name'],
                        'schema_name': row['schema_name'], 
                        'object_type': row['object_type'],
                        'cluster_name': row['cluster_name'],
                        'write_frontier': row['write_frontier'],
                        'write_frontier_time': row['write_frontier_time'],
                        'lag_seconds': row['lag_seconds'],
                        'lag_ms': lag_ms,
                        'threshold_seconds': threshold_seconds
                    })
                
                # 2. Get full dependency chains for lagging objects
                dependency_query = """
                WITH MUTUALLY RECURSIVE
                input_of (source text, target text) AS (
                    SELECT dependency_id, object_id 
                    FROM mz_internal.mz_compute_dependencies
                ),
                probes (id text) AS (
                    SELECT object_id
                    FROM mz_internal.mz_frontiers
                    WHERE write_frontier > 0
                      AND mz_now() > to_timestamp(write_frontier::text::numeric / 1000) + INTERVAL '1 second' * %s
                      AND object_id LIKE 'u%%'
                ),
                depends_on(probe text, prev text, next text) AS (
                    SELECT id, id, id FROM probes
                    UNION
                    SELECT depends_on.probe, input_of.source, input_of.target
                    FROM input_of, depends_on
                    WHERE depends_on.prev = input_of.target
                )
                SELECT 
                    depends_on.probe,
                    depends_on.prev as dependency_id,
                    depends_on.next as dependent_id,
                    prev_obj.name as dependency_name,
                    next_obj.name as dependent_name,
                    prev_obj.type as dependency_type,
                    next_obj.type as dependent_type
                FROM depends_on
                LEFT JOIN mz_objects prev_obj ON depends_on.prev = prev_obj.id
                LEFT JOIN mz_objects next_obj ON depends_on.next = next_obj.id
                WHERE probe != next
                ORDER BY probe, prev, next
                """
                
                await cur.execute(dependency_query, [threshold_seconds])
                async for row in cur:
                    result['dependency_chains'].append({
                        'probe_id': row['probe'],
                        'dependency_id': row['dependency_id'],
                        'dependent_id': row['dependent_id'],
                        'dependency_name': row['dependency_name'],
                        'dependent_name': row['dependent_name'],
                        'dependency_type': row['dependency_type'],
                        'dependent_type': row['dependent_type']
                    })
                
                # 3. Get critical path analysis with lag scoring
                critical_path_query = """
                WITH MUTUALLY RECURSIVE
                input_of (source text, target text) AS (
                    SELECT dependency_id, object_id 
                    FROM mz_internal.mz_compute_dependencies
                ),
                probes (id text) AS (
                    SELECT object_id
                    FROM mz_internal.mz_frontiers
                    WHERE write_frontier > 0
                      AND mz_now() > to_timestamp(write_frontier::text::numeric / 1000) + INTERVAL '1 second' * %s
                      AND object_id LIKE 'u%%'
                ),
                depends_on(probe text, prev text, next text) AS (
                    SELECT id, id, id FROM probes
                    UNION
                    SELECT depends_on.probe, input_of.source, input_of.target
                    FROM input_of, depends_on
                    WHERE depends_on.prev = input_of.target
                )
                SELECT 
                    depends_on.probe,
                    depends_on.prev as source_id,
                    depends_on.next as target_id,
                    prev_obj.name as source_name,
                    next_obj.name as target_name,
                    prev_obj.type as source_type,
                    next_obj.type as target_type,
                    fp.write_frontier::text::numeric - fn.write_frontier::text::numeric as lag_ms,
                    (fp.write_frontier::text::numeric - fn.write_frontier::text::numeric) / 1000.0 as lag_seconds
                FROM depends_on, 
                     mz_internal.mz_frontiers fn, 
                     mz_internal.mz_frontiers fp
                LEFT JOIN mz_objects prev_obj ON depends_on.prev = prev_obj.id
                LEFT JOIN mz_objects next_obj ON depends_on.next = next_obj.id
                WHERE probe != prev
                  AND depends_on.next = fn.object_id
                  AND depends_on.prev = fp.object_id  
                  AND fp.write_frontier > fn.write_frontier
                ORDER BY lag_ms DESC
                """
                
                await cur.execute(critical_path_query, [threshold_seconds])
                async for row in cur:
                    result['critical_paths'].append({
                        'probe_id': row['probe'],
                        'source_id': row['source_id'],
                        'target_id': row['target_id'],
                        'source_name': row['source_name'],
                        'target_name': row['target_name'],
                        'source_type': row['source_type'],
                        'target_type': row['target_type'],
                        'lag_ms': row['lag_ms'],
                        'lag_seconds': row['lag_seconds']
                    })
                    
        return result


    async def get_object_freshness_diagnostics(self, object_name: str, schema: str = "public") -> dict:
        """
        Get detailed freshness diagnostics for a specific object, showing its freshness and 
        the complete dependency chain with freshness information for each dependency.
        
        Args:
            object_name: Name of the object to analyze
            schema: Schema name (default: "public")
            
        Returns:
            Dictionary containing:
            - target_object: Information about the target object's freshness
            - dependency_chain: All dependencies with their individual freshness
            - freshness_summary: Summary of freshness issues in the chain
        """
        pool = self.pool
        result = {
            'target_object': None,
            'dependency_chain': [],
            'freshness_summary': {
                'total_dependencies': 0,
                'stale_dependencies': 0,
                'max_lag_seconds': 0,
                'critical_path_lag_seconds': 0
            }
        }
        
        async with pool.connection() as conn:
            await conn.set_autocommit(True)
            async with conn.cursor(row_factory=dict_row) as cur:
                
                # 1. Get target object information and freshness
                target_query = """
                SELECT 
                    f.object_id,
                    o.name as object_name,
                    s.name as schema_name,
                    o.type as object_type,
                    c.name as cluster_name,
                    f.write_frontier,
                    to_timestamp(f.write_frontier::text::numeric / 1000) as write_frontier_time,
                    EXTRACT(EPOCH FROM (mz_now()::timestamp - to_timestamp(f.write_frontier::text::numeric / 1000))) as lag_seconds
                FROM mz_objects o
                JOIN mz_schemas s ON o.schema_id = s.id
                LEFT JOIN mz_clusters c ON o.cluster_id = c.id
                LEFT JOIN mz_internal.mz_frontiers f ON o.id = f.object_id
                WHERE o.name = %s AND s.name = %s
                LIMIT 1
                """
                
                await cur.execute(target_query, [object_name, schema])
                target_row = await cur.fetchone()
                
                if not target_row:
                    return {
                        'error': f"Object '{object_name}' not found in schema '{schema}'"
                    }
                
                # Calculate lag in milliseconds
                lag_ms = target_row['lag_seconds'] * 1000 if target_row['lag_seconds'] else 0
                
                result['target_object'] = {
                    'object_id': target_row['object_id'],
                    'object_name': target_row['object_name'],
                    'schema_name': target_row['schema_name'], 
                    'object_type': target_row['object_type'],
                    'cluster_name': target_row['cluster_name'],
                    'write_frontier': target_row['write_frontier'],
                    'write_frontier_time': target_row['write_frontier_time'],
                    'lag_seconds': target_row['lag_seconds'],
                    'lag_ms': lag_ms,
                    'is_stale': lag_ms > 100  # Consider >100ms as stale
                }
                
                target_object_id = target_row['object_id']
                
                # 2. Get complete dependency chain with freshness for each dependency
                dependency_query = """
                WITH MUTUALLY RECURSIVE
                input_of (source text, target text) AS (
                    SELECT dependency_id, object_id 
                    FROM mz_internal.mz_compute_dependencies
                ),
                depends_on(prev text, next text, depth int) AS (
                    SELECT %s, %s, 0
                    UNION
                    SELECT input_of.source, depends_on.prev, depends_on.depth + 1
                    FROM input_of, depends_on
                    WHERE depends_on.prev = input_of.target
                    AND depends_on.depth < 10  -- Prevent infinite recursion
                )
                SELECT DISTINCT
                    depends_on.prev as dependency_id,
                    depends_on.next as dependent_id,
                    depends_on.depth,
                    dep_obj.name as dependency_name,
                    dep_obj.type as dependency_type,
                    dep_schema.name as dependency_schema,
                    dep_cluster.name as dependency_cluster,
                    dependent_obj.name as dependent_name,
                    dependent_obj.type as dependent_type,
                    dependent_schema.name as dependent_schema,
                    dependent_cluster.name as dependent_cluster,
                    dep_f.write_frontier as dependency_frontier,
                    to_timestamp(dep_f.write_frontier::text::numeric / 1000) as dependency_frontier_time,
                    EXTRACT(EPOCH FROM (mz_now()::timestamp - to_timestamp(dep_f.write_frontier::text::numeric / 1000))) as dependency_lag_seconds,
                    dependent_f.write_frontier as dependent_frontier,
                    to_timestamp(dependent_f.write_frontier::text::numeric / 1000) as dependent_frontier_time,
                    EXTRACT(EPOCH FROM (mz_now()::timestamp - to_timestamp(dependent_f.write_frontier::text::numeric / 1000))) as dependent_lag_seconds
                FROM depends_on
                LEFT JOIN mz_objects dep_obj ON depends_on.prev = dep_obj.id
                LEFT JOIN mz_schemas dep_schema ON dep_obj.schema_id = dep_schema.id
                LEFT JOIN mz_clusters dep_cluster ON dep_obj.cluster_id = dep_cluster.id
                LEFT JOIN mz_internal.mz_frontiers dep_f ON depends_on.prev = dep_f.object_id
                LEFT JOIN mz_objects dependent_obj ON depends_on.next = dependent_obj.id
                LEFT JOIN mz_schemas dependent_schema ON dependent_obj.schema_id = dependent_schema.id
                LEFT JOIN mz_clusters dependent_cluster ON dependent_obj.cluster_id = dependent_cluster.id
                LEFT JOIN mz_internal.mz_frontiers dependent_f ON depends_on.next = dependent_f.object_id
                WHERE depends_on.prev != depends_on.next
                ORDER BY depends_on.depth, depends_on.prev, depends_on.next
                """
                
                await cur.execute(dependency_query, [target_object_id, target_object_id])
                
                max_lag = 0
                stale_count = 0
                total_count = 0
                
                async for row in cur:
                    total_count += 1
                    
                    # Calculate lag for dependency
                    dep_lag_seconds = row['dependency_lag_seconds'] or 0
                    dep_lag_ms = dep_lag_seconds * 1000
                    dependent_lag_seconds = row['dependent_lag_seconds'] or 0
                    dependent_lag_ms = dependent_lag_seconds * 1000
                    
                    is_dep_stale = dep_lag_ms > 100
                    is_dependent_stale = dependent_lag_ms > 100
                    
                    if is_dep_stale:
                        stale_count += 1
                    
                    max_lag = max(max_lag, dep_lag_seconds, dependent_lag_seconds)
                    
                    result['dependency_chain'].append({
                        'depth': row['depth'],
                        'dependency': {
                            'object_id': row['dependency_id'],
                            'name': row['dependency_name'],
                            'type': row['dependency_type'],
                            'schema': row['dependency_schema'],
                            'cluster': row['dependency_cluster'],
                            'write_frontier': row['dependency_frontier'],
                            'write_frontier_time': row['dependency_frontier_time'],
                            'lag_seconds': dep_lag_seconds,
                            'lag_ms': dep_lag_ms,
                            'is_stale': is_dep_stale
                        },
                        'dependent': {
                            'object_id': row['dependent_id'],
                            'name': row['dependent_name'],
                            'type': row['dependent_type'],
                            'schema': row['dependent_schema'],
                            'cluster': row['dependent_cluster'],
                            'write_frontier': row['dependent_frontier'],
                            'write_frontier_time': row['dependent_frontier_time'],
                            'lag_seconds': dependent_lag_seconds,
                            'lag_ms': dependent_lag_ms,
                            'is_stale': is_dependent_stale
                        }
                    })
                
                # Update summary
                result['freshness_summary'] = {
                    'total_dependencies': total_count,
                    'stale_dependencies': stale_count,
                    'max_lag_seconds': max_lag,
                    'critical_path_lag_seconds': max_lag  # For now, use max lag as critical path
                }
                    
        return result


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
