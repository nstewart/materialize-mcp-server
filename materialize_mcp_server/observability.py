"""
Observability module for the Materialize MCP server.
Provides metrics collection, health checks, and monitoring capabilities.
"""
import asyncio
import time
import psutil
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Deque
from contextlib import asynccontextmanager
import logging

from psycopg_pool import AsyncConnectionPool


@dataclass
class ToolMetrics:
    """Metrics for a specific tool."""
    call_count: int = 0
    success_count: int = 0
    error_count: int = 0
    total_duration_ms: float = 0.0
    min_duration_ms: float = float('inf')
    max_duration_ms: float = 0.0
    recent_calls: Deque[Dict[str, Any]] = field(default_factory=lambda: deque(maxlen=100))
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate as a percentage."""
        if self.call_count == 0:
            return 0.0
        return (self.success_count / self.call_count) * 100.0
    
    @property
    def avg_duration_ms(self) -> float:
        """Calculate average duration in milliseconds."""
        if self.call_count == 0:
            return 0.0
        return self.total_duration_ms / self.call_count


@dataclass
class ConnectionPoolMetrics:
    """Metrics for database connection pool."""
    total_connections: int = 0
    active_connections: int = 0
    idle_connections: int = 0
    min_connections: int = 0
    max_connections: int = 0
    pool_hits: int = 0
    pool_misses: int = 0
    connection_errors: int = 0
    total_queries: int = 0
    query_errors: int = 0
    avg_query_duration_ms: float = 0.0


@dataclass
class SystemMetrics:
    """System-level metrics."""
    cpu_percent: float = 0.0
    memory_percent: float = 0.0
    memory_used_mb: float = 0.0
    disk_usage_percent: float = 0.0
    uptime_seconds: float = 0.0
    python_version: str = ""
    
    @classmethod
    def collect(cls) -> 'SystemMetrics':
        """Collect current system metrics."""
        import sys
        import platform
        
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        return cls(
            cpu_percent=psutil.cpu_percent(interval=0.1),
            memory_percent=memory.percent,
            memory_used_mb=memory.used / 1024 / 1024,
            disk_usage_percent=disk.percent,
            uptime_seconds=time.time() - psutil.boot_time(),
            python_version=f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
        )


class MetricsCollector:
    """
    Collects and stores metrics for the MCP server.
    """
    
    def __init__(self):
        self.start_time = time.time()
        self.tool_metrics: Dict[str, ToolMetrics] = defaultdict(ToolMetrics)
        self.connection_metrics = ConnectionPoolMetrics()
        self.system_metrics = SystemMetrics()
        self.health_checks: Dict[str, bool] = {}
        self.error_counts: Dict[str, int] = defaultdict(int)
        self._lock = asyncio.Lock()
    
    async def record_tool_call(
        self, 
        tool_name: str, 
        duration_ms: float, 
        success: bool, 
        error: Optional[Exception] = None
    ):
        """Record metrics for a tool call."""
        async with self._lock:
            metrics = self.tool_metrics[tool_name]
            
            metrics.call_count += 1
            metrics.total_duration_ms += duration_ms
            metrics.min_duration_ms = min(metrics.min_duration_ms, duration_ms)
            metrics.max_duration_ms = max(metrics.max_duration_ms, duration_ms)
            
            if success:
                metrics.success_count += 1
            else:
                metrics.error_count += 1
                if error:
                    error_type = type(error).__name__
                    self.error_counts[f"{tool_name}:{error_type}"] += 1
            
            # Store recent call info
            metrics.recent_calls.append({
                "timestamp": time.time(),
                "duration_ms": duration_ms,
                "success": success,
                "error": str(error) if error else None
            })
    
    async def record_database_operation(
        self, 
        operation_type: str, 
        duration_ms: float, 
        success: bool,
        rows_affected: int = 0
    ):
        """Record metrics for a database operation."""
        async with self._lock:
            self.connection_metrics.total_queries += 1
            
            if success:
                # Update average query duration
                current_avg = self.connection_metrics.avg_query_duration_ms
                total_queries = self.connection_metrics.total_queries
                self.connection_metrics.avg_query_duration_ms = (
                    (current_avg * (total_queries - 1) + duration_ms) / total_queries
                )
            else:
                self.connection_metrics.query_errors += 1
    
    async def update_connection_pool_metrics(self, pool: AsyncConnectionPool):
        """Update connection pool metrics."""
        async with self._lock:
            # Note: psycopg pool doesn't expose all these metrics directly
            # This is a simplified version - in production you'd want more detailed metrics
            self.connection_metrics.min_connections = pool.min_size
            self.connection_metrics.max_connections = pool.max_size
            # pool.get_stats() would be ideal but isn't available in current version
    
    def update_system_metrics(self):
        """Update system-level metrics."""
        self.system_metrics = SystemMetrics.collect()
    
    def get_uptime_seconds(self) -> float:
        """Get server uptime in seconds."""
        return time.time() - self.start_time
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get a summary of all metrics."""
        return {
            "uptime_seconds": self.get_uptime_seconds(),
            "tool_metrics": {
                name: {
                    "call_count": metrics.call_count,
                    "success_count": metrics.success_count,
                    "error_count": metrics.error_count,
                    "success_rate": metrics.success_rate,
                    "avg_duration_ms": metrics.avg_duration_ms,
                    "min_duration_ms": metrics.min_duration_ms if metrics.min_duration_ms != float('inf') else 0,
                    "max_duration_ms": metrics.max_duration_ms
                }
                for name, metrics in self.tool_metrics.items()
            },
            "connection_metrics": {
                "total_queries": self.connection_metrics.total_queries,
                "query_errors": self.connection_metrics.query_errors,
                "avg_query_duration_ms": self.connection_metrics.avg_query_duration_ms,
                "min_connections": self.connection_metrics.min_connections,
                "max_connections": self.connection_metrics.max_connections
            },
            "system_metrics": {
                "cpu_percent": self.system_metrics.cpu_percent,
                "memory_percent": self.system_metrics.memory_percent,
                "memory_used_mb": self.system_metrics.memory_used_mb,
                "disk_usage_percent": self.system_metrics.disk_usage_percent,
                "python_version": self.system_metrics.python_version
            },
            "error_counts": dict(self.error_counts),
            "health_checks": self.health_checks
        }


class HealthChecker:
    """
    Performs health checks for various system components.
    """
    
    def __init__(self, pool: AsyncConnectionPool, logger: logging.Logger):
        self.pool = pool
        self.logger = logger
        self.last_check_time = 0
        self.check_interval = 30  # seconds
        self.health_status = {
            "database": False,
            "connection_pool": False,
            "system": False
        }
    
    async def check_database_health(self) -> bool:
        """Check if database connection is healthy."""
        try:
            async with self.pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT 1")
                    result = await cur.fetchone()
                    return result[0] == 1
        except Exception as e:
            self.logger.error(f"Database health check failed: {e}")
            return False
    
    async def check_connection_pool_health(self) -> bool:
        """Check if connection pool is healthy."""
        try:
            # Basic check - try to get pool statistics
            # In a real implementation, you'd check pool size, available connections, etc.
            return not self.pool.closed
        except Exception as e:
            self.logger.error(f"Connection pool health check failed: {e}")
            return False
    
    def check_system_health(self) -> bool:
        """Check if system resources are healthy."""
        try:
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            # Consider system unhealthy if memory usage > 90% or disk usage > 95%
            if memory.percent > 90:
                self.logger.warning(f"High memory usage: {memory.percent}%")
                return False
            
            if disk.percent > 95:
                self.logger.warning(f"High disk usage: {disk.percent}%")
                return False
            
            return True
        except Exception as e:
            self.logger.error(f"System health check failed: {e}")
            return False
    
    async def perform_health_checks(self) -> Dict[str, bool]:
        """Perform all health checks and return results."""
        current_time = time.time()
        
        # Only run checks if enough time has passed
        if current_time - self.last_check_time < self.check_interval:
            return self.health_status
        
        self.logger.debug("Performing health checks...")
        
        # Run health checks
        self.health_status["database"] = await self.check_database_health()
        self.health_status["connection_pool"] = await self.check_connection_pool_health()
        self.health_status["system"] = self.check_system_health()
        
        self.last_check_time = current_time
        
        # Log any failed checks
        failed_checks = [name for name, status in self.health_status.items() if not status]
        if failed_checks:
            self.logger.warning(f"Health checks failed: {', '.join(failed_checks)}")
        else:
            self.logger.debug("All health checks passed")
        
        return self.health_status
    
    def is_healthy(self) -> bool:
        """Check if all systems are healthy."""
        return all(self.health_status.values())


@asynccontextmanager
async def track_tool_call(
    metrics_collector: MetricsCollector,
    tool_name: str,
    logger: logging.Logger
):
    """
    Context manager to track tool call metrics and logging.
    """
    start_time = time.time()
    correlation_id = getattr(logger, 'correlation_id', None)
    
    try:
        logger.info(
            f"Tool call started: {tool_name}",
            extra={
                "event_type": "tool_call_start",
                "tool_name": tool_name,
                "correlation_id": correlation_id
            }
        )
        yield
        
        # Success case
        duration_ms = (time.time() - start_time) * 1000
        await metrics_collector.record_tool_call(tool_name, duration_ms, True)
        
        logger.info(
            f"Tool call completed: {tool_name} ({duration_ms:.2f}ms)",
            extra={
                "event_type": "tool_call_success",
                "tool_name": tool_name,
                "duration_ms": duration_ms,
                "correlation_id": correlation_id
            }
        )
    except Exception as e:
        # Error case
        duration_ms = (time.time() - start_time) * 1000
        await metrics_collector.record_tool_call(tool_name, duration_ms, False, e)
        
        logger.error(
            f"Tool call failed: {tool_name} ({duration_ms:.2f}ms)",
            extra={
                "event_type": "tool_call_error",
                "tool_name": tool_name,
                "duration_ms": duration_ms,
                "error_type": type(e).__name__,
                "error_message": str(e),
                "correlation_id": correlation_id
            },
            exc_info=True
        )
        raise


async def create_health_check_endpoint() -> Dict[str, Any]:
    """
    Create a health check response that can be exposed via HTTP endpoint.
    This would typically be used with a web framework like FastAPI or Flask.
    """
    # This is a placeholder - in a real implementation, you'd integrate with your web framework
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "version": "1.0.0",
        "checks": {
            "database": "healthy",
            "connection_pool": "healthy",
            "system": "healthy"
        }
    }


# Global metrics collector instance
_metrics_collector: Optional[MetricsCollector] = None


def get_metrics_collector() -> MetricsCollector:
    """Get the global metrics collector instance."""
    global _metrics_collector
    if _metrics_collector is None:
        _metrics_collector = MetricsCollector()
    return _metrics_collector


def reset_metrics_collector():
    """Reset the global metrics collector (useful for testing)."""
    global _metrics_collector
    _metrics_collector = MetricsCollector()