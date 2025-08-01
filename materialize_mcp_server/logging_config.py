"""
Structured logging configuration for the Materialize MCP server.
"""
import json
import logging
import sys
import time
import uuid
from contextvars import ContextVar
from typing import Any, Dict, Optional
from datetime import datetime


# Context variable to store correlation ID across async calls
correlation_id_var: ContextVar[Optional[str]] = ContextVar('correlation_id', default=None)


class StructuredFormatter(logging.Formatter):
    """
    Custom formatter that outputs structured JSON logs with correlation IDs.
    """
    
    def __init__(self, service_name: str = "materialize_mcp_server", version: str = "1.0.0"):
        super().__init__()
        self.service_name = service_name
        self.version = version
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as structured JSON."""
        # Base log structure
        log_entry = {
            "timestamp": datetime.utcfromtimestamp(record.created).isoformat() + "Z",
            "level": record.levelname,
            "service": self.service_name,
            "version": self.version,
            "logger": record.name,
            "message": record.getMessage(),
            "correlation_id": correlation_id_var.get(),
        }
        
        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = {
                "type": record.exc_info[0].__name__ if record.exc_info[0] else None,
                "message": str(record.exc_info[1]) if record.exc_info[1] else None,
                "traceback": self.formatException(record.exc_info) if record.exc_info else None
            }
        
        # Add extra fields from the log record
        extra_fields = {}
        for key, value in record.__dict__.items():
            if key not in {
                'name', 'msg', 'args', 'levelname', 'levelno', 'pathname', 'filename',
                'module', 'lineno', 'funcName', 'created', 'msecs', 'relativeCreated',
                'thread', 'threadName', 'processName', 'process', 'message', 'exc_info',
                'exc_text', 'stack_info', 'getMessage'
            }:
                extra_fields[key] = value
        
        if extra_fields:
            log_entry["extra"] = extra_fields
        
        # Add source location for debug logs
        if record.levelno <= logging.DEBUG:
            log_entry["source"] = {
                "file": record.filename,
                "line": record.lineno,
                "function": record.funcName
            }
        
        return json.dumps(log_entry, default=str, separators=(',', ':'))


class CorrelationIdFilter(logging.Filter):
    """
    Filter that ensures all log records have a correlation ID.
    """
    
    def filter(self, record: logging.LogRecord) -> bool:
        """Add correlation ID to record if not present."""
        if not hasattr(record, 'correlation_id'):
            record.correlation_id = correlation_id_var.get()
        return True


def setup_structured_logging(
    service_name: str = "materialize_mcp_server",
    version: str = "1.0.0",
    log_level: str = "INFO",
    enable_json: bool = True
) -> logging.Logger:
    """
    Set up structured logging for the application.
    
    Args:
        service_name: Name of the service for log identification
        version: Version of the service
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        enable_json: Whether to use JSON structured logging
        
    Returns:
        Configured logger instance
    """
    # Create logger
    logger = logging.getLogger(service_name)
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Remove existing handlers to avoid duplicates
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Create console handler
    handler = logging.StreamHandler(sys.stderr)
    
    if enable_json:
        # Use structured JSON formatter
        formatter = StructuredFormatter(service_name, version)
    else:
        # Use simple text formatter for development
        formatter = logging.Formatter(
            fmt='%(asctime)s [%(levelname)s] %(name)s: %(message)s (correlation_id=%(correlation_id)s)',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    
    handler.setFormatter(formatter)
    
    # Add correlation ID filter
    correlation_filter = CorrelationIdFilter()
    handler.addFilter(correlation_filter)
    
    logger.addHandler(handler)
    
    # Prevent propagation to root logger to avoid duplicate logs
    logger.propagate = False
    
    return logger


def generate_correlation_id() -> str:
    """Generate a new correlation ID."""
    return str(uuid.uuid4())


def set_correlation_id(correlation_id: str) -> None:
    """Set the correlation ID for the current context."""
    correlation_id_var.set(correlation_id)


def get_correlation_id() -> Optional[str]:
    """Get the current correlation ID."""
    return correlation_id_var.get()


def ensure_correlation_id() -> str:
    """Ensure a correlation ID exists, creating one if necessary."""
    correlation_id = get_correlation_id()
    if not correlation_id:
        correlation_id = generate_correlation_id()
        set_correlation_id(correlation_id)
    return correlation_id


class LogContext:
    """
    Context manager for adding structured data to log records.
    """
    
    def __init__(self, logger: logging.Logger, **extra_fields):
        self.logger = logger
        self.extra_fields = extra_fields
        self.original_fields = {}
    
    def __enter__(self):
        # Store original values and set new ones
        for key, value in self.extra_fields.items():
            if hasattr(self.logger, key):
                self.original_fields[key] = getattr(self.logger, key)
            setattr(self.logger, key, value)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Restore original values
        for key in self.extra_fields:
            if key in self.original_fields:
                setattr(self.logger, key, self.original_fields[key])
            elif hasattr(self.logger, key):
                delattr(self.logger, key)


def log_tool_call(logger: logging.Logger, tool_name: str, args: Dict[str, Any], start_time: float = None):
    """
    Log a tool call with structured data.
    
    Args:
        logger: Logger instance
        tool_name: Name of the tool being called
        args: Arguments passed to the tool
        start_time: Start time of the tool call (from time.time())
    """
    if start_time is None:
        start_time = time.time()
    
    # Sanitize arguments to remove sensitive data
    sanitized_args = {}
    sensitive_keys = {'password', 'secret', 'token', 'key', 'credential'}
    
    for key, value in args.items():
        if any(sensitive_key in key.lower() for sensitive_key in sensitive_keys):
            sanitized_args[key] = "[REDACTED]"
        else:
            sanitized_args[key] = value
    
    logger.info(
        f"Tool call started: {tool_name}",
        extra={
            "event_type": "tool_call_start",
            "tool_name": tool_name,
            "tool_args": sanitized_args,
            "start_time": start_time
        }
    )


def log_tool_result(
    logger: logging.Logger, 
    tool_name: str, 
    start_time: float, 
    success: bool = True, 
    error: Optional[Exception] = None,
    result_size: Optional[int] = None
):
    """
    Log the result of a tool call.
    
    Args:
        logger: Logger instance
        tool_name: Name of the tool that was called
        start_time: Start time of the tool call
        success: Whether the tool call was successful
        error: Exception if the tool call failed
        result_size: Size of the result (e.g., number of rows returned)
    """
    end_time = time.time()
    duration = end_time - start_time
    
    extra_data = {
        "event_type": "tool_call_end",
        "tool_name": tool_name,
        "duration_ms": round(duration * 1000, 2),
        "success": success
    }
    
    if result_size is not None:
        extra_data["result_size"] = result_size
    
    if success:
        logger.info(
            f"Tool call completed: {tool_name} ({duration*1000:.2f}ms)",
            extra=extra_data
        )
    else:
        if error:
            extra_data["error_type"] = type(error).__name__
            extra_data["error_message"] = str(error)
        
        logger.error(
            f"Tool call failed: {tool_name} ({duration*1000:.2f}ms)",
            extra=extra_data,
            exc_info=error
        )


def log_database_operation(
    logger: logging.Logger,
    operation: str,
    query: str = None,
    params: list = None,
    rows_affected: int = None,
    duration_ms: float = None
):
    """
    Log a database operation with structured data.
    
    Args:
        logger: Logger instance
        operation: Type of operation (SELECT, INSERT, UPDATE, etc.)
        query: SQL query (will be truncated if too long)
        params: Query parameters
        rows_affected: Number of rows affected
        duration_ms: Duration in milliseconds
    """
    extra_data = {
        "event_type": "database_operation",
        "operation": operation
    }
    
    if query:
        # Truncate long queries
        truncated_query = query[:1000] + "..." if len(query) > 1000 else query
        extra_data["query"] = truncated_query
    
    if params:
        # Sanitize parameters
        sanitized_params = []
        for param in params:
            if isinstance(param, str) and any(
                keyword in param.lower() for keyword in ['password', 'secret', 'token']
            ):
                sanitized_params.append("[REDACTED]")
            else:
                sanitized_params.append(param)
        extra_data["params"] = sanitized_params
    
    if rows_affected is not None:
        extra_data["rows_affected"] = rows_affected
    
    if duration_ms is not None:
        extra_data["duration_ms"] = duration_ms
    
    logger.debug(
        f"Database operation: {operation}",
        extra=extra_data
    )