import argparse
import os
import re
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse

from .validation import InputValidator, ValidationError


class ConfigurationError(Exception):
    """Raised when configuration is invalid or cannot be loaded."""
    pass


@dataclass(frozen=True)
class Config:
    dsn: str
    transport: str
    host: str
    port: int
    pool_min_size: int
    pool_max_size: int
    log_level: str
    enable_json_logging: bool
    service_name: str
    version: str
    
    def validate(self) -> None:
        """Validate the configuration settings."""
        errors = []
        
        # Validate DSN
        try:
            parsed_dsn = urlparse(self.dsn)
            if not parsed_dsn.scheme:
                errors.append("DSN must include a scheme (e.g., postgresql://)")
            if not parsed_dsn.hostname:
                errors.append("DSN must include a hostname")
            if parsed_dsn.port and (parsed_dsn.port < 1 or parsed_dsn.port > 65535):
                errors.append("DSN port must be between 1 and 65535")
        except Exception as e:
            errors.append(f"Invalid DSN format: {e}")
        
        # Validate transport
        if self.transport not in ["stdio", "sse"]:
            errors.append("Transport must be either 'stdio' or 'sse'")
        
        # Validate host (for SSE transport)
        if self.transport == "sse":
            # Basic IP/hostname validation
            if not re.match(r'^[a-zA-Z0-9.-]+$', self.host):
                errors.append("Host contains invalid characters")
        
        # Validate port
        port_result = InputValidator.validate_port(self.port)
        if not port_result.is_valid:
            errors.extend([error.message for error in port_result.errors])
        
        # Validate pool sizes
        min_result = InputValidator.validate_pool_size(self.pool_min_size, "pool_min_size")
        if not min_result.is_valid:
            errors.extend([error.message for error in min_result.errors])
            
        max_result = InputValidator.validate_pool_size(self.pool_max_size, "pool_max_size")
        if not max_result.is_valid:
            errors.extend([error.message for error in max_result.errors])
        
        if self.pool_min_size > self.pool_max_size:
            errors.append("pool_min_size cannot be greater than pool_max_size")
        
        # Validate log level
        valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if self.log_level.upper() not in valid_log_levels:
            errors.append(f"log_level must be one of: {', '.join(valid_log_levels)}")
        
        # Validate service name
        if not self.service_name:
            errors.append("service_name cannot be empty")
        elif not re.match(r'^[a-zA-Z0-9_-]+$', self.service_name):
            errors.append("service_name can only contain letters, numbers, underscores, and hyphens")
        
        # Validate version
        if not self.version:
            errors.append("version cannot be empty")
        elif not re.match(r'^\d+\.\d+\.\d+(-[a-zA-Z0-9.-]+)?$', self.version):
            errors.append("version must follow semantic versioning (e.g., 1.0.0 or 1.0.0-beta.1)")
        
        if errors:
            raise ValidationError("configuration", "; ".join(errors))


def load_config() -> Config:
    """Load and validate configuration from environment variables and command line arguments."""
    parser = argparse.ArgumentParser(
        description="Run Materialize MCP server",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Environment Variables:
  MCP_TRANSPORT          Communication transport (stdio|sse)
  MZ_DSN                 Materialize database connection string
  MCP_HOST               Server host for SSE transport
  MCP_PORT               Server port for SSE transport
  MCP_POOL_MIN_SIZE      Minimum database connection pool size
  MCP_POOL_MAX_SIZE      Maximum database connection pool size
  MCP_LOG_LEVEL          Logging level (DEBUG|INFO|WARNING|ERROR|CRITICAL)
  MCP_ENABLE_JSON_LOG    Enable JSON structured logging (true|false)
  MCP_SERVICE_NAME       Service name for logging and metrics
  MCP_VERSION            Service version
"""
    )
    parser.add_argument(
        "--transport",
        choices=["stdio", "sse"],
        default=os.getenv("MCP_TRANSPORT", "stdio"),
        help="Communication transport (default: stdio)",
    )

    parser.add_argument(
        "--mz-dsn",
        default=os.getenv(
            "MZ_DSN", "postgresql://materialize@localhost:6875/materialize"
        ),
        help="Materialize DSN (default: postgresql://materialize@localhost:6875/materialize)",
    )

    parser.add_argument(
        "--host",
        default=os.getenv("MCP_HOST", "0.0.0.0"),
        help="Server host (default: 0.0.0.0)",
    )

    parser.add_argument(
        "--port",
        type=int,
        default=_safe_int(os.getenv("MCP_PORT", "3001")),
        help="Server port for SSE transport (default: 3001)",
    )

    parser.add_argument(
        "--pool-min-size",
        type=int,
        default=_safe_int(os.getenv("MCP_POOL_MIN_SIZE", "1")),
        help="Minimum connection pool size (default: 1)",
    )

    parser.add_argument(
        "--pool-max-size",
        type=int,
        default=_safe_int(os.getenv("MCP_POOL_MAX_SIZE", "10")),
        help="Maximum connection pool size (default: 10)",
    )

    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default=os.getenv("MCP_LOG_LEVEL", "INFO").upper(),
        help="Logging level (default: INFO)",
    )

    parser.add_argument(
        "--enable-json-logging",
        action="store_true",
        default=_safe_bool(os.getenv("MCP_ENABLE_JSON_LOG", "true")),
        help="Enable JSON structured logging (default: true)",
    )
    
    parser.add_argument(
        "--service-name",
        default=os.getenv("MCP_SERVICE_NAME", "materialize_mcp_server"),
        help="Service name for logging and metrics (default: materialize_mcp_server)",
    )
    
    parser.add_argument(
        "--version",
        default=os.getenv("MCP_VERSION", "1.0.0"),
        help="Service version (default: 1.0.0)",
    )

    try:
        args = parser.parse_args()
    except SystemExit as e:
        # Provide better error handling for argument parsing
        if e.code != 0:
            raise ValidationError("arguments", "Invalid command line arguments provided")
        raise
    
    try:
        config = Config(
            dsn=args.mz_dsn,
            transport=args.transport,
            host=args.host,
            port=_safe_int(str(args.port)),
            pool_min_size=_safe_int(str(args.pool_min_size)),
            pool_max_size=_safe_int(str(args.pool_max_size)),
            log_level=args.log_level.upper(),
            enable_json_logging=args.enable_json_logging,
            service_name=args.service_name,
            version=args.version,
        )
        
        # Validate the configuration
        config.validate()
        return config
        
    except ValidationError:
        raise
    except Exception as e:
        raise ValidationError("configuration", f"Failed to create configuration: {e}")


def _safe_int(value: str, default: int = 0) -> int:
    """Safely convert string to int with fallback."""
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def _safe_bool(value: str, default: bool = False) -> bool:
    """Safely convert string to bool with fallback."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ('true', '1', 'yes', 'on', 'enabled')
    return default
