"""
Input validation and security utilities for the Materialize MCP server.
"""
import re
from typing import Any, Dict, List, Optional
from dataclasses import dataclass


class ValidationError(Exception):
    """Raised when input validation fails."""
    def __init__(self, field: str, message: str):
        self.field = field
        self.message = message
        super().__init__(f"Validation error for '{field}': {message}")


@dataclass
class ValidationResult:
    """Result of input validation."""
    is_valid: bool
    errors: List[ValidationError]
    
    def add_error(self, field: str, message: str):
        """Add a validation error."""
        self.errors.append(ValidationError(field, message))
        self.is_valid = False


class InputValidator:
    """Validates input parameters for MCP tools."""
    
    # SQL identifier pattern (letters, digits, underscores, max 63 chars)
    IDENTIFIER_PATTERN = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]{0,62}$')
    
    # Cluster size pattern (digits + 'cc' or predefined sizes)
    CLUSTER_SIZE_PATTERN = re.compile(r'^(\d+cc|xsmall|small|medium|large|xlarge|2xlarge|3xlarge|4xlarge|5xlarge|6xlarge)$')
    
    # Schema name pattern (same as identifier but can have dots for qualified names)
    SCHEMA_PATTERN = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_.]{0,62}$')
    
    # Port range validation
    MIN_PORT = 1
    MAX_PORT = 65535
    
    # Connection pool size limits
    MIN_POOL_SIZE = 1
    MAX_POOL_SIZE = 100
    
    @staticmethod
    def validate_identifier(value: Any, field_name: str) -> ValidationResult:
        """Validate SQL identifier (table names, column names, etc.)."""
        result = ValidationResult(is_valid=True, errors=[])
        
        if not isinstance(value, str):
            result.add_error(field_name, f"must be a string, got {type(value).__name__}")
            return result
            
        if not value:
            result.add_error(field_name, "cannot be empty")
            return result
            
        if len(value) > 63:
            result.add_error(field_name, "cannot exceed 63 characters")
            return result
            
        if not InputValidator.IDENTIFIER_PATTERN.match(value):
            result.add_error(field_name, "must contain only letters, digits, and underscores, and start with a letter or underscore")
            return result
            
        # Check for SQL keywords that should be quoted
        sql_keywords = {
            'select', 'from', 'where', 'group', 'order', 'by', 'having', 'union',
            'insert', 'update', 'delete', 'create', 'drop', 'alter', 'table',
            'view', 'index', 'cluster', 'schema', 'database', 'user', 'role'
        }
        if value.lower() in sql_keywords:
            result.add_error(field_name, f"'{value}' is a reserved SQL keyword and must be quoted")
            
        return result
    
    @staticmethod
    def validate_cluster_size(value: Any, field_name: str = "cluster_size") -> ValidationResult:
        """Validate cluster size specification."""
        result = ValidationResult(is_valid=True, errors=[])
        
        if not isinstance(value, str):
            result.add_error(field_name, f"must be a string, got {type(value).__name__}")
            return result
            
        if not value:
            result.add_error(field_name, "cannot be empty")
            return result
            
        if not InputValidator.CLUSTER_SIZE_PATTERN.match(value):
            result.add_error(field_name, "must be either a number followed by 'cc' (e.g., '100cc') or a predefined size (xsmall, small, medium, large, xlarge, 2xlarge, 3xlarge, 4xlarge, 5xlarge, 6xlarge)")
            
        return result
    
    @staticmethod
    def validate_sql_statements(value: Any, field_name: str = "sql_statements") -> ValidationResult:
        """Validate SQL statements list."""
        result = ValidationResult(is_valid=True, errors=[])
        
        if not isinstance(value, list):
            result.add_error(field_name, f"must be a list, got {type(value).__name__}")
            return result
            
        if not value:
            result.add_error(field_name, "cannot be empty")
            return result
            
        if len(value) > 100:
            result.add_error(field_name, "cannot contain more than 100 statements")
            return result
            
        for i, stmt in enumerate(value):
            if not isinstance(stmt, str):
                result.add_error(f"{field_name}[{i}]", f"must be a string, got {type(stmt).__name__}")
                continue
                
            if len(stmt.strip()) == 0:
                continue  # Allow empty statements
                
            if len(stmt) > 100000:  # 100KB limit per statement
                result.add_error(f"{field_name}[{i}]", "statement too long (max 100KB)")
                
            # Basic SQL injection protection - detect dangerous patterns
            dangerous_patterns = [
                r';\s*(drop|delete|truncate)\s+',  # Dangerous statements after semicolon
                r'--.*?(drop|delete|truncate)',    # SQL injection attempts in comments
                r'/\*.*?(drop|delete|truncate).*?\*/',  # SQL injection in block comments
            ]
            
            stmt_lower = stmt.lower()
            for pattern in dangerous_patterns:
                if re.search(pattern, stmt_lower, re.IGNORECASE | re.DOTALL):
                    result.add_error(f"{field_name}[{i}]", "potentially dangerous SQL pattern detected")
                    
        return result
    
    @staticmethod
    def validate_schema_name(value: Any, field_name: str = "schema") -> ValidationResult:
        """Validate schema name (can be qualified with database)."""
        result = ValidationResult(is_valid=True, errors=[])
        
        if value is None:
            return result  # Optional field
            
        if not isinstance(value, str):
            result.add_error(field_name, f"must be a string, got {type(value).__name__}")
            return result
            
        if not value:
            result.add_error(field_name, "cannot be empty")
            return result
            
        if len(value) > 127:  # Allow longer for qualified names
            result.add_error(field_name, "cannot exceed 127 characters")
            return result
            
        if not InputValidator.SCHEMA_PATTERN.match(value):
            result.add_error(field_name, "must contain only letters, digits, underscores, and dots, and start with a letter or underscore")
            
        return result
    
    @staticmethod
    def validate_port(value: Any, field_name: str = "port") -> ValidationResult:
        """Validate port number."""
        result = ValidationResult(is_valid=True, errors=[])
        
        if not isinstance(value, int):
            result.add_error(field_name, f"must be an integer, got {type(value).__name__}")
            return result
            
        if value < InputValidator.MIN_PORT or value > InputValidator.MAX_PORT:
            result.add_error(field_name, f"must be between {InputValidator.MIN_PORT} and {InputValidator.MAX_PORT}")
            
        return result
    
    @staticmethod
    def validate_pool_size(value: Any, field_name: str = "pool_size") -> ValidationResult:
        """Validate connection pool size."""
        result = ValidationResult(is_valid=True, errors=[])
        
        if not isinstance(value, int):
            result.add_error(field_name, f"must be an integer, got {type(value).__name__}")
            return result
            
        if value < InputValidator.MIN_POOL_SIZE or value > InputValidator.MAX_POOL_SIZE:
            result.add_error(field_name, f"must be between {InputValidator.MIN_POOL_SIZE} and {InputValidator.MAX_POOL_SIZE}")
            
        return result
    
    @staticmethod
    def validate_threshold_seconds(value: Any, field_name: str = "threshold_seconds") -> ValidationResult:
        """Validate freshness threshold in seconds."""
        result = ValidationResult(is_valid=True, errors=[])
        
        if not isinstance(value, (int, float)):
            result.add_error(field_name, f"must be a number, got {type(value).__name__}")
            return result
            
        if value < 0:
            result.add_error(field_name, "must be non-negative")
            return result
            
        if value > 86400:  # 24 hours max
            result.add_error(field_name, "cannot exceed 86400 seconds (24 hours)")
            
        return result
    
    @staticmethod
    def validate_create_cluster_args(args: Dict[str, Any]) -> ValidationResult:
        """Validate arguments for create_cluster tool."""
        result = ValidationResult(is_valid=True, errors=[])
        
        # Validate cluster_name
        cluster_name = args.get("cluster_name")
        cluster_result = InputValidator.validate_identifier(cluster_name, "cluster_name")
        result.errors.extend(cluster_result.errors)
        if not cluster_result.is_valid:
            result.is_valid = False
            
        # Validate size
        size = args.get("size")
        size_result = InputValidator.validate_cluster_size(size, "size")
        result.errors.extend(size_result.errors)
        if not size_result.is_valid:
            result.is_valid = False
            
        return result
    
    @staticmethod
    def validate_run_sql_transaction_args(args: Dict[str, Any]) -> ValidationResult:
        """Validate arguments for run_sql_transaction tool."""
        result = ValidationResult(is_valid=True, errors=[])
        
        # Validate cluster_name
        cluster_name = args.get("cluster_name")
        cluster_result = InputValidator.validate_identifier(cluster_name, "cluster_name")
        result.errors.extend(cluster_result.errors)
        if not cluster_result.is_valid:
            result.is_valid = False
            
        # Validate sql_statements
        sql_statements = args.get("sql_statements")
        sql_result = InputValidator.validate_sql_statements(sql_statements, "sql_statements")
        result.errors.extend(sql_result.errors)
        if not sql_result.is_valid:
            result.is_valid = False
            
        # Validate isolation_level (optional)
        isolation_level = args.get("isolation_level")
        if isolation_level is not None:
            valid_levels = {
                'read uncommitted', 'read committed', 'repeatable read', 'serializable',
                'strict serializable'
            }
            if not isinstance(isolation_level, str) or isolation_level.lower() not in valid_levels:
                result.add_error("isolation_level", f"must be one of: {', '.join(valid_levels)}")
                
        return result


def sanitize_sql_identifier(identifier: str) -> str:
    """
    Safely quote and sanitize SQL identifiers.
    Returns properly quoted identifier for use in SQL queries.
    """
    # Remove any existing quotes
    clean_identifier = identifier.strip('"')
    
    # Quote the identifier to prevent SQL injection
    return f'"{clean_identifier}"'


def validate_and_sanitize_args(tool_name: str, args: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate and sanitize arguments for a specific tool.
    
    Args:
        tool_name: Name of the tool being called
        args: Arguments to validate
        
    Returns:
        Sanitized arguments
        
    Raises:
        ValidationError: If validation fails
    """
    if tool_name == "create_cluster":
        result = InputValidator.validate_create_cluster_args(args)
        if not result.is_valid:
            raise ValidationError("validation", f"Validation failed: {', '.join(str(e) for e in result.errors)}")
        return {
            "cluster_name": sanitize_sql_identifier(args["cluster_name"]),
            "size": args["size"]  # Size is validated but not sanitized as it's not used in dynamic SQL
        }
    
    elif tool_name == "run_sql_transaction":
        result = InputValidator.validate_run_sql_transaction_args(args)
        if not result.is_valid:
            raise ValidationError("validation", f"Validation failed: {', '.join(str(e) for e in result.errors)}")
        return {
            "cluster_name": sanitize_sql_identifier(args["cluster_name"]),
            "sql_statements": args["sql_statements"],  # SQL statements are validated but not modified
            "isolation_level": args.get("isolation_level")
        }
    
    # Add more tool validations as needed
    return args