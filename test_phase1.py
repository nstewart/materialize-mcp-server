#!/usr/bin/env python3
"""
Simple test script to verify Phase 1 enterprise improvements are working.
"""
import json
import subprocess
import sys
from pathlib import Path

def test_config_validation():
    """Test configuration validation works properly."""
    print("Testing configuration validation...")
    
    # Test invalid port
    result = subprocess.run([
        "uv", "run", "materialize-mcp",
        "--port", "99999"
    ], capture_output=True, text=True)
    assert result.returncode != 0, "Should fail with invalid port"
    assert "must be between 1 and 65535" in result.stderr, f"Expected port validation error, got: {result.stderr}"
    
    # Test invalid DSN
    result = subprocess.run([
        "uv", "run", "materialize-mcp",
        "--mz-dsn", "invalid-dsn"
    ], capture_output=True, text=True)
    assert result.returncode != 0, "Should fail with invalid DSN"
    assert "DSN must include a scheme" in result.stderr, f"Expected DSN validation error, got: {result.stderr}"
    
    print("✅ Configuration validation tests passed")

def test_help_output():
    """Test that help output includes new features."""
    print("Testing enhanced help output...")
    
    result = subprocess.run([
        "uv", "run", "materialize-mcp", "--help"
    ], capture_output=True, text=True)
    assert result.returncode == 0, "Help should succeed"
    
    # Check for new options
    help_text = result.stdout
    assert "--enable-json-logging" in help_text, "Should include JSON logging option"
    assert "--service-name" in help_text, "Should include service name option"
    assert "Environment Variables:" in help_text, "Should include environment variables section"
    
    print("✅ Enhanced help output test passed")

def test_validation_imports():
    """Test that validation modules can be imported."""
    print("Testing module imports...")
    
    try:
        from materialize_mcp_server.validation import InputValidator, ValidationError
        from materialize_mcp_server.logging_config import setup_structured_logging
        from materialize_mcp_server.observability import MetricsCollector
        from materialize_mcp_server.config import ConfigurationError
        print("✅ All Phase 1 modules import successfully")
    except ImportError as e:
        print(f"❌ Import error: {e}")
        sys.exit(1)

def test_input_validation():
    """Test input validation functions."""
    print("Testing input validation...")
    
    from materialize_mcp_server.validation import InputValidator, ValidationError
    
    # Test valid cluster name
    result = InputValidator.validate_identifier("my_cluster", "cluster_name")
    assert result.is_valid, "Valid cluster name should pass"
    
    # Test invalid cluster name
    result = InputValidator.validate_identifier("123invalid", "cluster_name")
    assert not result.is_valid, "Invalid cluster name should fail"
    
    # Test SQL injection patterns
    result = InputValidator.validate_sql_statements(["SELECT * FROM users; DROP TABLE users;"], "sql")
    assert not result.is_valid, "SQL injection pattern should be detected"
    
    print("✅ Input validation tests passed")

def test_logging_config():
    """Test structured logging configuration."""
    print("Testing logging configuration...")
    
    from materialize_mcp_server.logging_config import setup_structured_logging, generate_correlation_id
    
    # Test logger setup
    logger = setup_structured_logging(
        service_name="test_service",
        version="1.0.0",
        log_level="INFO",
        enable_json=False  # Use text format for testing
    )
    
    assert logger.name == "test_service", "Logger should have correct name"
    
    # Test correlation ID generation
    correlation_id = generate_correlation_id()
    assert len(correlation_id) > 0, "Should generate non-empty correlation ID"
    
    print("✅ Logging configuration tests passed")

def test_observability():
    """Test observability metrics collection."""
    print("Testing observability...")
    
    from materialize_mcp_server.observability import MetricsCollector, get_metrics_collector
    
    # Test metrics collector
    collector = get_metrics_collector()
    assert isinstance(collector, MetricsCollector), "Should return MetricsCollector instance"
    
    # Test metrics recording (async would require more setup)
    summary = collector.get_metrics_summary()
    assert "uptime_seconds" in summary, "Summary should include uptime"
    assert "tool_metrics" in summary, "Summary should include tool metrics"
    
    print("✅ Observability tests passed")

def main():
    """Run all Phase 1 tests."""
    print("🚀 Running Phase 1 Enterprise Improvements Tests\n")
    
    try:
        test_validation_imports()
        test_input_validation()
        test_logging_config()
        test_observability()
        test_help_output()
        test_config_validation()
        
        print("\n🎉 All Phase 1 tests passed! Enterprise improvements are working correctly.")
        print("\nPhase 1 improvements include:")
        print("✅ Input validation and SQL injection protection")
        print("✅ Structured logging with correlation IDs") 
        print("✅ Basic observability metrics and health checks")
        print("✅ Enhanced configuration validation")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()