"""
Unit tests for configuration loader.
"""
import pytest
import tempfile
from pathlib import Path

from src.validation_framework.core.config_loader import ConfigLoader
from src.validation_framework.core.exceptions import ConfigurationError


class TestConfigLoader:
    """Tests for ConfigLoader."""

    def test_load_valid_config(self):
        """Test loading a valid configuration."""
        config_yaml = """
connections:
  - name: test_spark
    type: spark
    enabled: true
    config:
      master: local[*]
      app_name: Test

validations:
  - name: Test Validation
    type: row_count
    enabled: true
    source: test_spark
    target: test_spark
    source_query: "SELECT COUNT(*) FROM table1"
    target_query: "SELECT COUNT(*) FROM table2"

reporters:
  - type: console
    enabled: true

settings:
  log_level: INFO
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(config_yaml)
            config_path = f.name

        try:
            loader = ConfigLoader(config_path)
            config = loader.load()

            assert len(config.connections) == 1
            assert len(config.validations) == 1
            assert len(config.reporters) == 1
            assert config.settings.log_level == 'INFO'
            assert config.connections[0].name == 'test_spark'
            assert config.validations[0].name == 'Test Validation'

        finally:
            Path(config_path).unlink()

    def test_load_nonexistent_file(self):
        """Test loading from non-existent file."""
        with pytest.raises(ConfigurationError) as exc_info:
            ConfigLoader('/nonexistent/config.yaml')

        assert 'not found' in str(exc_info.value).lower()

    def test_env_variable_substitution(self):
        """Test environment variable substitution."""
        import os

        os.environ['TEST_HOST'] = 'testhost'
        os.environ['TEST_PORT'] = '9999'

        config_yaml = """
connections:
  - name: test_conn
    type: hive
    enabled: true
    config:
      host: "${TEST_HOST}"
      port: ${TEST_PORT}

validations:
  - name: Test
    type: row_count
    enabled: true
    source: test_conn
    target: test_conn
    source_query: "SELECT 1"
    target_query: "SELECT 1"

reporters:
  - type: console
    enabled: true
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(config_yaml)
            config_path = f.name

        try:
            loader = ConfigLoader(config_path)
            config = loader.load()

            assert config.connections[0].config['host'] == 'testhost'
            assert config.connections[0].config['port'] == 9999

        finally:
            Path(config_path).unlink()
            del os.environ['TEST_HOST']
            del os.environ['TEST_PORT']

    def test_invalid_validation_type(self):
        """Test configuration with invalid validation type."""
        config_yaml = """
connections:
  - name: test_conn
    type: spark
    enabled: true
    config:
      master: local[*]

validations:
  - name: Test
    type: invalid_type
    enabled: true
    source: test_conn
    target: test_conn
    source_query: "SELECT 1"
    target_query: "SELECT 1"

reporters:
  - type: console
    enabled: true
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(config_yaml)
            config_path = f.name

        try:
            loader = ConfigLoader(config_path)
            with pytest.raises(ConfigurationError):
                loader.load()

        finally:
            Path(config_path).unlink()

    def test_missing_connection_reference(self):
        """Test validation referencing non-existent connection."""
        config_yaml = """
connections:
  - name: test_conn
    type: spark
    enabled: true
    config:
      master: local[*]

validations:
  - name: Test
    type: row_count
    enabled: true
    source: test_conn
    target: nonexistent_conn
    source_query: "SELECT 1"
    target_query: "SELECT 1"

reporters:
  - type: console
    enabled: true
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(config_yaml)
            config_path = f.name

        try:
            loader = ConfigLoader(config_path)
            with pytest.raises(ConfigurationError) as exc_info:
                loader.load()

            assert 'unknown' in str(exc_info.value).lower()

        finally:
            Path(config_path).unlink()

    def test_empty_config_file(self):
        """Test loading empty configuration file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write('')
            config_path = f.name

        try:
            loader = ConfigLoader(config_path)
            with pytest.raises(ConfigurationError) as exc_info:
                loader.load()

            assert 'empty' in str(exc_info.value).lower()

        finally:
            Path(config_path).unlink()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
