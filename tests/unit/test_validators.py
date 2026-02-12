"""
Unit tests for validators.
"""
import pytest
from unittest.mock import Mock, MagicMock
import pandas as pd

from src.validation_framework.validators.row_count_validator import RowCountValidator
from src.validation_framework.validators.data_quality_validator import DataQualityValidator
from src.validation_framework.validators.schema_validator import SchemaValidator
from src.validation_framework.models.enums import ValidationStatus, ValidationType


class TestRowCountValidator:
    """Tests for RowCountValidator."""

    def test_row_count_match(self):
        """Test validation when row counts match exactly."""
        config = {
            'name': 'Test Row Count',
            'type': 'row_count',
            'source_query': 'SELECT COUNT(*) FROM source',
            'target_query': 'SELECT COUNT(*) FROM target',
            'thresholds': {}
        }

        validator = RowCountValidator(config)

        # Mock connectors
        source_connector = Mock()
        source_connector.name = 'source'
        source_connector.get_row_count.return_value = 1000

        target_connector = Mock()
        target_connector.name = 'target'
        target_connector.get_row_count.return_value = 1000

        # Execute validation
        result = validator._execute_validation(source_connector, target_connector)

        # Assertions
        assert result.status == ValidationStatus.PASSED
        assert result.source_count == 1000
        assert result.target_count == 1000
        assert result.difference == 0
        assert result.difference_percent == 0.0

    def test_row_count_within_threshold(self):
        """Test validation when difference is within threshold."""
        config = {
            'name': 'Test Row Count',
            'type': 'row_count',
            'source_query': 'SELECT COUNT(*) FROM source',
            'target_query': 'SELECT COUNT(*) FROM target',
            'thresholds': {
                'max_difference_percent': 1.0
            }
        }

        validator = RowCountValidator(config)

        source_connector = Mock()
        source_connector.name = 'source'
        source_connector.get_row_count.return_value = 1000

        target_connector = Mock()
        target_connector.name = 'target'
        target_connector.get_row_count.return_value = 1005  # 0.5% difference

        result = validator._execute_validation(source_connector, target_connector)

        assert result.status in [ValidationStatus.PASSED, ValidationStatus.WARNING]
        assert result.difference_percent == 0.5

    def test_row_count_exceeds_threshold(self):
        """Test validation when difference exceeds threshold."""
        config = {
            'name': 'Test Row Count',
            'type': 'row_count',
            'source_query': 'SELECT COUNT(*) FROM source',
            'target_query': 'SELECT COUNT(*) FROM target',
            'thresholds': {
                'max_difference_percent': 1.0
            }
        }

        validator = RowCountValidator(config)

        source_connector = Mock()
        source_connector.name = 'source'
        source_connector.get_row_count.return_value = 1000

        target_connector = Mock()
        target_connector.name = 'target'
        target_connector.get_row_count.return_value = 1100  # 10% difference

        result = validator._execute_validation(source_connector, target_connector)

        assert result.status == ValidationStatus.FAILED
        assert result.difference_percent == 10.0


class TestDataQualityValidator:
    """Tests for DataQualityValidator."""

    def test_no_quality_issues(self):
        """Test validation when data quality is perfect."""
        config = {
            'name': 'Test Quality',
            'type': 'data_quality',
            'source_query': 'SELECT * FROM source',
            'target_query': 'SELECT * FROM target',
            'thresholds': {}
        }

        validator = DataQualityValidator(config)

        # Create mock data without issues
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['A', 'B', 'C'],
            'value': [100, 200, 300]
        })

        source_connector = Mock()
        source_connector.name = 'source'
        source_connector.read_data.return_value = df

        target_connector = Mock()
        target_connector.name = 'target'
        target_connector.read_data.return_value = df

        result = validator._execute_validation(source_connector, target_connector)

        assert result.status == ValidationStatus.PASSED
        assert result.null_count == 0
        assert result.duplicate_count == 0

    def test_nulls_within_threshold(self):
        """Test validation with nulls within threshold."""
        config = {
            'name': 'Test Quality',
            'type': 'data_quality',
            'source_query': 'SELECT * FROM source',
            'target_query': 'SELECT * FROM target',
            'thresholds': {
                'max_null_percent': 10.0
            }
        }

        validator = DataQualityValidator(config)

        source_df = pd.DataFrame({'id': [1, 2, 3], 'name': ['A', 'B', 'C']})
        target_df = pd.DataFrame({'id': [1, 2, 3], 'name': ['A', None, 'C']})  # 1 null

        source_connector = Mock()
        source_connector.name = 'source'
        source_connector.read_data.return_value = source_df

        target_connector = Mock()
        target_connector.name = 'target'
        target_connector.read_data.return_value = target_df

        result = validator._execute_validation(source_connector, target_connector)

        assert result.status in [ValidationStatus.PASSED, ValidationStatus.WARNING]
        assert result.null_count > 0


class TestSchemaValidator:
    """Tests for SchemaValidator."""

    def test_schemas_match(self):
        """Test validation when schemas match."""
        config = {
            'name': 'Test Schema',
            'type': 'schema',
            'source_table': 'source_table',
            'target_table': 'target_table',
            'thresholds': {}
        }

        validator = SchemaValidator(config)

        schema = {'id': 'int', 'name': 'string', 'value': 'double'}

        source_connector = Mock()
        source_connector.name = 'source'
        source_connector.get_schema.return_value = schema

        target_connector = Mock()
        target_connector.name = 'target'
        target_connector.get_schema.return_value = schema

        result = validator._execute_validation(source_connector, target_connector)

        assert result.status == ValidationStatus.PASSED
        assert len(result.schema_differences) == 0

    def test_schema_missing_column(self):
        """Test validation when target is missing a column."""
        config = {
            'name': 'Test Schema',
            'type': 'schema',
            'source_table': 'source_table',
            'target_table': 'target_table',
            'thresholds': {}
        }

        validator = SchemaValidator(config)

        source_schema = {'id': 'int', 'name': 'string', 'value': 'double'}
        target_schema = {'id': 'int', 'name': 'string'}  # Missing 'value'

        source_connector = Mock()
        source_connector.name = 'source'
        source_connector.get_schema.return_value = source_schema

        target_connector = Mock()
        target_connector.name = 'target'
        target_connector.get_schema.return_value = target_schema

        result = validator._execute_validation(source_connector, target_connector)

        assert result.status in [ValidationStatus.WARNING, ValidationStatus.FAILED]
        assert len(result.schema_differences) > 0
        assert any('value' in diff for diff in result.schema_differences)

    def test_schema_type_mismatch(self):
        """Test validation when data types don't match."""
        config = {
            'name': 'Test Schema',
            'type': 'schema',
            'source_table': 'source_table',
            'target_table': 'target_table',
            'thresholds': {}
        }

        validator = SchemaValidator(config)

        source_schema = {'id': 'int', 'name': 'string'}
        target_schema = {'id': 'string', 'name': 'string'}  # id is string instead of int

        source_connector = Mock()
        source_connector.name = 'source'
        source_connector.get_schema.return_value = source_schema

        target_connector = Mock()
        target_connector.name = 'target'
        target_connector.get_schema.return_value = target_schema

        result = validator._execute_validation(source_connector, target_connector)

        assert len(result.schema_differences) > 0
        assert any('type mismatch' in diff.lower() for diff in result.schema_differences)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
