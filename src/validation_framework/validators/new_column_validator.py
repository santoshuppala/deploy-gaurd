"""
New Column Validator implementation.
Validates newly added columns with specific rules and constraints.
"""
from typing import Dict, List, Any, Optional
import pandas as pd

from ..core.base_validator import BaseValidator
from ..core.base_connector import BaseConnector
from ..models.validation_result import ValidationResult
from ..models.enums import ValidationType, ValidationStatus
from ..utils.helpers import safe_divide


class NewColumnValidator(BaseValidator):
    """Validates newly added columns in target data."""

    def get_validation_type(self) -> ValidationType:
        """Return validation type."""
        return ValidationType.SCHEMA  # Reuse schema type or extend enums

    def _execute_validation(self, source_connector: BaseConnector,
                          target_connector: BaseConnector) -> ValidationResult:
        """
        Execute new column validation.

        Validates:
        1. New columns exist in target
        2. New columns have appropriate data types
        3. New columns meet nullability requirements
        4. New columns have valid values (if specified)
        5. New columns have default values where required

        Args:
            source_connector: Source data connector
            target_connector: Target data connector

        Returns:
            ValidationResult with new column validation outcome
        """
        # Get configuration
        source_ref = self.config.get('source_table') or self.config.get('source_query')
        target_ref = self.config.get('target_table') or self.config.get('target_query')
        new_columns_config = self.config.get('metadata', {}).get('new_columns', [])

        if not source_ref or not target_ref:
            return self._create_result(
                status=ValidationStatus.ERROR,
                source_connector=source_connector,
                target_connector=target_connector,
                error_message="source and target references are required"
            )

        if not new_columns_config:
            return self._create_result(
                status=ValidationStatus.ERROR,
                source_connector=source_connector,
                target_connector=target_connector,
                error_message="new_columns configuration is required in metadata"
            )

        # Get schemas
        self.logger.info(f"Getting schema from {source_connector.name}")
        source_schema = self._get_schema(source_connector, source_ref)

        self.logger.info(f"Getting schema from {target_connector.name}")
        target_schema = self._get_schema(target_connector, target_ref)

        # Read sample data from target to validate new columns
        self.logger.info(f"Reading sample data from target for validation")
        target_data = self._read_sample_data(target_connector, target_ref)

        # Validate each new column
        validation_results = []
        all_passed = True

        for col_config in new_columns_config:
            col_name = col_config['name']
            result = self._validate_new_column(
                col_name, col_config, source_schema, target_schema, target_data
            )
            validation_results.append(result)
            if not result['passed']:
                all_passed = False

        # Determine overall status
        if all_passed:
            status = ValidationStatus.PASSED
            message = f"All {len(validation_results)} new columns validated successfully"
        else:
            failed_count = sum(1 for r in validation_results if not r['passed'])
            status = ValidationStatus.FAILED
            message = f"{failed_count}/{len(validation_results)} new column validations failed"

        self.logger.info(message)

        return self._create_result(
            status=status,
            source_connector=source_connector,
            target_connector=target_connector,
            metadata={
                'validation_results': validation_results,
                'summary': message,
                'new_columns_count': len(validation_results),
                'passed_count': sum(1 for r in validation_results if r['passed']),
                'failed_count': sum(1 for r in validation_results if not r['passed'])
            }
        )

    def _validate_new_column(self, col_name: str, col_config: Dict,
                            source_schema: Dict, target_schema: Dict,
                            target_data: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate a single new column.

        Args:
            col_name: Column name
            col_config: Column configuration
            source_schema: Source schema
            target_schema: Target schema
            target_data: Sample target data

        Returns:
            Dictionary with validation results
        """
        checks_passed = []
        checks_failed = []

        # Check 1: Column should NOT exist in source
        if col_name in source_schema:
            checks_failed.append(f"Column '{col_name}' exists in source (expected only in target)")
        else:
            checks_passed.append(f"Column '{col_name}' correctly absent from source")

        # Check 2: Column MUST exist in target
        if col_name not in target_schema:
            checks_failed.append(f"Column '{col_name}' missing from target")
            return {
                'column': col_name,
                'passed': False,
                'checks_passed': checks_passed,
                'checks_failed': checks_failed
            }
        else:
            checks_passed.append(f"Column '{col_name}' exists in target")

        # Check 3: Data type validation
        expected_type = col_config.get('expected_type')
        if expected_type:
            actual_type = str(target_schema[col_name])
            if self._normalize_type(actual_type) == self._normalize_type(expected_type):
                checks_passed.append(f"Data type matches: {actual_type}")
            else:
                checks_failed.append(
                    f"Data type mismatch: expected {expected_type}, got {actual_type}"
                )

        # Check 4: Nullability validation
        nullable = col_config.get('nullable', True)
        if col_name in target_data.columns:
            null_count = target_data[col_name].isnull().sum()
            null_percent = safe_divide(null_count, len(target_data), 0.0) * 100

            if not nullable and null_count > 0:
                checks_failed.append(
                    f"Column should be NOT NULL but has {null_count} nulls ({null_percent:.1f}%)"
                )
            else:
                checks_passed.append(
                    f"Nullability check passed: {null_count} nulls ({null_percent:.1f}%)"
                )

            # Check for excessive nulls even if nullable
            max_null_percent = col_config.get('max_null_percent')
            if max_null_percent is not None and null_percent > max_null_percent:
                checks_failed.append(
                    f"Null percentage {null_percent:.1f}% exceeds threshold {max_null_percent}%"
                )

        # Check 5: Default value validation
        default_value = col_config.get('default_value')
        if default_value is not None and col_name in target_data.columns:
            non_null_data = target_data[col_name].dropna()
            if len(non_null_data) > 0:
                value_counts = non_null_data.value_counts()
                default_count = value_counts.get(default_value, 0)
                default_percent = safe_divide(default_count, len(non_null_data), 0.0) * 100

                min_default_percent = col_config.get('min_default_percent', 0)
                if default_percent >= min_default_percent:
                    checks_passed.append(
                        f"Default value '{default_value}' present in {default_percent:.1f}% of records"
                    )
                else:
                    checks_failed.append(
                        f"Default value '{default_value}' only in {default_percent:.1f}% "
                        f"(expected >= {min_default_percent}%)"
                    )

        # Check 6: Value range validation (for numeric columns)
        min_value = col_config.get('min_value')
        max_value = col_config.get('max_value')
        if (min_value is not None or max_value is not None) and col_name in target_data.columns:
            try:
                numeric_data = pd.to_numeric(target_data[col_name], errors='coerce').dropna()
                if len(numeric_data) > 0:
                    actual_min = numeric_data.min()
                    actual_max = numeric_data.max()

                    if min_value is not None and actual_min < min_value:
                        checks_failed.append(
                            f"Min value {actual_min} below threshold {min_value}"
                        )
                    elif min_value is not None:
                        checks_passed.append(f"Min value {actual_min} >= {min_value}")

                    if max_value is not None and actual_max > max_value:
                        checks_failed.append(
                            f"Max value {actual_max} exceeds threshold {max_value}"
                        )
                    elif max_value is not None:
                        checks_passed.append(f"Max value {actual_max} <= {max_value}")
            except Exception as e:
                checks_failed.append(f"Error validating value range: {str(e)}")

        # Check 7: Allowed values validation (for categorical columns)
        allowed_values = col_config.get('allowed_values')
        if allowed_values and col_name in target_data.columns:
            unique_values = target_data[col_name].dropna().unique()
            invalid_values = [v for v in unique_values if v not in allowed_values]

            if invalid_values:
                checks_failed.append(
                    f"Invalid values found: {invalid_values[:5]}... "
                    f"(total {len(invalid_values)} invalid)"
                )
            else:
                checks_passed.append(
                    f"All values are from allowed set ({len(unique_values)} unique values)"
                )

        # Check 8: Pattern validation (for string columns)
        pattern = col_config.get('pattern')
        if pattern and col_name in target_data.columns:
            try:
                import re
                string_data = target_data[col_name].dropna().astype(str)
                if len(string_data) > 0:
                    matches = string_data.str.match(pattern)
                    match_percent = safe_divide(matches.sum(), len(string_data), 0.0) * 100

                    min_match_percent = col_config.get('min_pattern_match_percent', 100)
                    if match_percent >= min_match_percent:
                        checks_passed.append(
                            f"Pattern match: {match_percent:.1f}% of values match pattern"
                        )
                    else:
                        checks_failed.append(
                            f"Pattern match: only {match_percent:.1f}% match "
                            f"(expected >= {min_match_percent}%)"
                        )
            except Exception as e:
                checks_failed.append(f"Error validating pattern: {str(e)}")

        # Compile results
        passed = len(checks_failed) == 0

        return {
            'column': col_name,
            'passed': passed,
            'checks_passed': checks_passed,
            'checks_failed': checks_failed,
            'total_checks': len(checks_passed) + len(checks_failed)
        }

    def _get_schema(self, connector: BaseConnector, reference: str) -> Dict[str, str]:
        """Get schema from connector."""
        if 'SELECT' in reference.upper():
            df = connector.read_data(reference, limit=1)
            return {col: str(dtype) for col, dtype in df.dtypes.items()}
        else:
            return connector.get_schema(reference)

    def _read_sample_data(self, connector: BaseConnector, reference: str,
                         sample_size: int = 1000) -> pd.DataFrame:
        """Read sample data for validation."""
        if 'SELECT' in reference.upper():
            return connector.read_data(reference, limit=sample_size)
        else:
            query = f"SELECT * FROM {reference}"
            return connector.read_data(query, limit=sample_size)

    def _normalize_type(self, data_type: str) -> str:
        """Normalize data type for comparison."""
        type_lower = data_type.lower()

        if 'int' in type_lower or 'long' in type_lower or 'bigint' in type_lower:
            return 'integer'
        elif 'float' in type_lower or 'double' in type_lower or 'decimal' in type_lower:
            return 'numeric'
        elif 'string' in type_lower or 'varchar' in type_lower or 'char' in type_lower or 'text' in type_lower:
            return 'string'
        elif 'bool' in type_lower:
            return 'boolean'
        elif 'date' in type_lower:
            return 'date'
        elif 'timestamp' in type_lower or 'datetime' in type_lower:
            return 'timestamp'
        elif 'object' in type_lower:
            return 'string'
        else:
            return type_lower
