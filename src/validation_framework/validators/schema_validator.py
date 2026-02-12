"""
Schema validator implementation.
Validates that source and target schemas match.
"""
from typing import Dict, List, Any

from ..core.base_validator import BaseValidator
from ..core.base_connector import BaseConnector
from ..models.validation_result import ValidationResult
from ..models.enums import ValidationType, ValidationStatus


class SchemaValidator(BaseValidator):
    """Validates schema compatibility between source and target."""

    def get_validation_type(self) -> ValidationType:
        """Return validation type."""
        return ValidationType.SCHEMA

    def _execute_validation(self, source_connector: BaseConnector,
                          target_connector: BaseConnector) -> ValidationResult:
        """
        Execute schema validation.

        Args:
            source_connector: Source data connector
            target_connector: Target data connector

        Returns:
            ValidationResult with schema comparison
        """
        # Get table/query references
        source_ref = self.config.get('source_table') or self.config.get('source_query')
        target_ref = self.config.get('target_table') or self.config.get('target_query')

        if not source_ref or not target_ref:
            return self._create_result(
                status=ValidationStatus.ERROR,
                source_connector=source_connector,
                target_connector=target_connector,
                error_message="source_table and target_table (or source_query/target_query) are required"
            )

        # Get schemas
        self.logger.info(f"Getting schema from {source_connector.name}")
        source_schema = self._get_schema(source_connector, source_ref)

        self.logger.info(f"Getting schema from {target_connector.name}")
        target_schema = self._get_schema(target_connector, target_ref)

        # Compare schemas
        differences = self._compare_schemas(source_schema, target_schema)

        # Determine status
        status = self._determine_status(differences)

        return self._create_result(
            status=status,
            source_connector=source_connector,
            target_connector=target_connector,
            schema_differences=differences,
            metadata={
                'source_columns': len(source_schema),
                'target_columns': len(target_schema),
                'source_schema': source_schema,
                'target_schema': target_schema
            }
        )

    def _get_schema(self, connector: BaseConnector, reference: str) -> Dict[str, str]:
        """Get schema from connector."""
        if 'SELECT' in reference.upper():
            # For queries, read a sample and infer schema
            df = connector.read_data(reference, limit=1)
            return {col: str(dtype) for col, dtype in df.dtypes.items()}
        else:
            # For tables, use get_schema
            return connector.get_schema(reference)

    def _compare_schemas(self, source_schema: Dict[str, str],
                        target_schema: Dict[str, str]) -> List[str]:
        """
        Compare two schemas and return list of differences.

        Args:
            source_schema: Source schema dictionary
            target_schema: Target schema dictionary

        Returns:
            List of difference descriptions
        """
        differences = []

        source_cols = set(source_schema.keys())
        target_cols = set(target_schema.keys())

        # Check for missing columns in target
        missing_in_target = source_cols - target_cols
        if missing_in_target:
            for col in sorted(missing_in_target):
                differences.append(f"Column '{col}' exists in source but missing in target")

        # Check for extra columns in target
        extra_in_target = target_cols - source_cols
        if extra_in_target:
            for col in sorted(extra_in_target):
                differences.append(f"Column '{col}' exists in target but missing in source")

        # Check for type mismatches in common columns
        common_cols = source_cols & target_cols
        for col in sorted(common_cols):
            source_type = self._normalize_type(source_schema[col])
            target_type = self._normalize_type(target_schema[col])

            if source_type != target_type:
                differences.append(
                    f"Column '{col}' type mismatch: source={source_schema[col]}, "
                    f"target={target_schema[col]}"
                )

        return differences

    def _normalize_type(self, data_type: str) -> str:
        """
        Normalize data type for comparison.

        Different systems use different type names for similar types.
        This method maps them to common types.
        """
        type_lower = data_type.lower()

        # Map to normalized types
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
            return 'string'  # Pandas object type usually contains strings
        else:
            return type_lower

    def _determine_status(self, differences: List[str]) -> ValidationStatus:
        """
        Determine validation status based on schema differences.

        Args:
            differences: List of schema differences

        Returns:
            ValidationStatus
        """
        if not differences:
            self.logger.info("Schemas match perfectly")
            return ValidationStatus.PASSED

        # Check if differences are in critical columns
        critical_columns = self.config.get('metadata', {}).get('critical_columns', [])

        if critical_columns:
            for diff in differences:
                for col in critical_columns:
                    if f"'{col}'" in diff:
                        self.logger.error(f"Critical column affected: {diff}")
                        return ValidationStatus.FAILED

        # Non-critical differences result in warning
        self.logger.warning(f"Found {len(differences)} schema differences")
        for diff in differences:
            self.logger.warning(f"  - {diff}")

        return ValidationStatus.WARNING
