"""
Data quality validator implementation.
Validates data quality metrics like nulls, duplicates, and invalid records.
"""
from typing import Dict, Any, List
import pandas as pd

from ..core.base_validator import BaseValidator
from ..core.base_connector import BaseConnector
from ..models.validation_result import ValidationResult
from ..models.enums import ValidationType, ValidationStatus
from ..utils.helpers import safe_divide


class DataQualityValidator(BaseValidator):
    """Validates data quality metrics."""

    def get_validation_type(self) -> ValidationType:
        """Return validation type."""
        return ValidationType.DATA_QUALITY

    def _execute_validation(self, source_connector: BaseConnector,
                          target_connector: BaseConnector) -> ValidationResult:
        """
        Execute data quality validation.

        Args:
            source_connector: Source data connector
            target_connector: Target data connector

        Returns:
            ValidationResult with quality metrics
        """
        # Get queries
        source_query = self.config.get('source_query') or self.config.get('source_table')
        target_query = self.config.get('target_query') or self.config.get('target_table')

        if not source_query or not target_query:
            return self._create_result(
                status=ValidationStatus.ERROR,
                source_connector=source_connector,
                target_connector=target_connector,
                error_message="source_query/source_table and target_query/target_table are required"
            )

        # Read data from both sources
        self.logger.info(f"Reading data from {source_connector.name}")
        source_df = self._read_data(source_connector, source_query)

        self.logger.info(f"Reading data from {target_connector.name}")
        target_df = self._read_data(target_connector, target_query)

        # Calculate quality metrics for target
        null_count = self._count_nulls(target_df)
        duplicate_count = self._count_duplicates(target_df)
        invalid_count = self._count_invalid_records(target_df)

        total_rows = len(target_df)
        null_percent = safe_divide(null_count, total_rows, 0.0) * 100
        duplicate_percent = safe_divide(duplicate_count, total_rows, 0.0) * 100
        invalid_percent = safe_divide(invalid_count, total_rows, 0.0) * 100

        self.logger.info(
            f"Quality metrics - Nulls: {null_count} ({null_percent:.2f}%), "
            f"Duplicates: {duplicate_count} ({duplicate_percent:.2f}%), "
            f"Invalid: {invalid_count} ({invalid_percent:.2f}%)"
        )

        # Determine status based on thresholds
        status = self._determine_status(null_percent, duplicate_percent, invalid_percent)

        return self._create_result(
            status=status,
            source_connector=source_connector,
            target_connector=target_connector,
            source_count=len(source_df),
            target_count=len(target_df),
            null_count=null_count,
            duplicate_count=duplicate_count,
            invalid_count=invalid_count,
            metadata={
                'null_percent': round(null_percent, 2),
                'duplicate_percent': round(duplicate_percent, 2),
                'invalid_percent': round(invalid_percent, 2)
            }
        )

    def _read_data(self, connector: BaseConnector, query: str) -> pd.DataFrame:
        """Read data from connector."""
        if 'SELECT' in query.upper():
            return connector.read_data(query)
        else:
            # Assume it's a table name, read all data
            return connector.read_data(f"SELECT * FROM {query}")

    def _count_nulls(self, df: pd.DataFrame) -> int:
        """Count null values across all columns or specific columns."""
        check_columns = self.config.get('metadata', {}).get('check_columns')

        if check_columns:
            # Check specific columns
            return df[check_columns].isnull().sum().sum()
        else:
            # Check all columns
            return df.isnull().sum().sum()

    def _count_duplicates(self, df: pd.DataFrame) -> int:
        """Count duplicate rows based on primary key or all columns."""
        primary_key = self.config.get('metadata', {}).get('primary_key')

        if primary_key:
            # Check duplicates on primary key
            if isinstance(primary_key, str):
                primary_key = [primary_key]
            return df.duplicated(subset=primary_key).sum()
        else:
            # Check duplicates on all columns
            return df.duplicated().sum()

    def _count_invalid_records(self, df: pd.DataFrame) -> int:
        """
        Count invalid records based on validation rules.

        This is a placeholder - can be extended with specific validation logic.
        """
        invalid_count = 0

        # Example: Check for negative values in numeric columns
        numeric_columns = df.select_dtypes(include=['int64', 'float64']).columns
        for col in numeric_columns:
            if 'amount' in col.lower() or 'price' in col.lower() or 'quantity' in col.lower():
                invalid_count += (df[col] < 0).sum()

        # Example: Check for empty strings in important columns
        check_columns = self.config.get('metadata', {}).get('check_columns', [])
        for col in check_columns:
            if col in df.columns and df[col].dtype == 'object':
                invalid_count += (df[col].str.strip() == '').sum()

        return invalid_count

    def _determine_status(self, null_percent: float,
                         duplicate_percent: float,
                         invalid_percent: float) -> ValidationStatus:
        """
        Determine validation status based on quality metrics and thresholds.

        Args:
            null_percent: Percentage of null values
            duplicate_percent: Percentage of duplicates
            invalid_percent: Percentage of invalid records

        Returns:
            ValidationStatus
        """
        max_null = self.thresholds.get('max_null_percent')
        max_duplicate = self.thresholds.get('max_duplicate_percent')
        max_invalid = self.thresholds.get('max_invalid_percent')

        failed = False

        if max_null is not None and null_percent > max_null:
            self.logger.warning(f"Null percentage {null_percent:.2f}% exceeds threshold {max_null}%")
            failed = True

        if max_duplicate is not None and duplicate_percent > max_duplicate:
            self.logger.warning(
                f"Duplicate percentage {duplicate_percent:.2f}% exceeds threshold {max_duplicate}%"
            )
            failed = True

        if max_invalid is not None and invalid_percent > max_invalid:
            self.logger.warning(
                f"Invalid percentage {invalid_percent:.2f}% exceeds threshold {max_invalid}%"
            )
            failed = True

        if failed:
            return ValidationStatus.FAILED

        # Warning if any quality issues but within threshold
        if null_percent > 0 or duplicate_percent > 0 or invalid_percent > 0:
            return ValidationStatus.WARNING

        return ValidationStatus.PASSED
