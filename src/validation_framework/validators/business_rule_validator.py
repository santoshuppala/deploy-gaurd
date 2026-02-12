"""
Business rule validator implementation.
Validates custom business rules and aggregations between source and target.
"""
from typing import Dict, Any
import pandas as pd

from ..core.base_validator import BaseValidator
from ..core.base_connector import BaseConnector
from ..models.validation_result import ValidationResult
from ..models.enums import ValidationType, ValidationStatus
from ..utils.helpers import safe_divide


class BusinessRuleValidator(BaseValidator):
    """Validates business rules and custom logic."""

    def get_validation_type(self) -> ValidationType:
        """Return validation type."""
        return ValidationType.BUSINESS_RULE

    def _execute_validation(self, source_connector: BaseConnector,
                          target_connector: BaseConnector) -> ValidationResult:
        """
        Execute business rule validation.

        This validator is designed to validate aggregations and custom business logic
        by comparing query results between source and target.

        Args:
            source_connector: Source data connector
            target_connector: Target data connector

        Returns:
            ValidationResult with rule validation outcome
        """
        # Get queries
        source_query = self.config.get('source_query')
        target_query = self.config.get('target_query')

        if not source_query or not target_query:
            return self._create_result(
                status=ValidationStatus.ERROR,
                source_connector=source_connector,
                target_connector=target_connector,
                error_message="source_query and target_query are required for business rule validation"
            )

        # Execute queries
        self.logger.info(f"Executing source query on {source_connector.name}")
        source_df = source_connector.read_data(source_query)

        self.logger.info(f"Executing target query on {target_connector.name}")
        target_df = target_connector.read_data(target_query)

        # Get rule type
        rule_type = self.config.get('metadata', {}).get('rule_type', 'aggregation')

        # Execute appropriate validation based on rule type
        if rule_type == 'aggregation':
            result = self._validate_aggregation(
                source_df, target_df, source_connector, target_connector
            )
        elif rule_type == 'row_by_row':
            result = self._validate_row_by_row(
                source_df, target_df, source_connector, target_connector
            )
        else:
            result = self._validate_generic(
                source_df, target_df, source_connector, target_connector
            )

        return result

    def _validate_aggregation(self, source_df: pd.DataFrame, target_df: pd.DataFrame,
                            source_connector: BaseConnector,
                            target_connector: BaseConnector) -> ValidationResult:
        """
        Validate aggregation results (e.g., SUM, AVG, COUNT).

        Expects single-row results with numeric values.
        """
        if len(source_df) == 0 or len(target_df) == 0:
            return self._create_result(
                status=ValidationStatus.ERROR,
                source_connector=source_connector,
                target_connector=target_connector,
                error_message="One or both queries returned no results"
            )

        # Get first row, first column value from each
        source_value = source_df.iloc[0, 0]
        target_value = target_df.iloc[0, 0]

        # Handle None/NaN
        if pd.isna(source_value) or pd.isna(target_value):
            return self._create_result(
                status=ValidationStatus.ERROR,
                source_connector=source_connector,
                target_connector=target_connector,
                error_message="Query returned NULL/NaN value"
            )

        # Convert to float for comparison
        try:
            source_value = float(source_value)
            target_value = float(target_value)
        except (ValueError, TypeError):
            return self._create_result(
                status=ValidationStatus.ERROR,
                source_connector=source_connector,
                target_connector=target_connector,
                error_message=f"Cannot compare non-numeric values: {source_value} vs {target_value}"
            )

        # Calculate difference
        difference = target_value - source_value
        difference_percent = safe_divide(abs(difference), abs(source_value), 0.0) * 100

        self.logger.info(
            f"Aggregation comparison - Source: {source_value:,.2f}, "
            f"Target: {target_value:,.2f}, Difference: {difference:,.2f} ({difference_percent:.4f}%)"
        )

        # Check threshold
        max_diff_percent = self.thresholds.get('max_difference_percent')

        if max_diff_percent is not None and difference_percent > max_diff_percent:
            status = ValidationStatus.FAILED
            self.logger.warning(
                f"Difference {difference_percent:.4f}% exceeds threshold {max_diff_percent}%"
            )
        elif difference != 0:
            status = ValidationStatus.WARNING
            self.logger.info(f"Values differ by {difference_percent:.4f}% but within threshold")
        else:
            status = ValidationStatus.PASSED
            self.logger.info("Values match exactly")

        return self._create_result(
            status=status,
            source_connector=source_connector,
            target_connector=target_connector,
            rule_results={
                'source_value': source_value,
                'target_value': target_value,
                'difference': difference,
                'difference_percent': difference_percent
            }
        )

    def _validate_row_by_row(self, source_df: pd.DataFrame, target_df: pd.DataFrame,
                           source_connector: BaseConnector,
                           target_connector: BaseConnector) -> ValidationResult:
        """
        Validate row-by-row comparison.

        Compares each row between source and target.
        """
        if len(source_df) != len(target_df):
            return self._create_result(
                status=ValidationStatus.FAILED,
                source_connector=source_connector,
                target_connector=target_connector,
                source_count=len(source_df),
                target_count=len(target_df),
                error_message=f"Row count mismatch: source={len(source_df)}, target={len(target_df)}"
            )

        # Compare DataFrames
        # Sort both by all columns for consistent comparison
        source_sorted = source_df.sort_values(by=list(source_df.columns)).reset_index(drop=True)
        target_sorted = target_df.sort_values(by=list(target_df.columns)).reset_index(drop=True)

        # Check if DataFrames are equal
        if source_sorted.equals(target_sorted):
            status = ValidationStatus.PASSED
            mismatches = 0
        else:
            # Find differences
            comparison = source_sorted == target_sorted
            mismatches = (~comparison).sum().sum()
            status = ValidationStatus.FAILED

        self.logger.info(f"Row-by-row comparison: {mismatches} mismatches found")

        return self._create_result(
            status=status,
            source_connector=source_connector,
            target_connector=target_connector,
            source_count=len(source_df),
            target_count=len(target_df),
            rule_results={
                'mismatches': int(mismatches),
                'match_percent': (1 - mismatches / (len(source_df) * len(source_df.columns))) * 100
            }
        )

    def _validate_generic(self, source_df: pd.DataFrame, target_df: pd.DataFrame,
                        source_connector: BaseConnector,
                        target_connector: BaseConnector) -> ValidationResult:
        """
        Generic validation - checks if results are identical.
        """
        if source_df.equals(target_df):
            status = ValidationStatus.PASSED
            message = "Results match perfectly"
        else:
            status = ValidationStatus.FAILED
            message = "Results differ"

        self.logger.info(message)

        return self._create_result(
            status=status,
            source_connector=source_connector,
            target_connector=target_connector,
            source_count=len(source_df),
            target_count=len(target_df),
            rule_results={'message': message}
        )
