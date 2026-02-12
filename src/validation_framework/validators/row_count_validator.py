"""
Row count validator implementation.
Validates that source and target have matching row counts within threshold.
"""
from typing import Dict, Any

from ..core.base_validator import BaseValidator
from ..core.base_connector import BaseConnector
from ..models.validation_result import ValidationResult
from ..models.enums import ValidationType, ValidationStatus
from ..utils.helpers import safe_divide


class RowCountValidator(BaseValidator):
    """Validates row counts between source and target."""

    def get_validation_type(self) -> ValidationType:
        """Return validation type."""
        return ValidationType.ROW_COUNT

    def _execute_validation(self, source_connector: BaseConnector,
                          target_connector: BaseConnector) -> ValidationResult:
        """
        Execute row count validation.

        Args:
            source_connector: Source data connector
            target_connector: Target data connector

        Returns:
            ValidationResult with row count comparison
        """
        # Get queries or table names
        source_query = self.config.get('source_query') or self.config.get('source_table')
        target_query = self.config.get('target_query') or self.config.get('target_table')

        if not source_query or not target_query:
            return self._create_result(
                status=ValidationStatus.ERROR,
                source_connector=source_connector,
                target_connector=target_connector,
                error_message="source_query/source_table and target_query/target_table are required"
            )

        # Get row counts
        self.logger.info(f"Getting source row count from {source_connector.name}")

        if 'SELECT' in source_query.upper():
            # It's a query, create a temp view and count
            source_df = source_connector.read_data(source_query)
            source_count = len(source_df)
        else:
            # It's a table name
            source_count = source_connector.get_row_count(source_query)

        self.logger.info(f"Source count: {source_count:,}")

        self.logger.info(f"Getting target row count from {target_connector.name}")

        if 'SELECT' in target_query.upper():
            target_df = target_connector.read_data(target_query)
            target_count = len(target_df)
        else:
            target_count = target_connector.get_row_count(target_query)

        self.logger.info(f"Target count: {target_count:,}")

        # Calculate difference
        difference = target_count - source_count
        difference_percent = safe_divide(abs(difference), source_count, 0.0) * 100

        # Check thresholds
        status = self._determine_status(
            source_count, target_count, difference, difference_percent
        )

        return self._create_result(
            status=status,
            source_connector=source_connector,
            target_connector=target_connector,
            source_count=source_count,
            target_count=target_count,
            difference=difference,
            difference_percent=difference_percent
        )

    def _determine_status(self, source_count: int, target_count: int,
                         difference: int, difference_percent: float) -> ValidationStatus:
        """
        Determine validation status based on counts and thresholds.

        Args:
            source_count: Source row count
            target_count: Target row count
            difference: Absolute difference
            difference_percent: Percentage difference

        Returns:
            ValidationStatus
        """
        # Check for zero counts
        fail_on_zero_source = self.thresholds.get('fail_on_zero_source', False)
        fail_on_zero_target = self.thresholds.get('fail_on_zero_target', False)

        if source_count == 0 and fail_on_zero_source:
            self.logger.warning("Source count is zero and fail_on_zero_source is enabled")
            return ValidationStatus.FAILED

        if target_count == 0 and fail_on_zero_target:
            self.logger.warning("Target count is zero and fail_on_zero_target is enabled")
            return ValidationStatus.FAILED

        # Check percentage threshold
        max_diff_percent = self.thresholds.get('max_difference_percent')
        if max_diff_percent is not None and difference_percent > max_diff_percent:
            self.logger.warning(
                f"Difference {difference_percent:.2f}% exceeds threshold {max_diff_percent}%"
            )
            return ValidationStatus.FAILED

        # Check absolute threshold
        max_diff_absolute = self.thresholds.get('max_difference_absolute')
        if max_diff_absolute is not None and abs(difference) > max_diff_absolute:
            self.logger.warning(
                f"Absolute difference {abs(difference):,} exceeds threshold {max_diff_absolute:,}"
            )
            return ValidationStatus.FAILED

        # Check if counts match exactly
        if source_count == target_count:
            self.logger.info("Row counts match exactly")
            return ValidationStatus.PASSED

        # Within threshold but not exact match
        if difference_percent > 0:
            self.logger.info(
                f"Row counts differ by {difference_percent:.2f}% but within threshold"
            )
            return ValidationStatus.WARNING

        return ValidationStatus.PASSED
