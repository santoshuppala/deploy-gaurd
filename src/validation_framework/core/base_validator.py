"""
Abstract base class for validators.
Defines the contract that all validator implementations must follow.
"""
from abc import ABC, abstractmethod
from typing import Dict, Any
from datetime import datetime
import time

from .base_connector import BaseConnector
from ..models.validation_result import ValidationResult
from ..models.enums import ValidationType, ValidationStatus
from ..utils.logger import get_logger


class BaseValidator(ABC):
    """Abstract base class for all validators."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize validator.

        Args:
            config: Validation configuration dictionary
        """
        self.config = config
        self.name = config.get('name', 'Unnamed Validation')
        self.logger = get_logger(f"{self.__class__.__name__}")
        self.thresholds = config.get('thresholds', {})

    @abstractmethod
    def get_validation_type(self) -> ValidationType:
        """
        Get the type of validation this validator performs.

        Returns:
            ValidationType enum value
        """
        pass

    @abstractmethod
    def _execute_validation(self, source_connector: BaseConnector,
                           target_connector: BaseConnector) -> ValidationResult:
        """
        Execute the actual validation logic.
        Must be implemented by subclasses.

        Args:
            source_connector: Source data connector
            target_connector: Target data connector

        Returns:
            ValidationResult with validation outcome

        Raises:
            ValidationError: If validation execution fails
        """
        pass

    def validate(self, source_connector: BaseConnector,
                target_connector: BaseConnector) -> ValidationResult:
        """
        Execute validation with error handling and timing.

        Args:
            source_connector: Source data connector
            target_connector: Target data connector

        Returns:
            ValidationResult with validation outcome
        """
        start_time = time.time()

        try:
            self.logger.info(f"Starting validation: {self.name}")
            self._validate_config()

            # Execute validation
            result = self._execute_validation(source_connector, target_connector)

            # Calculate execution time
            execution_time = time.time() - start_time
            result.execution_time_seconds = execution_time

            self.logger.info(
                f"Validation '{self.name}' completed: {result.status} "
                f"in {execution_time:.2f}s"
            )

            return result

        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.error(f"Validation '{self.name}' failed: {str(e)}", exc_info=True)

            return ValidationResult(
                name=self.name,
                validation_type=self.get_validation_type(),
                status=ValidationStatus.ERROR,
                source_name=source_connector.name,
                target_name=target_connector.name,
                execution_time_seconds=execution_time,
                error_message=str(e),
                error_details={'exception_type': type(e).__name__}
            )

    def _validate_config(self) -> None:
        """
        Validate that configuration is complete and correct.
        Override in subclasses if additional validation is needed.

        Raises:
            ConfigurationError: If configuration is invalid
        """
        if not self.name:
            from .exceptions import ConfigurationError
            raise ConfigurationError("Validation name is required")

    def _create_result(self, status: ValidationStatus,
                      source_connector: BaseConnector,
                      target_connector: BaseConnector,
                      **kwargs) -> ValidationResult:
        """
        Helper method to create a ValidationResult.

        Args:
            status: Validation status
            source_connector: Source connector
            target_connector: Target connector
            **kwargs: Additional fields for ValidationResult

        Returns:
            ValidationResult instance
        """
        return ValidationResult(
            name=self.name,
            validation_type=self.get_validation_type(),
            status=status,
            source_name=source_connector.name,
            target_name=target_connector.name,
            execution_time_seconds=0.0,  # Will be set by validate()
            **kwargs
        )

    def _check_threshold(self, actual_value: float, threshold_key: str,
                        default_threshold: float = None) -> bool:
        """
        Check if actual value is within configured threshold.

        Args:
            actual_value: Value to check
            threshold_key: Key in thresholds config
            default_threshold: Default if not configured

        Returns:
            True if within threshold, False otherwise
        """
        threshold = self.thresholds.get(threshold_key, default_threshold)
        if threshold is None:
            return True
        return abs(actual_value) <= threshold

    def get_config_value(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value with default.

        Args:
            key: Configuration key
            default: Default value if key not found

        Returns:
            Configuration value or default
        """
        return self.config.get(key, default)

    def __repr__(self) -> str:
        """String representation of validator."""
        return f"{self.__class__.__name__}(name='{self.name}')"
