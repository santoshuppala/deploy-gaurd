"""
Custom exceptions for the validation framework.
Provides specific exception types for different error scenarios.
"""


class ValidationFrameworkError(Exception):
    """Base exception for all validation framework errors."""
    pass


class ConfigurationError(ValidationFrameworkError):
    """Raised when there's an error in configuration files or settings."""

    def __init__(self, message: str, config_path: str = None, details: dict = None):
        self.config_path = config_path
        self.details = details or {}
        super().__init__(message)


class ConnectionError(ValidationFrameworkError):
    """Raised when connection to a data source fails."""

    def __init__(self, message: str, connector_type: str = None,
                 connection_details: dict = None, original_error: Exception = None):
        self.connector_type = connector_type
        self.connection_details = connection_details or {}
        self.original_error = original_error
        super().__init__(message)


class ValidationError(ValidationFrameworkError):
    """Raised when a validation operation fails unexpectedly."""

    def __init__(self, message: str, validation_name: str = None,
                 validation_type: str = None, details: dict = None):
        self.validation_name = validation_name
        self.validation_type = validation_type
        self.details = details or {}
        super().__init__(message)


class ConnectorError(ValidationFrameworkError):
    """Raised when a connector operation fails."""

    def __init__(self, message: str, connector_name: str = None,
                 operation: str = None, original_error: Exception = None):
        self.connector_name = connector_name
        self.operation = operation
        self.original_error = original_error
        super().__init__(message)


class ReporterError(ValidationFrameworkError):
    """Raised when report generation fails."""

    def __init__(self, message: str, reporter_type: str = None,
                 output_path: str = None, original_error: Exception = None):
        self.reporter_type = reporter_type
        self.output_path = output_path
        self.original_error = original_error
        super().__init__(message)


class SchemaError(ValidationFrameworkError):
    """Raised when there's a schema-related error."""

    def __init__(self, message: str, expected_schema: dict = None,
                 actual_schema: dict = None, differences: list = None):
        self.expected_schema = expected_schema
        self.actual_schema = actual_schema
        self.differences = differences or []
        super().__init__(message)


class DataQualityError(ValidationFrameworkError):
    """Raised when data quality checks fail critically."""

    def __init__(self, message: str, quality_metrics: dict = None,
                 failed_checks: list = None):
        self.quality_metrics = quality_metrics or {}
        self.failed_checks = failed_checks or []
        super().__init__(message)


class ThresholdExceededError(ValidationFrameworkError):
    """Raised when validation thresholds are exceeded."""

    def __init__(self, message: str, threshold_name: str = None,
                 expected_value: float = None, actual_value: float = None):
        self.threshold_name = threshold_name
        self.expected_value = expected_value
        self.actual_value = actual_value
        super().__init__(message)
