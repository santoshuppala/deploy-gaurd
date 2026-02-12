"""
Enumerations for validation framework.
Provides type-safe constants for validation statuses, types, connectors, and reporters.
"""
from enum import Enum


class ValidationStatus(str, Enum):
    """Status of a validation result."""
    PASSED = "PASSED"
    FAILED = "FAILED"
    WARNING = "WARNING"
    SKIPPED = "SKIPPED"
    ERROR = "ERROR"

    def __str__(self):
        return self.value

    @property
    def is_successful(self) -> bool:
        """Check if status indicates success."""
        return self in (ValidationStatus.PASSED, ValidationStatus.WARNING)

    @property
    def is_failure(self) -> bool:
        """Check if status indicates failure."""
        return self in (ValidationStatus.FAILED, ValidationStatus.ERROR)


class ValidationType(str, Enum):
    """Type of validation to perform."""
    ROW_COUNT = "row_count"
    DATA_QUALITY = "data_quality"
    SCHEMA = "schema"
    BUSINESS_RULE = "business_rule"
    NEW_COLUMN = "new_column"

    def __str__(self):
        return self.value


class ConnectorType(str, Enum):
    """Type of data source connector."""
    SPARK = "spark"
    HIVE = "hive"
    S3 = "s3"
    ADLS = "adls"
    GCS = "gcs"
    JDBC = "jdbc"

    def __str__(self):
        return self.value


class ReporterType(str, Enum):
    """Type of report output."""
    JSON = "json"
    HTML = "html"
    CONSOLE = "console"
    EMAIL = "email"
    DATABASE = "database"

    def __str__(self):
        return self.value
