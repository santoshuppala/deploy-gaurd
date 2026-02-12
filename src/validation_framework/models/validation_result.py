"""
Data models for validation results.
Contains ValidationResult and ValidationSummary classes for tracking validation outcomes.
"""
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Dict, List, Any, Optional
from .enums import ValidationStatus, ValidationType


@dataclass
class ValidationResult:
    """Individual validation result with metrics and status."""

    name: str
    validation_type: ValidationType
    status: ValidationStatus
    source_name: str
    target_name: str
    execution_time_seconds: float
    timestamp: datetime = field(default_factory=datetime.now)

    # Metrics
    source_count: Optional[int] = None
    target_count: Optional[int] = None
    difference: Optional[int] = None
    difference_percent: Optional[float] = None

    # Quality metrics
    null_count: Optional[int] = None
    duplicate_count: Optional[int] = None
    invalid_count: Optional[int] = None

    # Schema comparison
    schema_differences: Optional[List[str]] = None

    # Business rule results
    rule_results: Optional[Dict[str, Any]] = None

    # Error tracking
    error_message: Optional[str] = None
    error_details: Optional[Dict[str, Any]] = None

    # Additional metadata
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary for serialization."""
        result = asdict(self)
        result['validation_type'] = str(self.validation_type)
        result['status'] = str(self.status)
        result['timestamp'] = self.timestamp.isoformat()
        return result

    def is_successful(self) -> bool:
        """Check if validation passed or has only warnings."""
        return self.status.is_successful

    def is_failure(self) -> bool:
        """Check if validation failed or had errors."""
        return self.status.is_failure

    def get_summary_text(self) -> str:
        """Get human-readable summary of the validation."""
        if self.validation_type == ValidationType.ROW_COUNT:
            return (f"{self.name}: {self.status.value} - "
                   f"Source={self.source_count:,}, Target={self.target_count:,}, "
                   f"Diff={self.difference:,} ({self.difference_percent:.2f}%)")
        elif self.validation_type == ValidationType.DATA_QUALITY:
            return (f"{self.name}: {self.status.value} - "
                   f"Nulls={self.null_count or 0}, Duplicates={self.duplicate_count or 0}, "
                   f"Invalid={self.invalid_count or 0}")
        elif self.validation_type == ValidationType.SCHEMA:
            diff_count = len(self.schema_differences) if self.schema_differences else 0
            return f"{self.name}: {self.status.value} - {diff_count} schema differences found"
        else:
            return f"{self.name}: {self.status.value}"


@dataclass
class ValidationSummary:
    """Aggregated summary of all validation results."""

    total_validations: int
    passed: int
    failed: int
    warnings: int
    errors: int
    skipped: int
    success_rate: float
    total_execution_time_seconds: float
    start_time: datetime
    end_time: datetime
    results: List[ValidationResult] = field(default_factory=list)

    @classmethod
    def from_results(cls, results: List[ValidationResult],
                     start_time: datetime, end_time: datetime) -> 'ValidationSummary':
        """Create summary from list of validation results."""
        total = len(results)
        passed = sum(1 for r in results if r.status == ValidationStatus.PASSED)
        failed = sum(1 for r in results if r.status == ValidationStatus.FAILED)
        warnings = sum(1 for r in results if r.status == ValidationStatus.WARNING)
        errors = sum(1 for r in results if r.status == ValidationStatus.ERROR)
        skipped = sum(1 for r in results if r.status == ValidationStatus.SKIPPED)

        successful = passed + warnings
        success_rate = (successful / total * 100) if total > 0 else 0

        total_time = (end_time - start_time).total_seconds()

        return cls(
            total_validations=total,
            passed=passed,
            failed=failed,
            warnings=warnings,
            errors=errors,
            skipped=skipped,
            success_rate=success_rate,
            total_execution_time_seconds=total_time,
            start_time=start_time,
            end_time=end_time,
            results=results
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert summary to dictionary for serialization."""
        return {
            'total_validations': self.total_validations,
            'passed': self.passed,
            'failed': self.failed,
            'warnings': self.warnings,
            'errors': self.errors,
            'skipped': self.skipped,
            'success_rate': round(self.success_rate, 2),
            'total_execution_time_seconds': round(self.total_execution_time_seconds, 2),
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat(),
            'results': [r.to_dict() for r in self.results]
        }

    def has_failures(self) -> bool:
        """Check if any validations failed."""
        return self.failed > 0 or self.errors > 0

    def get_exit_code(self) -> int:
        """Get appropriate exit code for CI/CD integration."""
        if self.errors > 0 or self.failed > 0:
            return 1  # Failure
        elif self.warnings > 0:
            return 2  # Warning
        return 0  # Success

    def get_summary_text(self) -> str:
        """Get human-readable summary text."""
        return (f"Validation Summary: {self.passed}/{self.total_validations} passed, "
                f"{self.failed} failed, {self.warnings} warnings, {self.errors} errors, "
                f"{self.skipped} skipped - Success Rate: {self.success_rate:.1f}%")
