"""
Abstract base class for report generators.
Defines the contract that all reporter implementations must follow.
"""
from abc import ABC, abstractmethod
from typing import Dict, Any
from pathlib import Path

from ..models.validation_result import ValidationSummary
from ..models.enums import ReporterType
from ..utils.logger import get_logger


class BaseReporter(ABC):
    """Abstract base class for all report generators."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize reporter.

        Args:
            config: Reporter configuration dictionary
        """
        self.config = config
        self.logger = get_logger(f"{self.__class__.__name__}")
        self.output_path = config.get('output_path')
        self.enabled = config.get('enabled', True)

    @abstractmethod
    def get_reporter_type(self) -> ReporterType:
        """
        Get the type of reporter.

        Returns:
            ReporterType enum value
        """
        pass

    @abstractmethod
    def generate_report(self, summary: ValidationSummary) -> str:
        """
        Generate report from validation summary.

        Args:
            summary: ValidationSummary with all results

        Returns:
            Path to generated report or status message

        Raises:
            ReporterError: If report generation fails
        """
        pass

    def report(self, summary: ValidationSummary) -> str:
        """
        Generate report with error handling.

        Args:
            summary: ValidationSummary with all results

        Returns:
            Path to generated report or status message
        """
        if not self.enabled:
            self.logger.info(f"{self.__class__.__name__} is disabled, skipping")
            return "Reporter disabled"

        try:
            self.logger.info(f"Generating {self.get_reporter_type()} report")
            result = self.generate_report(summary)
            self.logger.info(f"Report generated successfully: {result}")
            return result

        except Exception as e:
            self.logger.error(f"Failed to generate report: {str(e)}", exc_info=True)
            from ..core.exceptions import ReporterError
            raise ReporterError(
                message=f"Failed to generate {self.get_reporter_type()} report: {str(e)}",
                reporter_type=str(self.get_reporter_type()),
                output_path=self.output_path,
                original_error=e
            )

    def _ensure_output_directory(self) -> None:
        """Create output directory if it doesn't exist."""
        if self.output_path:
            output_dir = Path(self.output_path).parent
            output_dir.mkdir(parents=True, exist_ok=True)

    def _validate_config(self) -> None:
        """
        Validate reporter configuration.
        Override in subclasses if additional validation is needed.

        Raises:
            ConfigurationError: If configuration is invalid
        """
        pass

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

    def is_enabled(self) -> bool:
        """Check if reporter is enabled."""
        return self.enabled

    def __repr__(self) -> str:
        """String representation of reporter."""
        return (f"{self.__class__.__name__}"
                f"(type={self.get_reporter_type()}, enabled={self.enabled})")
