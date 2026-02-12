"""
Factory for creating report generators.
Implements the Factory pattern for easy addition of new reporters.
"""
from typing import Dict, Any, Type
from .base_reporter import BaseReporter
from ..models.enums import ReporterType
from ..utils.logger import get_logger
from ..core.exceptions import ConfigurationError

# Import all reporter implementations
from .json_reporter import JSONReporter
from .console_reporter import ConsoleReporter
from .html_reporter import HTMLReporter
from .email_reporter import EmailReporter
from .database_reporter import DatabaseReporter


class ReporterFactory:
    """Factory for creating reporter instances."""

    # Registry mapping reporter types to implementation classes
    _registry: Dict[ReporterType, Type[BaseReporter]] = {
        ReporterType.JSON: JSONReporter,
        ReporterType.CONSOLE: ConsoleReporter,
        ReporterType.HTML: HTMLReporter,
        ReporterType.EMAIL: EmailReporter,
        ReporterType.DATABASE: DatabaseReporter,
    }

    _logger = get_logger("ReporterFactory")

    @classmethod
    def create(cls, reporter_type: str, config: Dict[str, Any]) -> BaseReporter:
        """
        Create a reporter instance.

        Args:
            reporter_type: Type of reporter (json, html, console, email, database)
            config: Reporter configuration dictionary

        Returns:
            Reporter instance

        Raises:
            ConfigurationError: If reporter type is not supported
        """
        try:
            # Convert string to enum
            rep_type = ReporterType(reporter_type.lower())
        except ValueError:
            raise ConfigurationError(
                f"Unknown reporter type: {reporter_type}. "
                f"Supported types: {', '.join([t.value for t in ReporterType])}"
            )

        if rep_type not in cls._registry:
            raise ConfigurationError(
                f"Reporter type '{reporter_type}' is registered but not implemented"
            )

        reporter_class = cls._registry[rep_type]
        cls._logger.info(f"Creating {reporter_type} reporter")

        try:
            return reporter_class(config=config)
        except Exception as e:
            raise ConfigurationError(
                f"Failed to create reporter of type '{reporter_type}': {str(e)}",
                details={'reporter_type': reporter_type, 'error': str(e)}
            )

    @classmethod
    def register(cls, reporter_type: ReporterType,
                reporter_class: Type[BaseReporter]) -> None:
        """
        Register a new reporter type.

        This allows users to add custom reporter implementations.

        Args:
            reporter_type: Reporter type enum
            reporter_class: Reporter implementation class

        Example:
            ReporterFactory.register(
                ReporterType.CUSTOM,
                MyCustomReporter
            )
        """
        if not issubclass(reporter_class, BaseReporter):
            raise TypeError(
                f"Reporter class must inherit from BaseReporter, "
                f"got {reporter_class.__name__}"
            )

        cls._registry[reporter_type] = reporter_class
        cls._logger.info(
            f"Registered reporter: {reporter_type.value} -> {reporter_class.__name__}"
        )

    @classmethod
    def get_supported_types(cls) -> list:
        """
        Get list of supported reporter types.

        Returns:
            List of supported reporter type strings
        """
        return [rep_type.value for rep_type in cls._registry.keys()]

    @classmethod
    def is_supported(cls, reporter_type: str) -> bool:
        """
        Check if reporter type is supported.

        Args:
            reporter_type: Reporter type string

        Returns:
            True if supported, False otherwise
        """
        try:
            rep_type = ReporterType(reporter_type.lower())
            return rep_type in cls._registry
        except ValueError:
            return False


def create_reporter(reporter_type: str, config: Dict[str, Any]) -> BaseReporter:
    """
    Convenience function to create a reporter.

    Args:
        reporter_type: Type of reporter
        config: Reporter configuration dictionary

    Returns:
        Reporter instance
    """
    return ReporterFactory.create(reporter_type, config)
