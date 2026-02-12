"""
Main validation orchestration engine.
Coordinates connectors, validators, and reporters to execute validation workflows.
"""
from typing import List, Dict, Any
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from .config_loader import load_config
from .base_connector import BaseConnector
from .base_validator import BaseValidator
from ..connectors.connector_factory import ConnectorFactory
from ..validators.validator_factory import ValidatorFactory
from ..reporters.reporter_factory import ReporterFactory
from ..models.validation_result import ValidationResult, ValidationSummary
from ..models.validation_config import FrameworkConfig
from ..utils.logger import ValidationLogger, get_logger
from ..utils.metrics import MetricsCollector
from .exceptions import ValidationFrameworkError, ConnectionError


class ValidationEngine:
    """Main orchestration engine for validation framework."""

    def __init__(self, config_path: str):
        """
        Initialize validation engine.

        Args:
            config_path: Path to YAML configuration file
        """
        self.config_path = config_path
        self.config: FrameworkConfig = None
        self.logger = None
        self.metrics = MetricsCollector()
        self.connectors: Dict[str, BaseConnector] = {}
        self.results: List[ValidationResult] = []

    def run(self) -> ValidationSummary:
        """
        Execute validation workflow.

        Returns:
            ValidationSummary with all validation results

        Raises:
            ValidationFrameworkError: If execution fails
        """
        self.metrics.start()
        start_time = datetime.now()

        try:
            # Load configuration
            self._load_configuration()

            # Setup logging
            self._setup_logging()

            self.logger.info("=" * 80)
            self.logger.info("Starting Validation Engine")
            self.logger.info("=" * 80)

            # Initialize connectors
            self._initialize_connectors()

            # Execute validations
            self._execute_validations()

            # Generate reports
            end_time = datetime.now()
            summary = ValidationSummary.from_results(self.results, start_time, end_time)
            self._generate_reports(summary)

            # Log summary
            self.logger.info("=" * 80)
            self.logger.info(summary.get_summary_text())
            self.logger.info("=" * 80)

            self.metrics.end()
            self.logger.info(self.metrics.get_summary())

            return summary

        except Exception as e:
            self.logger.error(f"Validation engine failed: {str(e)}", exc_info=True)
            raise ValidationFrameworkError(f"Engine execution failed: {str(e)}")

        finally:
            # Cleanup connections
            self._cleanup_connections()

    def _load_configuration(self) -> None:
        """Load and validate configuration."""
        self.logger = get_logger("ValidationEngine")
        self.logger.info(f"Loading configuration from: {self.config_path}")

        try:
            self.config = load_config(self.config_path)
            self.logger.info(
                f"Configuration loaded: {len(self.config.connections)} connections, "
                f"{len(self.config.validations)} validations, "
                f"{len(self.config.reporters)} reporters"
            )
        except Exception as e:
            self.logger.error(f"Failed to load configuration: {str(e)}")
            raise

    def _setup_logging(self) -> None:
        """Setup logging based on configuration."""
        log_level = self.config.settings.log_level
        log_file = self.config.settings.log_file

        self.logger = ValidationLogger.get_logger(
            name="ValidationEngine",
            log_level=log_level,
            log_file=log_file,
            console_output=True
        )

    def _initialize_connectors(self) -> None:
        """Initialize all configured connectors."""
        self.logger.info("Initializing connectors...")

        for conn_config in self.config.connections:
            if not conn_config.enabled:
                self.logger.info(f"Skipping disabled connector: {conn_config.name}")
                continue

            try:
                self.logger.info(f"Creating connector: {conn_config.name} ({conn_config.type})")

                connector = ConnectorFactory.create(
                    name=conn_config.name,
                    connector_type=conn_config.type,
                    config=conn_config.config
                )

                self.connectors[conn_config.name] = connector
                self.metrics.record_connector(conn_config.name)

                self.logger.info(f"Connector '{conn_config.name}' created successfully")

            except Exception as e:
                self.logger.error(f"Failed to create connector '{conn_config.name}': {str(e)}")
                if not self.config.settings.continue_on_error:
                    raise

        self.logger.info(f"Initialized {len(self.connectors)} connectors")

    def _execute_validations(self) -> None:
        """Execute all configured validations."""
        self.logger.info("Starting validations...")

        enabled_validations = [v for v in self.config.validations if v.enabled]

        if not enabled_validations:
            self.logger.warning("No enabled validations found")
            return

        if self.config.settings.parallel_execution:
            self._execute_validations_parallel(enabled_validations)
        else:
            self._execute_validations_sequential(enabled_validations)

        self.logger.info(f"Completed {len(self.results)} validations")

    def _execute_validations_sequential(self, validations: List) -> None:
        """Execute validations sequentially."""
        for val_config in validations:
            result = self._execute_single_validation(val_config)
            if result:
                self.results.append(result)

    def _execute_validations_parallel(self, validations: List) -> None:
        """Execute validations in parallel."""
        max_workers = self.config.settings.max_workers

        self.logger.info(f"Executing validations in parallel with {max_workers} workers")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_validation = {
                executor.submit(self._execute_single_validation, val_config): val_config
                for val_config in validations
            }

            for future in as_completed(future_to_validation):
                val_config = future_to_validation[future]
                try:
                    result = future.result()
                    if result:
                        self.results.append(result)
                except Exception as e:
                    self.logger.error(f"Validation '{val_config.name}' failed: {str(e)}")
                    if not self.config.settings.continue_on_error:
                        raise

    def _execute_single_validation(self, val_config) -> ValidationResult:
        """
        Execute a single validation.

        Args:
            val_config: ValidationConfig instance

        Returns:
            ValidationResult or None
        """
        try:
            self.logger.info(f"Executing validation: {val_config.name}")

            # Get connectors
            source_connector = self.connectors.get(val_config.source)
            target_connector = self.connectors.get(val_config.target)

            if not source_connector:
                raise ConnectionError(f"Source connector '{val_config.source}' not found")
            if not target_connector:
                raise ConnectionError(f"Target connector '{val_config.target}' not found")

            # Connect if not already connected
            if not source_connector.is_connected():
                source_connector.connect()

            if not target_connector.is_connected():
                target_connector.connect()

            # Create validator
            validator = ValidatorFactory.create(
                validation_type=val_config.type,
                config=val_config.dict()
            )

            self.metrics.record_validator(val_config.type)

            # Execute validation
            result = validator.validate(source_connector, target_connector)

            self.metrics.record_validation(result.status.value)

            return result

        except Exception as e:
            self.logger.error(f"Validation '{val_config.name}' failed: {str(e)}", exc_info=True)

            if not self.config.settings.continue_on_error:
                raise

            # Return error result
            from ..models.enums import ValidationStatus, ValidationType
            return ValidationResult(
                name=val_config.name,
                validation_type=ValidationType(val_config.type),
                status=ValidationStatus.ERROR,
                source_name=val_config.source,
                target_name=val_config.target,
                execution_time_seconds=0.0,
                error_message=str(e)
            )

    def _generate_reports(self, summary: ValidationSummary) -> None:
        """
        Generate all configured reports.

        Args:
            summary: ValidationSummary to report
        """
        self.logger.info("Generating reports...")

        for reporter_config in self.config.reporters:
            if not reporter_config.enabled:
                self.logger.info(f"Skipping disabled reporter: {reporter_config.type}")
                continue

            try:
                self.logger.info(f"Generating {reporter_config.type} report")

                reporter = ReporterFactory.create(
                    reporter_type=reporter_config.type,
                    config=reporter_config.dict()
                )

                self.metrics.record_reporter(reporter_config.type)

                result = reporter.report(summary)

                self.logger.info(f"Report generated: {result}")

            except Exception as e:
                self.logger.error(f"Failed to generate {reporter_config.type} report: {str(e)}")
                # Don't fail entire execution if reporting fails
                continue

    def _cleanup_connections(self) -> None:
        """Disconnect all connectors."""
        self.logger.info("Cleaning up connections...")

        for name, connector in self.connectors.items():
            try:
                if connector.is_connected():
                    connector.disconnect()
                    self.logger.debug(f"Disconnected: {name}")
            except Exception as e:
                self.logger.warning(f"Error disconnecting '{name}': {str(e)}")

        self.logger.info("Cleanup complete")

    def get_exit_code(self, summary: ValidationSummary) -> int:
        """
        Get appropriate exit code based on validation results.

        Args:
            summary: ValidationSummary

        Returns:
            Exit code (0=success, 1=failure, 2=warning)
        """
        return summary.get_exit_code()


def run_validation(config_path: str) -> ValidationSummary:
    """
    Convenience function to run validation.

    Args:
        config_path: Path to configuration file

    Returns:
        ValidationSummary
    """
    engine = ValidationEngine(config_path)
    return engine.run()
