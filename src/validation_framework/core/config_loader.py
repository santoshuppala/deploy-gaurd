"""
Configuration loader for YAML-based validation configurations.
Handles parsing, validation, and environment variable substitution.
"""
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
from pydantic import ValidationError as PydanticValidationError

from ..models.validation_config import FrameworkConfig
from ..utils.helpers import substitute_env_variables
from ..utils.logger import get_logger
from .exceptions import ConfigurationError


class ConfigLoader:
    """Loads and validates YAML configuration files."""

    def __init__(self, config_path: str):
        """
        Initialize config loader.

        Args:
            config_path: Path to YAML configuration file

        Raises:
            ConfigurationError: If config file doesn't exist
        """
        self.config_path = Path(config_path)
        self.logger = get_logger(self.__class__.__name__)

        if not self.config_path.exists():
            raise ConfigurationError(
                f"Configuration file not found: {config_path}",
                config_path=config_path
            )

    def load(self) -> FrameworkConfig:
        """
        Load and validate configuration from YAML file.

        Returns:
            FrameworkConfig instance with validated configuration

        Raises:
            ConfigurationError: If configuration is invalid
        """
        try:
            self.logger.info(f"Loading configuration from: {self.config_path}")

            # Load YAML file
            with open(self.config_path, 'r') as f:
                raw_config = yaml.safe_load(f)

            if not raw_config:
                raise ConfigurationError(
                    "Configuration file is empty",
                    config_path=str(self.config_path)
                )

            # Substitute environment variables
            config_data = substitute_env_variables(raw_config)

            # Validate with Pydantic
            try:
                config = FrameworkConfig(**config_data)
                self.logger.info(
                    f"Configuration loaded successfully: "
                    f"{len(config.connections)} connections, "
                    f"{len(config.validations)} validations, "
                    f"{len(config.reporters)} reporters"
                )
                return config

            except PydanticValidationError as e:
                error_details = self._format_pydantic_errors(e)
                raise ConfigurationError(
                    f"Configuration validation failed:\n{error_details}",
                    config_path=str(self.config_path),
                    details={'validation_errors': e.errors()}
                )

        except yaml.YAMLError as e:
            raise ConfigurationError(
                f"Failed to parse YAML configuration: {str(e)}",
                config_path=str(self.config_path),
                details={'yaml_error': str(e)}
            )
        except Exception as e:
            if isinstance(e, ConfigurationError):
                raise
            raise ConfigurationError(
                f"Failed to load configuration: {str(e)}",
                config_path=str(self.config_path),
                details={'error': str(e)}
            )

    @staticmethod
    def _format_pydantic_errors(error: PydanticValidationError) -> str:
        """Format Pydantic validation errors for display."""
        errors = []
        for err in error.errors():
            location = ' -> '.join(str(loc) for loc in err['loc'])
            message = err['msg']
            errors.append(f"  - {location}: {message}")
        return '\n'.join(errors)

    def load_raw(self) -> Dict[str, Any]:
        """
        Load configuration as raw dictionary without validation.

        Returns:
            Raw configuration dictionary

        Raises:
            ConfigurationError: If loading fails
        """
        try:
            with open(self.config_path, 'r') as f:
                raw_config = yaml.safe_load(f)
            return substitute_env_variables(raw_config)
        except Exception as e:
            raise ConfigurationError(
                f"Failed to load raw configuration: {str(e)}",
                config_path=str(self.config_path)
            )

    @staticmethod
    def from_dict(config_dict: Dict[str, Any]) -> FrameworkConfig:
        """
        Create FrameworkConfig from dictionary.

        Args:
            config_dict: Configuration dictionary

        Returns:
            FrameworkConfig instance

        Raises:
            ConfigurationError: If validation fails
        """
        try:
            config_data = substitute_env_variables(config_dict)
            return FrameworkConfig(**config_data)
        except PydanticValidationError as e:
            error_details = ConfigLoader._format_pydantic_errors(e)
            raise ConfigurationError(
                f"Configuration validation failed:\n{error_details}",
                details={'validation_errors': e.errors()}
            )

    @staticmethod
    def merge_configs(base_config: Dict[str, Any],
                     override_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Merge two configuration dictionaries.

        Args:
            base_config: Base configuration
            override_config: Override configuration (takes precedence)

        Returns:
            Merged configuration dictionary
        """
        from ..utils.helpers import deep_merge
        return deep_merge(base_config, override_config)

    def validate_only(self) -> bool:
        """
        Validate configuration without loading.

        Returns:
            True if valid, False otherwise
        """
        try:
            self.load()
            return True
        except ConfigurationError as e:
            self.logger.error(f"Configuration validation failed: {str(e)}")
            return False


def load_config(config_path: str) -> FrameworkConfig:
    """
    Convenience function to load configuration.

    Args:
        config_path: Path to YAML configuration file

    Returns:
        FrameworkConfig instance

    Raises:
        ConfigurationError: If loading or validation fails
    """
    loader = ConfigLoader(config_path)
    return loader.load()
