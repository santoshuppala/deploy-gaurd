"""
Factory for creating validators.
Implements the Factory pattern for easy addition of new validators.
"""
from typing import Dict, Any, Type
from ..core.base_validator import BaseValidator
from ..models.enums import ValidationType
from ..utils.logger import get_logger
from ..core.exceptions import ConfigurationError

# Import all validator implementations
from .row_count_validator import RowCountValidator
from .data_quality_validator import DataQualityValidator
from .schema_validator import SchemaValidator
from .business_rule_validator import BusinessRuleValidator
from .new_column_validator import NewColumnValidator


class ValidatorFactory:
    """Factory for creating validator instances."""

    # Registry mapping validation types to implementation classes
    _registry: Dict[ValidationType, Type[BaseValidator]] = {
        ValidationType.ROW_COUNT: RowCountValidator,
        ValidationType.DATA_QUALITY: DataQualityValidator,
        ValidationType.SCHEMA: SchemaValidator,
        ValidationType.BUSINESS_RULE: BusinessRuleValidator,
        ValidationType.NEW_COLUMN: NewColumnValidator,
    }

    _logger = get_logger("ValidatorFactory")

    @classmethod
    def create(cls, validation_type: str, config: Dict[str, Any]) -> BaseValidator:
        """
        Create a validator instance.

        Args:
            validation_type: Type of validation (row_count, data_quality, schema, business_rule)
            config: Validation configuration dictionary

        Returns:
            Validator instance

        Raises:
            ConfigurationError: If validation type is not supported
        """
        try:
            # Convert string to enum
            val_type = ValidationType(validation_type.lower())
        except ValueError:
            raise ConfigurationError(
                f"Unknown validation type: {validation_type}. "
                f"Supported types: {', '.join([t.value for t in ValidationType])}"
            )

        if val_type not in cls._registry:
            raise ConfigurationError(
                f"Validation type '{validation_type}' is registered but not implemented"
            )

        validator_class = cls._registry[val_type]
        cls._logger.info(f"Creating {validation_type} validator: {config.get('name', 'Unnamed')}")

        try:
            return validator_class(config=config)
        except Exception as e:
            raise ConfigurationError(
                f"Failed to create validator of type '{validation_type}': {str(e)}",
                details={'validation_type': validation_type, 'error': str(e)}
            )

    @classmethod
    def register(cls, validation_type: ValidationType,
                validator_class: Type[BaseValidator]) -> None:
        """
        Register a new validator type.

        This allows users to add custom validator implementations.

        Args:
            validation_type: Validation type enum
            validator_class: Validator implementation class

        Example:
            ValidatorFactory.register(
                ValidationType.CUSTOM,
                MyCustomValidator
            )
        """
        if not issubclass(validator_class, BaseValidator):
            raise TypeError(
                f"Validator class must inherit from BaseValidator, "
                f"got {validator_class.__name__}"
            )

        cls._registry[validation_type] = validator_class
        cls._logger.info(
            f"Registered validator: {validation_type.value} -> {validator_class.__name__}"
        )

    @classmethod
    def get_supported_types(cls) -> list:
        """
        Get list of supported validation types.

        Returns:
            List of supported validation type strings
        """
        return [val_type.value for val_type in cls._registry.keys()]

    @classmethod
    def is_supported(cls, validation_type: str) -> bool:
        """
        Check if validation type is supported.

        Args:
            validation_type: Validation type string

        Returns:
            True if supported, False otherwise
        """
        try:
            val_type = ValidationType(validation_type.lower())
            return val_type in cls._registry
        except ValueError:
            return False


def create_validator(validation_type: str, config: Dict[str, Any]) -> BaseValidator:
    """
    Convenience function to create a validator.

    Args:
        validation_type: Type of validation
        config: Validation configuration dictionary

    Returns:
        Validator instance
    """
    return ValidatorFactory.create(validation_type, config)
