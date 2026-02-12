"""
Factory for creating data source connectors.
Implements the Factory pattern for easy addition of new connectors.
"""
from typing import Dict, Any, Type
from ..core.base_connector import BaseConnector
from ..models.enums import ConnectorType
from ..utils.logger import get_logger
from ..core.exceptions import ConfigurationError

# Import all connector implementations
from .spark_connector import SparkConnector
from .hive_connector import HiveConnector
from .s3_connector import S3Connector
from .adls_connector import ADLSConnector
from .gcs_connector import GCSConnector


class ConnectorFactory:
    """Factory for creating connector instances."""

    # Registry mapping connector types to implementation classes
    _registry: Dict[ConnectorType, Type[BaseConnector]] = {
        ConnectorType.SPARK: SparkConnector,
        ConnectorType.HIVE: HiveConnector,
        ConnectorType.S3: S3Connector,
        ConnectorType.ADLS: ADLSConnector,
        ConnectorType.GCS: GCSConnector,
    }

    _logger = get_logger("ConnectorFactory")

    @classmethod
    def create(cls, name: str, connector_type: str, config: Dict[str, Any]) -> BaseConnector:
        """
        Create a connector instance.

        Args:
            name: Connector instance name
            connector_type: Type of connector (spark, hive, s3, adls, gcs)
            config: Configuration dictionary

        Returns:
            Connector instance

        Raises:
            ConfigurationError: If connector type is not supported
        """
        try:
            # Convert string to enum
            conn_type = ConnectorType(connector_type.lower())
        except ValueError:
            raise ConfigurationError(
                f"Unknown connector type: {connector_type}. "
                f"Supported types: {', '.join([t.value for t in ConnectorType])}"
            )

        if conn_type not in cls._registry:
            raise ConfigurationError(
                f"Connector type '{connector_type}' is registered but not implemented"
            )

        connector_class = cls._registry[conn_type]
        cls._logger.info(f"Creating {connector_type} connector: {name}")

        try:
            return connector_class(name=name, config=config)
        except Exception as e:
            raise ConfigurationError(
                f"Failed to create connector '{name}' of type '{connector_type}': {str(e)}",
                details={'connector_type': connector_type, 'error': str(e)}
            )

    @classmethod
    def register(cls, connector_type: ConnectorType,
                connector_class: Type[BaseConnector]) -> None:
        """
        Register a new connector type.

        This allows users to add custom connector implementations.

        Args:
            connector_type: Connector type enum
            connector_class: Connector implementation class

        Example:
            ConnectorFactory.register(
                ConnectorType.JDBC,
                MyCustomJDBCConnector
            )
        """
        if not issubclass(connector_class, BaseConnector):
            raise TypeError(
                f"Connector class must inherit from BaseConnector, "
                f"got {connector_class.__name__}"
            )

        cls._registry[connector_type] = connector_class
        cls._logger.info(
            f"Registered connector: {connector_type.value} -> {connector_class.__name__}"
        )

    @classmethod
    def get_supported_types(cls) -> list:
        """
        Get list of supported connector types.

        Returns:
            List of supported connector type strings
        """
        return [conn_type.value for conn_type in cls._registry.keys()]

    @classmethod
    def is_supported(cls, connector_type: str) -> bool:
        """
        Check if connector type is supported.

        Args:
            connector_type: Connector type string

        Returns:
            True if supported, False otherwise
        """
        try:
            conn_type = ConnectorType(connector_type.lower())
            return conn_type in cls._registry
        except ValueError:
            return False


def create_connector(name: str, connector_type: str,
                    config: Dict[str, Any]) -> BaseConnector:
    """
    Convenience function to create a connector.

    Args:
        name: Connector instance name
        connector_type: Type of connector
        config: Configuration dictionary

    Returns:
        Connector instance
    """
    return ConnectorFactory.create(name, connector_type, config)
