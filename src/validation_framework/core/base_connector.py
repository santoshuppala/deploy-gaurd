"""
Abstract base class for data connectors.
Defines the contract that all connector implementations must follow.
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from contextlib import contextmanager
import pandas as pd

from ..utils.logger import get_logger


class BaseConnector(ABC):
    """Abstract base class for all data source connectors."""

    def __init__(self, name: str, config: Dict[str, Any]):
        """
        Initialize connector.

        Args:
            name: Connector instance name
            config: Configuration dictionary
        """
        self.name = name
        self.config = config
        self.logger = get_logger(f"{self.__class__.__name__}")
        self._connected = False

    @abstractmethod
    def connect(self) -> None:
        """
        Establish connection to the data source.
        Must set self._connected = True on success.

        Raises:
            ConnectionError: If connection fails
        """
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """
        Close connection to the data source.
        Must set self._connected = False.
        """
        pass

    @abstractmethod
    def read_data(self, query: str, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Read data from source using SQL query or path.

        Args:
            query: SQL query or file path
            limit: Maximum number of rows to return

        Returns:
            DataFrame with query results

        Raises:
            ConnectorError: If read operation fails
        """
        pass

    @abstractmethod
    def get_row_count(self, table_or_path: str) -> int:
        """
        Get total row count from table or file.

        Args:
            table_or_path: Table name or file path

        Returns:
            Number of rows

        Raises:
            ConnectorError: If count operation fails
        """
        pass

    @abstractmethod
    def get_schema(self, table_or_path: str) -> Dict[str, str]:
        """
        Get schema/column information.

        Args:
            table_or_path: Table name or file path

        Returns:
            Dictionary mapping column names to data types

        Raises:
            ConnectorError: If schema retrieval fails
        """
        pass

    @abstractmethod
    def execute_query(self, query: str) -> Any:
        """
        Execute arbitrary query.

        Args:
            query: Query to execute

        Returns:
            Query result (type depends on connector)

        Raises:
            ConnectorError: If query execution fails
        """
        pass

    def test_connection(self) -> bool:
        """
        Test if connection is working.

        Returns:
            True if connection is successful, False otherwise
        """
        try:
            self.connect()
            return self._connected
        except Exception as e:
            self.logger.error(f"Connection test failed: {str(e)}")
            return False
        finally:
            if self._connected:
                self.disconnect()

    def is_connected(self) -> bool:
        """Check if connector is currently connected."""
        return self._connected

    @contextmanager
    def connection(self):
        """
        Context manager for automatic connection management.

        Usage:
            with connector.connection():
                data = connector.read_data("SELECT * FROM table")
        """
        try:
            if not self._connected:
                self.connect()
            yield self
        finally:
            if self._connected:
                self.disconnect()

    def __enter__(self):
        """Support for 'with' statement."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Cleanup on exit from 'with' statement."""
        self.disconnect()
        return False

    def get_connector_info(self) -> Dict[str, Any]:
        """Get connector information for debugging/logging."""
        return {
            'name': self.name,
            'type': self.__class__.__name__,
            'connected': self._connected,
            'config': {k: v for k, v in self.config.items()
                      if k not in ['password', 'secret', 'token', 'key']}
        }

    def __repr__(self) -> str:
        """String representation of connector."""
        return f"{self.__class__.__name__}(name='{self.name}', connected={self._connected})"
