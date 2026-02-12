"""
Hive/Hadoop connector implementation.
Provides connectivity to Hive for data validation.
"""
from typing import Dict, Any, Optional
import pandas as pd

from ..core.base_connector import BaseConnector
from ..core.exceptions import ConnectionError, ConnectorError


class HiveConnector(BaseConnector):
    """Connector for Apache Hive."""

    def __init__(self, name: str, config: Dict[str, Any]):
        """
        Initialize Hive connector.

        Args:
            name: Connector instance name
            config: Configuration dictionary with Hive connection settings
        """
        super().__init__(name, config)
        self.connection = None
        self.cursor = None
        self.host = config.get('host', 'localhost')
        self.port = config.get('port', 10000)
        self.database = config.get('database', 'default')
        self.username = config.get('username')
        self.password = config.get('password')
        self.auth_mechanism = config.get('auth_mechanism', 'PLAIN')

    def connect(self) -> None:
        """Establish connection to Hive."""
        try:
            from pyhive import hive
            from thrift.transport import TSocket, TTransport
            from thrift.protocol import TBinaryProtocol

            self.logger.info(f"Connecting to Hive at {self.host}:{self.port}/{self.database}")

            self.connection = hive.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                username=self.username,
                password=self.password,
                auth=self.auth_mechanism
            )

            self.cursor = self.connection.cursor()
            self._connected = True
            self.logger.info("Connected to Hive successfully")

        except ImportError:
            raise ConnectionError(
                "PyHive is not installed. Install it with: pip install pyhive thrift sasl thrift_sasl",
                connector_type="hive"
            )
        except Exception as e:
            raise ConnectionError(
                f"Failed to connect to Hive: {str(e)}",
                connector_type="hive",
                connection_details={
                    'host': self.host,
                    'port': self.port,
                    'database': self.database
                },
                original_error=e
            )

    def disconnect(self) -> None:
        """Close connection to Hive."""
        try:
            if self.cursor:
                self.cursor.close()
                self.cursor = None

            if self.connection:
                self.connection.close()
                self.connection = None

            self._connected = False
            self.logger.info("Disconnected from Hive")

        except Exception as e:
            self.logger.error(f"Error disconnecting from Hive: {str(e)}")

    def read_data(self, query: str, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Read data using Hive query.

        Args:
            query: HiveQL query to execute
            limit: Maximum number of rows to return

        Returns:
            Pandas DataFrame with query results
        """
        if not self._connected or not self.cursor:
            raise ConnectorError("Not connected to Hive", connector_name=self.name)

        try:
            self.logger.debug(f"Executing query: {query}")

            if limit and 'LIMIT' not in query.upper():
                query = f"{query} LIMIT {limit}"

            self.cursor.execute(query)
            columns = [desc[0] for desc in self.cursor.description]
            data = self.cursor.fetchall()

            df = pd.DataFrame(data, columns=columns)
            self.logger.info(f"Retrieved {len(df)} rows")
            return df

        except Exception as e:
            raise ConnectorError(
                f"Failed to execute query: {str(e)}",
                connector_name=self.name,
                operation="read_data",
                original_error=e
            )

    def get_row_count(self, table_or_path: str) -> int:
        """
        Get row count from Hive table.

        Args:
            table_or_path: Table name (format: database.table or table)

        Returns:
            Number of rows
        """
        if not self._connected or not self.cursor:
            raise ConnectorError("Not connected to Hive", connector_name=self.name)

        try:
            query = f"SELECT COUNT(*) FROM {table_or_path}"
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            count = result[0] if result else 0

            self.logger.info(f"Row count for {table_or_path}: {count:,}")
            return count

        except Exception as e:
            raise ConnectorError(
                f"Failed to get row count: {str(e)}",
                connector_name=self.name,
                operation="get_row_count",
                original_error=e
            )

    def get_schema(self, table_or_path: str) -> Dict[str, str]:
        """
        Get schema information from Hive table.

        Args:
            table_or_path: Table name

        Returns:
            Dictionary mapping column names to data types
        """
        if not self._connected or not self.cursor:
            raise ConnectorError("Not connected to Hive", connector_name=self.name)

        try:
            query = f"DESCRIBE {table_or_path}"
            self.cursor.execute(query)
            results = self.cursor.fetchall()

            schema = {}
            for row in results:
                # Hive DESCRIBE returns (col_name, data_type, comment)
                col_name = row[0].strip()
                data_type = row[1].strip() if len(row) > 1 else 'string'

                # Skip partition and other metadata rows
                if col_name and not col_name.startswith('#'):
                    schema[col_name] = data_type

            self.logger.info(f"Retrieved schema with {len(schema)} columns")
            return schema

        except Exception as e:
            raise ConnectorError(
                f"Failed to get schema: {str(e)}",
                connector_name=self.name,
                operation="get_schema",
                original_error=e
            )

    def execute_query(self, query: str) -> Any:
        """
        Execute HiveQL query.

        Args:
            query: HiveQL query to execute

        Returns:
            Query results
        """
        if not self._connected or not self.cursor:
            raise ConnectorError("Not connected to Hive", connector_name=self.name)

        try:
            self.logger.debug(f"Executing query: {query}")
            self.cursor.execute(query)

            # Check if query returns results
            if self.cursor.description:
                return self.cursor.fetchall()
            else:
                return None

        except Exception as e:
            raise ConnectorError(
                f"Failed to execute query: {str(e)}",
                connector_name=self.name,
                operation="execute_query",
                original_error=e
            )
