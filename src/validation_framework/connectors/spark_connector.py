"""
Apache Spark connector implementation.
Provides connectivity to Spark SQL for data validation.
"""
from typing import Dict, Any, Optional
import pandas as pd

from ..core.base_connector import BaseConnector
from ..core.exceptions import ConnectionError, ConnectorError


class SparkConnector(BaseConnector):
    """Connector for Apache Spark."""

    def __init__(self, name: str, config: Dict[str, Any]):
        """
        Initialize Spark connector.

        Args:
            name: Connector instance name
            config: Configuration dictionary with Spark settings
        """
        super().__init__(name, config)
        self.spark = None
        self.app_name = config.get('app_name', 'Validation Framework')
        self.master = config.get('master', 'local[*]')
        self.spark_config = config.get('config', {})

    def connect(self) -> None:
        """Establish Spark session."""
        try:
            from pyspark.sql import SparkSession

            self.logger.info(f"Creating Spark session: {self.app_name} on {self.master}")

            builder = SparkSession.builder \
                .appName(self.app_name) \
                .master(self.master)

            # Apply additional Spark configurations
            for key, value in self.spark_config.items():
                builder = builder.config(key, value)

            self.spark = builder.getOrCreate()
            self._connected = True
            self.logger.info(f"Spark session created successfully: {self.spark.sparkContext.applicationId}")

        except ImportError:
            raise ConnectionError(
                "PySpark is not installed. Install it with: pip install pyspark",
                connector_type="spark"
            )
        except Exception as e:
            raise ConnectionError(
                f"Failed to create Spark session: {str(e)}",
                connector_type="spark",
                connection_details={'app_name': self.app_name, 'master': self.master},
                original_error=e
            )

    def disconnect(self) -> None:
        """Close Spark session."""
        if self.spark:
            try:
                self.logger.info("Stopping Spark session")
                self.spark.stop()
                self._connected = False
                self.logger.info("Spark session stopped")
            except Exception as e:
                self.logger.error(f"Error stopping Spark session: {str(e)}")
        self.spark = None

    def read_data(self, query: str, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Read data using Spark SQL query.

        Args:
            query: SQL query to execute
            limit: Maximum number of rows to return

        Returns:
            Pandas DataFrame with query results
        """
        if not self._connected or not self.spark:
            raise ConnectorError("Not connected to Spark", connector_name=self.name)

        try:
            self.logger.debug(f"Executing query: {query}")

            # Check if query is a file path
            if self._is_file_path(query):
                df = self._read_file(query)
            else:
                df = self.spark.sql(query)

            if limit:
                df = df.limit(limit)

            # Convert to Pandas
            pandas_df = df.toPandas()
            self.logger.info(f"Retrieved {len(pandas_df)} rows")
            return pandas_df

        except Exception as e:
            raise ConnectorError(
                f"Failed to execute query: {str(e)}",
                connector_name=self.name,
                operation="read_data",
                original_error=e
            )

    def get_row_count(self, table_or_path: str) -> int:
        """
        Get row count from table or file.

        Args:
            table_or_path: Table name or file path

        Returns:
            Number of rows
        """
        if not self._connected or not self.spark:
            raise ConnectorError("Not connected to Spark", connector_name=self.name)

        try:
            if self._is_file_path(table_or_path):
                df = self._read_file(table_or_path)
            else:
                df = self.spark.table(table_or_path)

            count = df.count()
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
        Get schema information.

        Args:
            table_or_path: Table name or file path

        Returns:
            Dictionary mapping column names to data types
        """
        if not self._connected or not self.spark:
            raise ConnectorError("Not connected to Spark", connector_name=self.name)

        try:
            if self._is_file_path(table_or_path):
                df = self._read_file(table_or_path)
            else:
                df = self.spark.table(table_or_path)

            schema = {field.name: str(field.dataType) for field in df.schema.fields}
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
        Execute Spark SQL query.

        Args:
            query: SQL query to execute

        Returns:
            Spark DataFrame
        """
        if not self._connected or not self.spark:
            raise ConnectorError("Not connected to Spark", connector_name=self.name)

        try:
            self.logger.debug(f"Executing query: {query}")
            result = self.spark.sql(query)
            return result

        except Exception as e:
            raise ConnectorError(
                f"Failed to execute query: {str(e)}",
                connector_name=self.name,
                operation="execute_query",
                original_error=e
            )

    def _is_file_path(self, path: str) -> bool:
        """Check if string is a file path."""
        file_formats = ['.parquet', '.csv', '.json', '.orc', '.avro']
        return any(fmt in path.lower() for fmt in file_formats) or \
               path.startswith('s3://') or path.startswith('hdfs://') or \
               path.startswith('file://')

    def _read_file(self, path: str):
        """Read file from path and return Spark DataFrame."""
        if path.endswith('.parquet') or 'parquet' in path:
            return self.spark.read.parquet(path)
        elif path.endswith('.csv'):
            return self.spark.read.csv(path, header=True, inferSchema=True)
        elif path.endswith('.json'):
            return self.spark.read.json(path)
        elif path.endswith('.orc'):
            return self.spark.read.orc(path)
        else:
            # Try to infer format
            return self.spark.read.load(path)
