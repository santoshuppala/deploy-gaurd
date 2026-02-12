"""
Azure Data Lake Storage (ADLS) connector implementation.
Provides connectivity to Azure ADLS for data validation.
"""
from typing import Dict, Any, Optional
import pandas as pd
from io import BytesIO

from ..core.base_connector import BaseConnector
from ..core.exceptions import ConnectionError, ConnectorError


class ADLSConnector(BaseConnector):
    """Connector for Azure Data Lake Storage (ADLS Gen2)."""

    def __init__(self, name: str, config: Dict[str, Any]):
        """
        Initialize ADLS connector.

        Args:
            name: Connector instance name
            config: Configuration dictionary with ADLS settings
        """
        super().__init__(name, config)
        self.service_client = None
        self.file_system_client = None
        self.account_name = config.get('account_name')
        self.account_key = config.get('account_key')
        self.container = config.get('container')
        self.file_system = config.get('file_system', self.container)

    def connect(self) -> None:
        """Establish connection to ADLS."""
        try:
            from azure.storage.filedatalake import DataLakeServiceClient

            self.logger.info(f"Connecting to ADLS account: {self.account_name}")

            account_url = f"https://{self.account_name}.dfs.core.windows.net"

            self.service_client = DataLakeServiceClient(
                account_url=account_url,
                credential=self.account_key
            )

            # Get file system client
            self.file_system_client = self.service_client.get_file_system_client(
                file_system=self.file_system
            )

            # Test connection
            self.file_system_client.get_file_system_properties()

            self._connected = True
            self.logger.info(f"Connected to ADLS container '{self.file_system}' successfully")

        except ImportError:
            raise ConnectionError(
                "azure-storage-file-datalake is not installed. "
                "Install it with: pip install azure-storage-file-datalake",
                connector_type="adls"
            )
        except Exception as e:
            raise ConnectionError(
                f"Failed to connect to ADLS: {str(e)}",
                connector_type="adls",
                connection_details={
                    'account_name': self.account_name,
                    'container': self.file_system
                },
                original_error=e
            )

    def disconnect(self) -> None:
        """Close ADLS connection."""
        self.service_client = None
        self.file_system_client = None
        self._connected = False
        self.logger.info("Disconnected from ADLS")

    def read_data(self, query: str, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Read data from ADLS path.

        Args:
            query: ADLS path (e.g., 'path/to/file.parquet' or 'folder/*.csv')
            limit: Maximum number of rows to return

        Returns:
            Pandas DataFrame with data
        """
        if not self._connected:
            raise ConnectorError("Not connected to ADLS", connector_name=self.name)

        try:
            # Check file type and read accordingly
            if query.endswith('.parquet') or 'parquet' in query:
                df = self._read_parquet(query)
            elif query.endswith('.csv'):
                df = self._read_csv(query)
            elif query.endswith('.json'):
                df = self._read_json(query)
            else:
                # Try to read as parquet by default
                df = self._read_parquet(query)

            if limit:
                df = df.head(limit)

            self.logger.info(f"Retrieved {len(df)} rows from ADLS")
            return df

        except Exception as e:
            raise ConnectorError(
                f"Failed to read data from ADLS: {str(e)}",
                connector_name=self.name,
                operation="read_data",
                original_error=e
            )

    def get_row_count(self, table_or_path: str) -> int:
        """
        Get row count from ADLS file(s).

        Args:
            table_or_path: ADLS path

        Returns:
            Number of rows
        """
        if not self._connected:
            raise ConnectorError("Not connected to ADLS", connector_name=self.name)

        try:
            df = self.read_data(table_or_path)
            count = len(df)
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
        Get schema from ADLS file(s).

        Args:
            table_or_path: ADLS path

        Returns:
            Dictionary mapping column names to data types
        """
        if not self._connected:
            raise ConnectorError("Not connected to ADLS", connector_name=self.name)

        try:
            df = self.read_data(table_or_path, limit=1)
            schema = {col: str(dtype) for col, dtype in df.dtypes.items()}
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
        Execute query (for ADLS, this just reads data).

        Args:
            query: ADLS path

        Returns:
            DataFrame with data
        """
        return self.read_data(query)

    def _read_file_content(self, file_path: str) -> bytes:
        """Read file content from ADLS."""
        file_client = self.file_system_client.get_file_client(file_path)
        download = file_client.download_file()
        return download.readall()

    def _read_parquet(self, path: str) -> pd.DataFrame:
        """Read Parquet file from ADLS."""
        if '*' in path:
            return self._read_multiple_files(path, 'parquet')

        content = self._read_file_content(path)
        return pd.read_parquet(BytesIO(content))

    def _read_csv(self, path: str) -> pd.DataFrame:
        """Read CSV file from ADLS."""
        if '*' in path:
            return self._read_multiple_files(path, 'csv')

        content = self._read_file_content(path)
        return pd.read_csv(BytesIO(content))

    def _read_json(self, path: str) -> pd.DataFrame:
        """Read JSON file from ADLS."""
        if '*' in path:
            return self._read_multiple_files(path, 'json')

        content = self._read_file_content(path)
        return pd.read_json(BytesIO(content))

    def _read_multiple_files(self, path_pattern: str, file_format: str) -> pd.DataFrame:
        """Read multiple files matching pattern."""
        # Get directory prefix
        prefix = path_pattern.split('*')[0].rstrip('/')

        # List files in directory
        paths = self.file_system_client.get_paths(path=prefix)

        dfs = []
        for path in paths:
            if path.is_directory:
                continue

            file_name = path.name
            if file_name.endswith(f'.{file_format}'):
                try:
                    content = self._read_file_content(file_name)

                    if file_format == 'parquet':
                        df = pd.read_parquet(BytesIO(content))
                    elif file_format == 'csv':
                        df = pd.read_csv(BytesIO(content))
                    elif file_format == 'json':
                        df = pd.read_json(BytesIO(content))
                    else:
                        continue

                    dfs.append(df)
                except Exception as e:
                    self.logger.warning(f"Failed to read {file_name}: {str(e)}")

        if not dfs:
            return pd.DataFrame()

        return pd.concat(dfs, ignore_index=True)
