"""
Google Cloud Storage (GCS) connector implementation.
Provides connectivity to GCS for data validation.
"""
from typing import Dict, Any, Optional
import pandas as pd
from io import BytesIO

from ..core.base_connector import BaseConnector
from ..core.exceptions import ConnectionError, ConnectorError


class GCSConnector(BaseConnector):
    """Connector for Google Cloud Storage."""

    def __init__(self, name: str, config: Dict[str, Any]):
        """
        Initialize GCS connector.

        Args:
            name: Connector instance name
            config: Configuration dictionary with GCS settings
        """
        super().__init__(name, config)
        self.client = None
        self.bucket_obj = None
        self.bucket = config.get('bucket')
        self.project_id = config.get('project_id')
        self.credentials_path = config.get('credentials_path')
        self.prefix = config.get('prefix', '')

    def connect(self) -> None:
        """Establish connection to GCS."""
        try:
            from google.cloud import storage
            import os

            self.logger.info(f"Connecting to GCS bucket: {self.bucket}")

            # Set credentials if provided
            if self.credentials_path:
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.credentials_path

            # Create client
            if self.project_id:
                self.client = storage.Client(project=self.project_id)
            else:
                self.client = storage.Client()

            # Get bucket
            self.bucket_obj = self.client.bucket(self.bucket)

            # Test connection
            if not self.bucket_obj.exists():
                raise Exception(f"Bucket '{self.bucket}' does not exist")

            self._connected = True
            self.logger.info(f"Connected to GCS bucket '{self.bucket}' successfully")

        except ImportError:
            raise ConnectionError(
                "google-cloud-storage is not installed. "
                "Install it with: pip install google-cloud-storage",
                connector_type="gcs"
            )
        except Exception as e:
            raise ConnectionError(
                f"Failed to connect to GCS: {str(e)}",
                connector_type="gcs",
                connection_details={'bucket': self.bucket, 'project_id': self.project_id},
                original_error=e
            )

    def disconnect(self) -> None:
        """Close GCS connection."""
        self.client = None
        self.bucket_obj = None
        self._connected = False
        self.logger.info("Disconnected from GCS")

    def read_data(self, query: str, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Read data from GCS path.

        Args:
            query: GCS path (e.g., 'gs://bucket/path/*.parquet' or 'path/to/file.csv')
            limit: Maximum number of rows to return

        Returns:
            Pandas DataFrame with data
        """
        if not self._connected:
            raise ConnectorError("Not connected to GCS", connector_name=self.name)

        try:
            # Parse GCS path
            gcs_path = self._parse_gcs_path(query)

            # Read files based on extension
            if gcs_path.endswith('.parquet') or 'parquet' in gcs_path:
                df = self._read_parquet(gcs_path)
            elif gcs_path.endswith('.csv'):
                df = self._read_csv(gcs_path)
            elif gcs_path.endswith('.json'):
                df = self._read_json(gcs_path)
            else:
                # Try to read as parquet by default
                df = self._read_parquet(gcs_path)

            if limit:
                df = df.head(limit)

            self.logger.info(f"Retrieved {len(df)} rows from GCS")
            return df

        except Exception as e:
            raise ConnectorError(
                f"Failed to read data from GCS: {str(e)}",
                connector_name=self.name,
                operation="read_data",
                original_error=e
            )

    def get_row_count(self, table_or_path: str) -> int:
        """
        Get row count from GCS file(s).

        Args:
            table_or_path: GCS path

        Returns:
            Number of rows
        """
        if not self._connected:
            raise ConnectorError("Not connected to GCS", connector_name=self.name)

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
        Get schema from GCS file(s).

        Args:
            table_or_path: GCS path

        Returns:
            Dictionary mapping column names to data types
        """
        if not self._connected:
            raise ConnectorError("Not connected to GCS", connector_name=self.name)

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
        Execute query (for GCS, this just reads data).

        Args:
            query: GCS path

        Returns:
            DataFrame with data
        """
        return self.read_data(query)

    def _parse_gcs_path(self, path: str) -> str:
        """Parse GCS path and remove gs:// prefix if present."""
        if path.startswith('gs://'):
            # Remove gs:// prefix
            path = path[5:]
            # Remove bucket name if present
            if path.startswith(self.bucket + '/'):
                path = path[len(self.bucket) + 1:]
        return path

    def _read_blob_content(self, blob_name: str) -> bytes:
        """Read blob content from GCS."""
        blob = self.bucket_obj.blob(blob_name)
        return blob.download_as_bytes()

    def _read_parquet(self, path: str) -> pd.DataFrame:
        """Read Parquet file(s) from GCS."""
        if '*' in path:
            return self._read_multiple_files(path, 'parquet')

        # For single parquet file, use GCS path directly
        gcs_path = f"gs://{self.bucket}/{path}"
        return pd.read_parquet(gcs_path)

    def _read_csv(self, path: str) -> pd.DataFrame:
        """Read CSV file from GCS."""
        if '*' in path:
            return self._read_multiple_files(path, 'csv')

        content = self._read_blob_content(path)
        return pd.read_csv(BytesIO(content))

    def _read_json(self, path: str) -> pd.DataFrame:
        """Read JSON file from GCS."""
        if '*' in path:
            return self._read_multiple_files(path, 'json')

        content = self._read_blob_content(path)
        return pd.read_json(BytesIO(content))

    def _read_multiple_files(self, path_pattern: str, file_format: str) -> pd.DataFrame:
        """Read multiple files matching pattern."""
        # Get directory prefix
        prefix = path_pattern.split('*')[0]

        # List blobs
        blobs = self.client.list_blobs(self.bucket_obj, prefix=prefix)

        dfs = []
        for blob in blobs:
            if blob.name.endswith(f'.{file_format}'):
                try:
                    if file_format == 'parquet':
                        gcs_path = f"gs://{self.bucket}/{blob.name}"
                        df = pd.read_parquet(gcs_path)
                    elif file_format == 'csv':
                        content = blob.download_as_bytes()
                        df = pd.read_csv(BytesIO(content))
                    elif file_format == 'json':
                        content = blob.download_as_bytes()
                        df = pd.read_json(BytesIO(content))
                    else:
                        continue

                    dfs.append(df)
                except Exception as e:
                    self.logger.warning(f"Failed to read {blob.name}: {str(e)}")

        if not dfs:
            return pd.DataFrame()

        return pd.concat(dfs, ignore_index=True)
