"""
AWS S3 connector implementation.
Provides connectivity to S3 for data validation.
"""
from typing import Dict, Any, Optional
import pandas as pd
from io import BytesIO

from ..core.base_connector import BaseConnector
from ..core.exceptions import ConnectionError, ConnectorError


class S3Connector(BaseConnector):
    """Connector for AWS S3."""

    def __init__(self, name: str, config: Dict[str, Any]):
        """
        Initialize S3 connector.

        Args:
            name: Connector instance name
            config: Configuration dictionary with S3 settings
        """
        super().__init__(name, config)
        self.s3_client = None
        self.s3_resource = None
        self.bucket = config.get('bucket')
        self.region = config.get('region', 'us-east-1')
        self.access_key = config.get('access_key')
        self.secret_key = config.get('secret_key')
        self.prefix = config.get('prefix', '')

    def connect(self) -> None:
        """Establish connection to S3."""
        try:
            import boto3

            self.logger.info(f"Connecting to S3 bucket: {self.bucket} in {self.region}")

            # Create S3 client and resource
            session_config = {
                'region_name': self.region
            }

            if self.access_key and self.secret_key:
                session_config['aws_access_key_id'] = self.access_key
                session_config['aws_secret_access_key'] = self.secret_key

            self.s3_client = boto3.client('s3', **session_config)
            self.s3_resource = boto3.resource('s3', **session_config)

            # Test connection by checking if bucket exists
            self.s3_client.head_bucket(Bucket=self.bucket)

            self._connected = True
            self.logger.info(f"Connected to S3 bucket '{self.bucket}' successfully")

        except ImportError:
            raise ConnectionError(
                "boto3 is not installed. Install it with: pip install boto3",
                connector_type="s3"
            )
        except Exception as e:
            raise ConnectionError(
                f"Failed to connect to S3: {str(e)}",
                connector_type="s3",
                connection_details={'bucket': self.bucket, 'region': self.region},
                original_error=e
            )

    def disconnect(self) -> None:
        """Close S3 connection."""
        self.s3_client = None
        self.s3_resource = None
        self._connected = False
        self.logger.info("Disconnected from S3")

    def read_data(self, query: str, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Read data from S3 path.

        Args:
            query: S3 path (e.g., 's3://bucket/path/*.parquet' or 'path/to/file.csv')
            limit: Maximum number of rows to return

        Returns:
            Pandas DataFrame with data
        """
        if not self._connected:
            raise ConnectorError("Not connected to S3", connector_name=self.name)

        try:
            # Parse S3 path
            s3_path = self._parse_s3_path(query)

            # Read files based on extension
            if s3_path.endswith('.parquet') or 'parquet' in s3_path:
                df = self._read_parquet(s3_path)
            elif s3_path.endswith('.csv'):
                df = self._read_csv(s3_path)
            elif s3_path.endswith('.json'):
                df = self._read_json(s3_path)
            else:
                # Try to read as parquet by default
                df = self._read_parquet(s3_path)

            if limit:
                df = df.head(limit)

            self.logger.info(f"Retrieved {len(df)} rows from S3")
            return df

        except Exception as e:
            raise ConnectorError(
                f"Failed to read data from S3: {str(e)}",
                connector_name=self.name,
                operation="read_data",
                original_error=e
            )

    def get_row_count(self, table_or_path: str) -> int:
        """
        Get row count from S3 file(s).

        Args:
            table_or_path: S3 path

        Returns:
            Number of rows
        """
        if not self._connected:
            raise ConnectorError("Not connected to S3", connector_name=self.name)

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
        Get schema from S3 file(s).

        Args:
            table_or_path: S3 path

        Returns:
            Dictionary mapping column names to data types
        """
        if not self._connected:
            raise ConnectorError("Not connected to S3", connector_name=self.name)

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
        Execute query (for S3, this just reads data).

        Args:
            query: S3 path

        Returns:
            DataFrame with data
        """
        return self.read_data(query)

    def _parse_s3_path(self, path: str) -> str:
        """Parse S3 path and remove s3:// prefix if present."""
        if path.startswith('s3://'):
            # Remove s3:// prefix
            path = path[5:]
            # Remove bucket name if present
            if path.startswith(self.bucket + '/'):
                path = path[len(self.bucket) + 1:]
        return path

    def _read_parquet(self, path: str) -> pd.DataFrame:
        """Read Parquet file(s) from S3."""
        import pyarrow.parquet as pq

        # Check if path contains wildcards
        if '*' in path:
            return self._read_multiple_files(path, 'parquet')

        s3_path = f"s3://{self.bucket}/{path}"
        return pd.read_parquet(s3_path)

    def _read_csv(self, path: str) -> pd.DataFrame:
        """Read CSV file from S3."""
        obj = self.s3_client.get_object(Bucket=self.bucket, Key=path)
        return pd.read_csv(BytesIO(obj['Body'].read()))

    def _read_json(self, path: str) -> pd.DataFrame:
        """Read JSON file from S3."""
        obj = self.s3_client.get_object(Bucket=self.bucket, Key=path)
        return pd.read_json(BytesIO(obj['Body'].read()))

    def _read_multiple_files(self, path_pattern: str, file_format: str) -> pd.DataFrame:
        """Read multiple files matching pattern."""
        # Get directory prefix
        prefix = path_pattern.split('*')[0]

        # List objects
        objects = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)

        if 'Contents' not in objects:
            return pd.DataFrame()

        # Read and concatenate all matching files
        dfs = []
        for obj in objects['Contents']:
            key = obj['Key']
            if key.endswith(f'.{file_format}'):
                try:
                    if file_format == 'parquet':
                        df = pd.read_parquet(f"s3://{self.bucket}/{key}")
                    elif file_format == 'csv':
                        df = self._read_csv(key)
                    elif file_format == 'json':
                        df = self._read_json(key)
                    else:
                        continue
                    dfs.append(df)
                except Exception as e:
                    self.logger.warning(f"Failed to read {key}: {str(e)}")

        if not dfs:
            return pd.DataFrame()

        return pd.concat(dfs, ignore_index=True)
