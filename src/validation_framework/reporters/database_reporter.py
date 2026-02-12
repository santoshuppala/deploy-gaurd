"""
Database reporter implementation.
Stores validation results in a database.
"""
from datetime import datetime
from typing import Dict, Any
import json

from .base_reporter import BaseReporter
from ..models.validation_result import ValidationSummary, ValidationResult
from ..models.enums import ReporterType


class DatabaseReporter(BaseReporter):
    """Stores validation results in a database."""

    def get_reporter_type(self) -> ReporterType:
        """Return reporter type."""
        return ReporterType.DATABASE

    def generate_report(self, summary: ValidationSummary) -> str:
        """
        Store validation results in database.

        Args:
            summary: ValidationSummary with all results

        Returns:
            Status message
        """
        connection_string = self.config.get('connection_string')
        table_name = self.config.get('table_name', 'validation_results')
        schema = self.config.get('schema')

        if not connection_string:
            raise ValueError("Database connection_string is required")

        # Determine database type from connection string
        if 'postgresql' in connection_string or 'postgres' in connection_string:
            return self._store_postgres(summary, connection_string, schema, table_name)
        elif 'mysql' in connection_string:
            return self._store_mysql(summary, connection_string, schema, table_name)
        elif 'sqlite' in connection_string:
            return self._store_sqlite(summary, connection_string, table_name)
        else:
            raise ValueError(f"Unsupported database type in connection string: {connection_string}")

    def _store_postgres(self, summary: ValidationSummary, conn_str: str,
                       schema: str, table_name: str) -> str:
        """Store results in PostgreSQL."""
        try:
            import psycopg2
            from psycopg2.extras import execute_values

            self.logger.info(f"Connecting to PostgreSQL database")

            with psycopg2.connect(conn_str) as conn:
                with conn.cursor() as cursor:
                    # Create table if not exists
                    full_table = f"{schema}.{table_name}" if schema else table_name
                    self._create_postgres_table(cursor, full_table)

                    # Insert summary
                    summary_id = self._insert_summary(cursor, summary, full_table)

                    # Insert individual results
                    self._insert_results(cursor, summary.results, summary_id, full_table)

                    conn.commit()

            self.logger.info(f"Results stored in database: {full_table}")
            return f"Results stored in {full_table}"

        except ImportError:
            raise ImportError("psycopg2 is not installed. Install it with: pip install psycopg2-binary")
        except Exception as e:
            error_msg = f"Failed to store results in PostgreSQL: {str(e)}"
            self.logger.error(error_msg)
            raise Exception(error_msg)

    def _store_mysql(self, summary: ValidationSummary, conn_str: str,
                    schema: str, table_name: str) -> str:
        """Store results in MySQL."""
        try:
            import mysql.connector

            self.logger.info(f"Connecting to MySQL database")

            # Parse connection string (simplified)
            conn = mysql.connector.connect(
                host=conn_str.split('@')[1].split(':')[0] if '@' in conn_str else 'localhost',
                database=schema or 'validation',
                user=self.config.get('username', 'root'),
                password=self.config.get('password', '')
            )

            cursor = conn.cursor()

            # Create table if not exists
            self._create_mysql_table(cursor, table_name)

            # Insert summary
            summary_id = self._insert_summary(cursor, summary, table_name)

            # Insert results
            self._insert_results(cursor, summary.results, summary_id, table_name)

            conn.commit()
            cursor.close()
            conn.close()

            self.logger.info(f"Results stored in database: {table_name}")
            return f"Results stored in {table_name}"

        except ImportError:
            raise ImportError("mysql-connector-python is not installed. "
                            "Install it with: pip install mysql-connector-python")
        except Exception as e:
            error_msg = f"Failed to store results in MySQL: {str(e)}"
            self.logger.error(error_msg)
            raise Exception(error_msg)

    def _store_sqlite(self, summary: ValidationSummary, conn_str: str,
                     table_name: str) -> str:
        """Store results in SQLite."""
        try:
            import sqlite3

            # Extract database path from connection string
            db_path = conn_str.replace('sqlite:///', '').replace('sqlite://', '')

            self.logger.info(f"Connecting to SQLite database: {db_path}")

            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            # Create table if not exists
            self._create_sqlite_table(cursor, table_name)

            # Insert summary
            summary_id = self._insert_summary(cursor, summary, table_name)

            # Insert results
            self._insert_results(cursor, summary.results, summary_id, table_name)

            conn.commit()
            conn.close()

            self.logger.info(f"Results stored in database: {db_path}")
            return f"Results stored in {db_path}"

        except Exception as e:
            error_msg = f"Failed to store results in SQLite: {str(e)}"
            self.logger.error(error_msg)
            raise Exception(error_msg)

    def _create_postgres_table(self, cursor, table_name: str) -> None:
        """Create PostgreSQL table for validation results."""
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name}_summary (
                id SERIAL PRIMARY KEY,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                total_validations INTEGER,
                passed INTEGER,
                failed INTEGER,
                warnings INTEGER,
                errors INTEGER,
                skipped INTEGER,
                success_rate FLOAT,
                total_execution_time FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name}_details (
                id SERIAL PRIMARY KEY,
                summary_id INTEGER REFERENCES {table_name}_summary(id),
                name VARCHAR(500),
                validation_type VARCHAR(50),
                status VARCHAR(50),
                source_name VARCHAR(200),
                target_name VARCHAR(200),
                source_count BIGINT,
                target_count BIGINT,
                difference BIGINT,
                difference_percent FLOAT,
                execution_time FLOAT,
                error_message TEXT,
                metadata JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

    def _create_mysql_table(self, cursor, table_name: str) -> None:
        """Create MySQL table for validation results."""
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name}_summary (
                id INT AUTO_INCREMENT PRIMARY KEY,
                start_time DATETIME,
                end_time DATETIME,
                total_validations INT,
                passed INT,
                failed INT,
                warnings INT,
                errors INT,
                skipped INT,
                success_rate FLOAT,
                total_execution_time FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name}_details (
                id INT AUTO_INCREMENT PRIMARY KEY,
                summary_id INT,
                name VARCHAR(500),
                validation_type VARCHAR(50),
                status VARCHAR(50),
                source_name VARCHAR(200),
                target_name VARCHAR(200),
                source_count BIGINT,
                target_count BIGINT,
                difference BIGINT,
                difference_percent FLOAT,
                execution_time FLOAT,
                error_message TEXT,
                metadata JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (summary_id) REFERENCES {table_name}_summary(id)
            )
        """)

    def _create_sqlite_table(self, cursor, table_name: str) -> None:
        """Create SQLite table for validation results."""
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name}_summary (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                start_time TEXT,
                end_time TEXT,
                total_validations INTEGER,
                passed INTEGER,
                failed INTEGER,
                warnings INTEGER,
                errors INTEGER,
                skipped INTEGER,
                success_rate REAL,
                total_execution_time REAL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name}_details (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                summary_id INTEGER,
                name TEXT,
                validation_type TEXT,
                status TEXT,
                source_name TEXT,
                target_name TEXT,
                source_count INTEGER,
                target_count INTEGER,
                difference INTEGER,
                difference_percent REAL,
                execution_time REAL,
                error_message TEXT,
                metadata TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (summary_id) REFERENCES {table_name}_summary(id)
            )
        """)

    def _insert_summary(self, cursor, summary: ValidationSummary,
                       table_name: str) -> int:
        """Insert summary record and return its ID."""
        cursor.execute(f"""
            INSERT INTO {table_name}_summary
            (start_time, end_time, total_validations, passed, failed, warnings,
             errors, skipped, success_rate, total_execution_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            summary.start_time,
            summary.end_time,
            summary.total_validations,
            summary.passed,
            summary.failed,
            summary.warnings,
            summary.errors,
            summary.skipped,
            summary.success_rate,
            summary.total_execution_time_seconds
        ))

        return cursor.fetchone()[0]

    def _insert_results(self, cursor, results: list, summary_id: int,
                       table_name: str) -> None:
        """Insert individual validation results."""
        for result in results:
            cursor.execute(f"""
                INSERT INTO {table_name}_details
                (summary_id, name, validation_type, status, source_name, target_name,
                 source_count, target_count, difference, difference_percent,
                 execution_time, error_message, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                summary_id,
                result.name,
                str(result.validation_type),
                str(result.status),
                result.source_name,
                result.target_name,
                result.source_count,
                result.target_count,
                result.difference,
                result.difference_percent,
                result.execution_time_seconds,
                result.error_message,
                json.dumps(result.metadata) if result.metadata else None
            ))
