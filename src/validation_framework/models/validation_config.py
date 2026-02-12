"""
Pydantic models for configuration validation.
Defines the structure and validation rules for YAML configuration files.
"""
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field, validator


class ConnectionConfig(BaseModel):
    """Configuration for a data source connection."""
    name: str
    type: str = Field(..., description="Connector type: spark, hive, s3, adls, gcs, jdbc")
    enabled: bool = True
    config: Dict[str, Any] = Field(default_factory=dict)

    @validator('type')
    def validate_type(cls, v):
        valid_types = ['spark', 'hive', 's3', 'adls', 'gcs', 'jdbc']
        if v.lower() not in valid_types:
            raise ValueError(f"Invalid connector type: {v}. Must be one of {valid_types}")
        return v.lower()


class ThresholdConfig(BaseModel):
    """Threshold configuration for validation."""
    max_difference_percent: Optional[float] = None
    max_difference_absolute: Optional[int] = None
    fail_on_zero_source: bool = False
    fail_on_zero_target: bool = False
    max_null_percent: Optional[float] = None
    max_duplicate_percent: Optional[float] = None
    max_invalid_percent: Optional[float] = None


class ValidationConfig(BaseModel):
    """Configuration for a single validation."""
    name: str
    type: str = Field(..., description="Validation type: row_count, data_quality, schema, business_rule, new_column")
    enabled: bool = True
    source: str = Field(..., description="Source connection name")
    target: str = Field(..., description="Target connection name")
    source_query: Optional[str] = None
    target_query: Optional[str] = None
    source_table: Optional[str] = None
    target_table: Optional[str] = None
    thresholds: ThresholdConfig = Field(default_factory=ThresholdConfig)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @validator('type')
    def validate_type(cls, v):
        valid_types = ['row_count', 'data_quality', 'schema', 'business_rule', 'new_column']
        if v.lower() not in valid_types:
            raise ValueError(f"Invalid validation type: {v}. Must be one of {valid_types}")
        return v.lower()

    @validator('source_query', 'target_query', always=True)
    def validate_query_or_table(cls, v, values):
        """Ensure either query or table is provided."""
        if 'source_table' in values and not v and not values.get('source_table'):
            raise ValueError("Either query or table must be provided")
        return v


class ReporterConfig(BaseModel):
    """Configuration for a report generator."""
    type: str = Field(..., description="Reporter type: json, html, console, email, database")
    enabled: bool = True
    output_path: Optional[str] = None
    config: Dict[str, Any] = Field(default_factory=dict)

    @validator('type')
    def validate_type(cls, v):
        valid_types = ['json', 'html', 'console', 'email', 'database']
        if v.lower() not in valid_types:
            raise ValueError(f"Invalid reporter type: {v}. Must be one of {valid_types}")
        return v.lower()


class SettingsConfig(BaseModel):
    """Global settings for validation execution."""
    parallel_execution: bool = False
    max_workers: int = 4
    log_level: str = "INFO"
    log_file: Optional[str] = "output/logs/validation.log"
    continue_on_error: bool = True
    query_timeout_seconds: int = 300
    connection_retry_attempts: int = 3
    connection_retry_delay_seconds: int = 5

    @validator('log_level')
    def validate_log_level(cls, v):
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if v.upper() not in valid_levels:
            raise ValueError(f"Invalid log level: {v}. Must be one of {valid_levels}")
        return v.upper()

    @validator('max_workers')
    def validate_max_workers(cls, v):
        if v < 1:
            raise ValueError("max_workers must be at least 1")
        return v


class FrameworkConfig(BaseModel):
    """Root configuration model for the entire validation framework."""
    connections: List[ConnectionConfig]
    validations: List[ValidationConfig]
    reporters: List[ReporterConfig]
    settings: SettingsConfig = Field(default_factory=SettingsConfig)

    @validator('connections')
    def validate_unique_connection_names(cls, v):
        names = [conn.name for conn in v]
        if len(names) != len(set(names)):
            raise ValueError("Connection names must be unique")
        return v

    @validator('validations')
    def validate_connection_references(cls, v, values):
        """Ensure validations reference existing connections."""
        if 'connections' in values:
            conn_names = {conn.name for conn in values['connections']}
            for validation in v:
                if validation.source not in conn_names:
                    raise ValueError(f"Validation '{validation.name}' references unknown source: {validation.source}")
                if validation.target not in conn_names:
                    raise ValueError(f"Validation '{validation.name}' references unknown target: {validation.target}")
        return v

    class Config:
        extra = 'allow'  # Allow extra fields for extensibility
