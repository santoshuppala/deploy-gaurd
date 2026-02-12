# Pre/Post Deployment Validation Framework for Big Data

A production-ready, extensible framework for pre and post deployment validation of big data pipelines supporting Apache Spark, Hive/Hadoop, and cloud storage platforms (S3, ADLS, GCS).

## Features

- **Multiple Data Sources**: Apache Spark, Hive, S3, Azure ADLS, Google Cloud Storage
- **Comprehensive Validations**: Row count reconciliation, data quality checks, schema validation, business rule validation
- **Flexible Configuration**: YAML-based configuration with environment variable support
- **Rich Reporting**: JSON, HTML (with charts), Console, Email, and Database output
- **Production Ready**: Comprehensive error handling, logging, retry logic, and parallel execution
- **Extensible Architecture**: Easy to add custom connectors, validators, and reporters

## Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/validation-framework.git
cd validation-framework

# Install dependencies
pip install -r requirements.txt

# Or install as a package
pip install -e .
```

### Basic Usage

1. **Create a configuration file** (or use an example):

```bash
python scripts/run_validation.py generate-config config/my_validation.yaml --type simple
```

2. **Edit the configuration** to match your data sources and validation requirements

3. **Run validations**:

```bash
python scripts/run_validation.py run config/my_validation.yaml
```

### Example Configuration

```yaml
connections:
  - name: source_spark
    type: spark
    enabled: true
    config:
      app_name: "Data Validation"
      master: "local[*]"

  - name: target_hive
    type: hive
    enabled: true
    config:
      host: "${HIVE_HOST}"
      port: 10000
      database: "production"

validations:
  - name: "Customer Row Count"
    type: row_count
    enabled: true
    source: source_spark
    target: target_hive
    source_query: "SELECT COUNT(*) FROM customers"
    target_query: "SELECT COUNT(*) FROM production.customers"
    thresholds:
      max_difference_percent: 0.1

reporters:
  - type: console
    enabled: true

  - type: json
    enabled: true
    output_path: "output/json/results.json"

  - type: html
    enabled: true
    output_path: "output/reports/report.html"
    config:
      include_charts: true
```

## Architecture

### Core Components

```
┌─────────────────────────────────────────────────────────────┐
│                    Validation Engine                        │
│  (Orchestrates connectors, validators, and reporters)       │
└──────────────┬──────────────┬──────────────┬────────────────┘
               │              │              │
       ┌───────▼──────┐  ┌───▼────────┐  ┌──▼──────────┐
       │  Connectors  │  │ Validators │  │  Reporters  │
       │  - Spark     │  │ - RowCount │  │  - JSON     │
       │  - Hive      │  │ - Quality  │  │  - HTML     │
       │  - S3        │  │ - Schema   │  │  - Console  │
       │  - ADLS      │  │ - Business │  │  - Email    │
       │  - GCS       │  │            │  │  - Database │
       └──────────────┘  └────────────┘  └─────────────┘
```

### Design Principles

- **SOLID Principles**: Maintainable and extensible code
- **Factory Pattern**: Easy addition of new components
- **Abstract Base Classes**: Clear contracts and interfaces
- **Configuration-Driven**: Flexible YAML-based configuration
- **Production-Ready**: Comprehensive error handling and logging

## Validation Types

### 1. Row Count Validation

Compares row counts between source and target with configurable thresholds:

```yaml
- name: "Table Row Count"
  type: row_count
  source_query: "SELECT COUNT(*) FROM source_table"
  target_query: "SELECT COUNT(*) FROM target_table"
  thresholds:
    max_difference_percent: 1.0
    fail_on_zero_source: true
```

### 2. Data Quality Validation

Checks for nulls, duplicates, and invalid records:

```yaml
- name: "Data Quality Check"
  type: data_quality
  source_query: "SELECT * FROM source_table"
  target_query: "SELECT * FROM target_table"
  thresholds:
    max_null_percent: 5.0
    max_duplicate_percent: 1.0
  metadata:
    check_columns: ["id", "email"]
    primary_key: "id"
```

### 3. Schema Validation

Validates schema compatibility between source and target:

```yaml
- name: "Schema Validation"
  type: schema
  source_table: "source_table"
  target_table: "target_table"
  metadata:
    critical_columns: ["id", "timestamp"]
```

### 4. Business Rule Validation

Validates custom business logic and aggregations:

```yaml
- name: "Revenue Validation"
  type: business_rule
  source_query: "SELECT SUM(amount) FROM orders"
  target_query: "SELECT SUM(revenue) FROM summary"
  thresholds:
    max_difference_percent: 0.01
  metadata:
    rule_type: "aggregation"
```

## CLI Commands

```bash
# Run validations
python scripts/run_validation.py run config/validation_config.yaml

# Validate configuration without running
python scripts/run_validation.py validate-config config/validation_config.yaml

# List supported connectors
python scripts/run_validation.py list-connectors

# List supported validators
python scripts/run_validation.py list-validators

# List supported reporters
python scripts/run_validation.py list-reporters

# Generate sample configuration
python scripts/run_validation.py generate-config config/my_config.yaml --type simple

# Run with options
python scripts/run_validation.py run config/validation_config.yaml \
  --log-level DEBUG \
  --parallel \
  --max-workers 8 \
  --fail-fast
```

## Configuration

### Environment Variables

Use environment variables in your configuration:

```yaml
connections:
  - name: prod_hive
    type: hive
    config:
      host: "${HIVE_HOST:localhost}"
      port: ${HIVE_PORT:10000}
      username: "${HIVE_USER}"
      password: "${HIVE_PASSWORD}"
```

Syntax: `${VAR_NAME}` or `${VAR_NAME:default_value}`

### Settings

```yaml
settings:
  parallel_execution: false
  max_workers: 4
  log_level: "INFO"
  log_file: "output/logs/validation.log"
  continue_on_error: true
  query_timeout_seconds: 300
  connection_retry_attempts: 3
```

## Extending the Framework

### Adding a Custom Connector

```python
from src.validation_framework.core.base_connector import BaseConnector

class MyCustomConnector(BaseConnector):
    def connect(self):
        # Implementation
        pass

    def disconnect(self):
        # Implementation
        pass

    def read_data(self, query, limit=None):
        # Implementation
        pass

# Register the connector
from src.validation_framework.connectors.connector_factory import ConnectorFactory
from src.validation_framework.models.enums import ConnectorType

ConnectorFactory.register(ConnectorType.CUSTOM, MyCustomConnector)
```

### Adding a Custom Validator

```python
from src.validation_framework.core.base_validator import BaseValidator

class MyCustomValidator(BaseValidator):
    def get_validation_type(self):
        return ValidationType.CUSTOM

    def _execute_validation(self, source_connector, target_connector):
        # Implementation
        pass

# Register the validator
from src.validation_framework.validators.validator_factory import ValidatorFactory

ValidatorFactory.register(ValidationType.CUSTOM, MyCustomValidator)
```

## Testing

```bash
# Run all tests
pytest tests/

# Run with coverage
pytest tests/ --cov=src/validation_framework --cov-report=html

# Run specific test file
pytest tests/unit/test_validators.py
```

## Project Structure

```
PrePost_Validation_generic/
├── src/validation_framework/
│   ├── core/                  # Core abstractions
│   ├── connectors/            # Data source connectors
│   ├── validators/            # Validation implementations
│   ├── models/                # Data models
│   ├── reporters/             # Report generators
│   └── utils/                 # Utilities
├── config/                    # Configuration files
├── scripts/                   # CLI scripts
├── tests/                     # Test suite
├── output/                    # Generated outputs
├── requirements.txt           # Dependencies
├── setup.py                   # Package setup
└── README.md                  # This file
```

## Best Practices

1. **Use meaningful validation names**: Makes reports easier to understand
2. **Set appropriate thresholds**: Balance between strictness and flexibility
3. **Enable parallel execution**: For large validation suites
4. **Use environment variables**: For sensitive credentials
5. **Monitor validation history**: Store results in database for trending
6. **Test configurations**: Use `validate-config` before running

## Troubleshooting

### Connection Errors

```bash
# Test connectivity separately
from src.validation_framework.connectors.spark_connector import SparkConnector

connector = SparkConnector("test", {"master": "local[*]", "app_name": "Test"})
connector.test_connection()
```

### Configuration Errors

```bash
# Validate configuration
python scripts/run_validation.py validate-config config/validation_config.yaml
```

### View Detailed Logs

```bash
# Set log level to DEBUG
python scripts/run_validation.py run config/validation_config.yaml --log-level DEBUG

# Check log files
tail -f output/logs/validation_*.log
```

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

- Documentation: [Link to docs]
- Issues: [GitHub Issues](https://github.com/yourusername/validation-framework/issues)
- Email: your.email@example.com

## Changelog

### Version 1.0.0 (2024-01-01)
- Initial release
- Support for Spark, Hive, S3, ADLS, GCS
- Row count, data quality, schema, and business rule validations
- JSON, HTML, Console, Email, and Database reporters
- CLI interface with multiple commands
- Comprehensive documentation and examples
# deploy-gaurd
# deploy-gaurd
