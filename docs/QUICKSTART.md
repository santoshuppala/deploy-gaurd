# Quick Start Guide

This guide will help you get started with the Pre/Post Deployment Validation Framework in 5 minutes.

## Prerequisites

- Python 3.8 or higher
- pip package manager
- (Optional) Apache Spark for Spark validations
- (Optional) Hive connection for Hive validations

## Installation

### 1. Clone or Download the Framework

```bash
cd /path/to/PrePost_Validation_generic
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

Or install in development mode:

```bash
pip install -e .
```

## Your First Validation

### Step 1: Generate a Configuration File

```bash
python scripts/run_validation.py generate-config config/my_first_validation.yaml --type simple
```

This creates a simple Spark validation configuration.

### Step 2: Edit the Configuration

Open `config/my_first_validation.yaml` and customize it:

```yaml
connections:
  - name: source_spark
    type: spark
    enabled: true
    config:
      app_name: "My First Validation"
      master: "local[*]"

  - name: target_spark
    type: spark
    enabled: true
    config:
      app_name: "My First Validation"
      master: "local[*]"

validations:
  - name: "Row Count Check"
    type: row_count
    enabled: true
    source: source_spark
    target: target_spark
    source_query: "SELECT COUNT(*) FROM source_table"
    target_query: "SELECT COUNT(*) FROM target_table"
    thresholds:
      max_difference_percent: 1.0

reporters:
  - type: console
    enabled: true

  - type: json
    enabled: true
    output_path: "output/json/results.json"
```

### Step 3: Validate Configuration

```bash
python scripts/run_validation.py validate-config config/my_first_validation.yaml
```

### Step 4: Run Validation

```bash
python scripts/run_validation.py run config/my_first_validation.yaml
```

## Common Use Cases

### Use Case 1: S3 to Hive Migration Validation

```bash
# Generate S3-to-Hive template
python scripts/run_validation.py generate-config config/s3_to_hive.yaml --type s3-to-hive

# Edit config with your S3 and Hive details
# Then run
python scripts/run_validation.py run config/s3_to_hive.yaml
```

### Use Case 2: Data Quality Checks

Add to your validation configuration:

```yaml
validations:
  - name: "Data Quality Check"
    type: data_quality
    enabled: true
    source: source_spark
    target: target_spark
    source_query: "SELECT * FROM source_table"
    target_query: "SELECT * FROM target_table"
    thresholds:
      max_null_percent: 5.0
      max_duplicate_percent: 1.0
    metadata:
      check_columns: ["id", "email"]
      primary_key: "id"
```

### Use Case 3: Schema Validation

```yaml
validations:
  - name: "Schema Check"
    type: schema
    enabled: true
    source: source_spark
    target: target_hive
    source_table: "source_database.table_name"
    target_table: "target_database.table_name"
    metadata:
      critical_columns: ["id", "timestamp", "amount"]
```

## Environment Variables

Set credentials and connection details via environment variables:

```bash
# Create .env file
cat > .env << EOF
HIVE_HOST=your-hive-host.com
HIVE_PORT=10000
HIVE_USER=your_username
HIVE_PASSWORD=your_password

AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
S3_BUCKET=your-bucket-name

SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
EMAIL_SENDER=alerts@yourcompany.com
EMAIL_RECIPIENT=team@yourcompany.com
EOF

# Load environment variables
export $(cat .env | xargs)

# Or use python-dotenv (automatically loaded by framework)
```

## CLI Commands Reference

```bash
# Run validations
python scripts/run_validation.py run config/validation_config.yaml

# Validate config
python scripts/run_validation.py validate-config config/validation_config.yaml

# List available connectors
python scripts/run_validation.py list-connectors

# List available validators
python scripts/run_validation.py list-validators

# List available reporters
python scripts/run_validation.py list-reporters

# Generate sample config
python scripts/run_validation.py generate-config config/new_config.yaml

# Run with debug logging
python scripts/run_validation.py run config/validation_config.yaml --log-level DEBUG

# Run in parallel
python scripts/run_validation.py run config/validation_config.yaml --parallel --max-workers 8

# Fail fast (stop on first error)
python scripts/run_validation.py run config/validation_config.yaml --fail-fast
```

## Viewing Results

### Console Output

Results are displayed in the console with color coding:
- ✓ Green = Passed
- ⚠ Yellow = Warning
- ✗ Red = Failed

### JSON Report

```bash
# View JSON report
cat output/json/results.json | jq .
```

### HTML Report

```bash
# Open in browser
open output/reports/validation_report.html
```

## Docker Usage

### Build Image

```bash
docker build -t validation-framework .
```

### Run Validation

```bash
docker run \
  -v $(pwd)/config:/app/config \
  -v $(pwd)/output:/app/output \
  -e HIVE_HOST=your-host \
  -e HIVE_USER=your-user \
  validation-framework run config/validation_config.yaml
```

### Interactive Mode

```bash
docker run -it \
  -v $(pwd)/config:/app/config \
  -v $(pwd)/output:/app/output \
  validation-framework bash
```

## Troubleshooting

### Connection Errors

Test connectivity:

```python
python -c "
from src.validation_framework.connectors.spark_connector import SparkConnector
conn = SparkConnector('test', {'master': 'local[*]', 'app_name': 'Test'})
print('Success!' if conn.test_connection() else 'Failed!')
"
```

### Configuration Errors

Validate your config before running:

```bash
python scripts/run_validation.py validate-config config/validation_config.yaml
```

### Import Errors

Make sure the project root is in your Python path:

```bash
export PYTHONPATH=/path/to/PrePost_Validation_generic:$PYTHONPATH
```

Or install in development mode:

```bash
pip install -e .
```

## Next Steps

1. **Read the Full Documentation**: Check `README.md` for detailed information
2. **Explore Examples**: Look at `config/examples/` for more configuration examples
3. **Run Tests**: Execute `pytest tests/` to verify your installation
4. **Customize**: Add your own connectors, validators, or reporters

## Getting Help

- Check the logs: `tail -f output/logs/validation_*.log`
- Run with debug logging: `--log-level DEBUG`
- Review example configurations in `config/examples/`
- Read the full README.md

## Common Pitfall

**Issue**: "Module not found" errors

**Solution**:
```bash
export PYTHONPATH=$(pwd):$PYTHONPATH
# or
pip install -e .
```

---

You're now ready to start validating your data pipelines! 🚀
