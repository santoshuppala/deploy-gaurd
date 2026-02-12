# New Column Validation Guide

## Overview

The **New Column Validator** is designed to validate newly added columns during schema evolution, ETL enhancements, or data pipeline upgrades. It performs comprehensive checks to ensure new columns meet requirements and contain valid data.

## Use Cases

1. **Schema Evolution**: Validating columns added during table migrations
2. **ETL Enhancements**: Verifying derived/calculated columns
3. **Data Enrichment**: Validating enriched data fields
4. **Compliance Requirements**: Checking newly added compliance fields
5. **Feature Engineering**: Validating ML feature columns

## Validation Checks

The New Column Validator performs **8 comprehensive checks**:

### 1. **Column Existence Check**
Verifies the column:
- Does NOT exist in source (confirming it's truly new)
- DOES exist in target (confirming it was added)

### 2. **Data Type Validation**
Compares actual data type against expected:
- Type matching with normalization
- Cross-platform compatibility (Spark, Hive, Pandas)

### 3. **Nullability Validation**
Checks null value constraints:
- NOT NULL requirement enforcement
- Maximum null percentage threshold
- Null count and percentage reporting

### 4. **Default Value Validation**
Validates default value presence:
- Default value frequency check
- Minimum percentage threshold
- Value distribution analysis

### 5. **Value Range Validation** (Numeric Columns)
Checks numeric value boundaries:
- Minimum value constraint
- Maximum value constraint
- Range violation detection

### 6. **Allowed Values Validation** (Categorical Columns)
Validates categorical constraints:
- Whitelist of allowed values
- Invalid value detection
- Unique value count

### 7. **Pattern Validation** (String Columns)
Regex pattern matching:
- Pattern compliance checking
- Minimum match percentage
- Format validation (e.g., UUID, country codes)

### 8. **Metadata Validation**
Additional custom checks based on column configuration

## Configuration Schema

### Basic Configuration

```yaml
validations:
  - name: "New Column Validation"
    type: new_column
    enabled: true
    source: source_connection
    target: target_connection
    source_table: "source_table"
    target_table: "target_table"
    metadata:
      new_columns:
        - name: "column_name"
          # Configuration options here
```

### Column Configuration Options

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | Yes | Column name to validate |
| `expected_type` | string | No | Expected data type |
| `nullable` | boolean | No | Whether nulls are allowed (default: true) |
| `max_null_percent` | float | No | Maximum percentage of nulls (0-100) |
| `default_value` | any | No | Expected default value |
| `min_default_percent` | float | No | Minimum % of records with default |
| `min_value` | number | No | Minimum numeric value |
| `max_value` | number | No | Maximum numeric value |
| `allowed_values` | list | No | Whitelist of allowed values |
| `pattern` | string | No | Regex pattern for validation |
| `min_pattern_match_percent` | float | No | Min % matching pattern (default: 100) |
| `description` | string | No | Column description |

## Common Scenarios

### Scenario 1: Simple Column Addition

Validate a basic new column with type and nullability checks.

```yaml
metadata:
  new_columns:
    - name: "customer_segment"
      expected_type: "string"
      nullable: false
      description: "Customer segmentation category"
```

**Checks performed:**
- ✓ Column absent from source
- ✓ Column present in target
- ✓ Data type is string
- ✓ No null values found

---

### Scenario 2: Column with Default Value

Validate a status column that should have a default value.

```yaml
metadata:
  new_columns:
    - name: "processing_status"
      expected_type: "string"
      nullable: false
      default_value: "PENDING"
      min_default_percent: 80.0
      allowed_values: ["PENDING", "PROCESSING", "COMPLETED", "FAILED"]
```

**Checks performed:**
- ✓ Column structure checks
- ✓ At least 80% of records have "PENDING" status
- ✓ All values are from allowed list
- ✓ No null values

---

### Scenario 3: Numeric Column with Range

Validate a score column with value constraints.

```yaml
metadata:
  new_columns:
    - name: "data_quality_score"
      expected_type: "double"
      nullable: true
      min_value: 0.0
      max_value: 100.0
      max_null_percent: 10.0
      description: "Quality score between 0-100"
```

**Checks performed:**
- ✓ Data type is numeric
- ✓ All values between 0 and 100
- ✓ Null percentage under 10%
- ✓ No values outside valid range

---

### Scenario 4: Categorical Column

Validate an enum-like column with specific allowed values.

```yaml
metadata:
  new_columns:
    - name: "account_tier"
      expected_type: "integer"
      nullable: false
      default_value: 1
      min_value: 1
      max_value: 5
      allowed_values: [1, 2, 3, 4, 5]
      description: "Account tier (1-5)"
```

**Checks performed:**
- ✓ Integer data type
- ✓ Values only from [1,2,3,4,5]
- ✓ Range between 1-5
- ✓ Most records have default tier 1

---

### Scenario 5: String Pattern Validation

Validate formatted string columns (codes, IDs, etc.).

```yaml
metadata:
  new_columns:
    # UUID format
    - name: "pipeline_run_id"
      expected_type: "string"
      nullable: false
      pattern: "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
      min_pattern_match_percent: 100.0
      description: "UUID of pipeline run"

    # Country code format (ISO 2-letter)
    - name: "country_code"
      expected_type: "string"
      nullable: true
      pattern: "^[A-Z]{2}$"
      min_pattern_match_percent: 98.0
      max_null_percent: 5.0
      description: "ISO 2-letter country code"

    # Product category format
    - name: "product_category"
      expected_type: "string"
      nullable: false
      pattern: "^[A-Z]{3}-[0-9]{4}$"
      min_pattern_match_percent: 95.0
      description: "Category code (e.g., ELE-1234)"
```

**Checks performed:**
- ✓ Pattern compliance
- ✓ Match percentage threshold
- ✓ Format validation
- ✓ Invalid format detection

---

### Scenario 6: Multiple New Columns (Schema Evolution)

Validate multiple related columns added together.

```yaml
metadata:
  new_columns:
    # Privacy compliance fields
    - name: "gdpr_consent"
      expected_type: "boolean"
      nullable: false
      default_value: false

    - name: "consent_date"
      expected_type: "timestamp"
      nullable: true
      max_null_percent: 50.0

    - name: "data_retention_days"
      expected_type: "integer"
      nullable: false
      default_value: 365
      min_value: 30
      max_value: 3650

    # Geolocation enrichment
    - name: "country_code"
      expected_type: "string"
      nullable: true
      pattern: "^[A-Z]{2}$"

    - name: "timezone"
      expected_type: "string"
      nullable: true
      allowed_values:
        - "UTC"
        - "America/New_York"
        - "Europe/London"
        - "Asia/Tokyo"
```

**Benefits:**
- Validates all related columns together
- Ensures consistent data quality
- Comprehensive schema evolution validation

---

### Scenario 7: ETL Pipeline Metadata Columns

Validate technical/audit columns added by ETL processes.

```yaml
metadata:
  new_columns:
    - name: "etl_batch_id"
      expected_type: "string"
      nullable: false
      pattern: "^batch_[0-9]{8}_[0-9]{6}$"
      description: "ETL batch identifier"

    - name: "etl_processed_timestamp"
      expected_type: "timestamp"
      nullable: false
      description: "Processing timestamp"

    - name: "data_source"
      expected_type: "string"
      nullable: false
      allowed_values: ["API", "File", "Stream", "Manual"]

    - name: "record_hash"
      expected_type: "string"
      nullable: false
      pattern: "^[a-f0-9]{64}$"
      min_pattern_match_percent: 100.0
      description: "SHA256 hash of record"

    - name: "is_duplicate"
      expected_type: "boolean"
      nullable: false
      default_value: false
```

---

### Scenario 8: Derived/Calculated Columns

Validate columns computed from source data.

```yaml
metadata:
  new_columns:
    - name: "total_with_tax"
      expected_type: "decimal"
      nullable: false
      min_value: 0.0
      description: "Total amount including tax"

    - name: "is_high_value"
      expected_type: "boolean"
      nullable: false
      default_value: false
      description: "Flag for transactions over threshold"

    - name: "age_group"
      expected_type: "string"
      nullable: true
      allowed_values: ["0-18", "19-35", "36-50", "51-65", "66+"]
      description: "Calculated age group"

    - name: "loyalty_tier"
      expected_type: "integer"
      nullable: false
      default_value: 1
      allowed_values: [1, 2, 3, 4, 5]
      description: "Loyalty tier calculated from purchase history"
```

## Complete Example

Here's a complete configuration example:

```yaml
connections:
  - name: source_spark
    type: spark
    enabled: true
    config:
      master: "local[*]"

  - name: target_hive
    type: hive
    enabled: true
    config:
      host: "${HIVE_HOST}"
      port: 10000
      database: "production"

validations:
  - name: "Validate New Customer Enrichment Columns"
    type: new_column
    enabled: true
    source: source_spark
    target: target_hive
    source_table: "raw.customers"
    target_table: "production.customers_enriched"
    metadata:
      new_columns:
        # Customer segmentation (ML-derived)
        - name: "customer_segment"
          expected_type: "string"
          nullable: false
          allowed_values: ["Premium", "Standard", "Basic", "New"]
          default_value: "New"
          min_default_percent: 10.0
          description: "ML-based customer segment"

        # Purchase behavior metrics
        - name: "lifetime_value"
          expected_type: "decimal"
          nullable: true
          min_value: 0.0
          max_null_percent: 20.0
          description: "Customer lifetime value"

        - name: "churn_risk_score"
          expected_type: "double"
          nullable: false
          min_value: 0.0
          max_value: 1.0
          description: "Churn probability (0-1)"

        # Geolocation
        - name: "country_iso2"
          expected_type: "string"
          nullable: true
          pattern: "^[A-Z]{2}$"
          min_pattern_match_percent: 95.0
          max_null_percent: 5.0

        # Audit fields
        - name: "enriched_timestamp"
          expected_type: "timestamp"
          nullable: false

reporters:
  - type: console
    enabled: true

  - type: json
    enabled: true
    output_path: "output/json/new_column_results.json"

  - type: html
    enabled: true
    output_path: "output/reports/new_column_report.html"
```

## Output and Reporting

### Validation Result Structure

```json
{
  "name": "Validate New Customer Enrichment Columns",
  "validation_type": "new_column",
  "status": "PASSED",
  "metadata": {
    "new_columns_count": 5,
    "passed_count": 5,
    "failed_count": 0,
    "validation_results": [
      {
        "column": "customer_segment",
        "passed": true,
        "checks_passed": [
          "Column 'customer_segment' correctly absent from source",
          "Column 'customer_segment' exists in target",
          "Data type matches: string",
          "Nullability check passed: 0 nulls (0.0%)",
          "All values are from allowed set (4 unique values)",
          "Default value 'New' present in 15.2% of records"
        ],
        "checks_failed": [],
        "total_checks": 6
      }
    ]
  }
}
```

### Console Output

```
Validation: Validate New Customer Enrichment Columns
Status: ✓ PASSED

Column: customer_segment
  ✓ Column 'customer_segment' correctly absent from source
  ✓ Column 'customer_segment' exists in target
  ✓ Data type matches: string
  ✓ Nullability check passed: 0 nulls (0.0%)
  ✓ All values are from allowed set (4 unique values)
  ✓ Default value 'New' present in 15.2% of records
  Total: 6/6 checks passed

Column: lifetime_value
  ✓ Column 'lifetime_value' correctly absent from source
  ✓ Column 'lifetime_value' exists in target
  ✓ Data type matches: decimal
  ✓ Nullability check passed: 156 nulls (15.6%)
  ✓ Min value 0.00 >= 0.0
  Total: 5/5 checks passed

Summary: All 5 new columns validated successfully
```

## Best Practices

### 1. **Start Simple, Add Complexity**
Begin with basic checks (existence, type) and add more as needed.

### 2. **Use Realistic Thresholds**
Set thresholds based on actual data patterns, not ideals.

### 3. **Validate in Stages**
- First deployment: Relaxed thresholds + warnings
- Later: Stricter thresholds + failures

### 4. **Document Column Purpose**
Always include descriptions for future reference.

### 5. **Test with Sample Data**
Validate configuration with small dataset first.

### 6. **Monitor Over Time**
Track validation results to identify trends.

### 7. **Combine with Other Validations**
Use alongside row_count and data_quality validations.

## Troubleshooting

### Issue: Column Not Detected as New

**Problem**: Column exists in both source and target

**Solution**: Check if column truly is new, or update configuration

### Issue: Type Mismatch

**Problem**: Different type names across platforms

**Solution**: Use normalized types or check type mapping

### Issue: High Null Percentage

**Problem**: More nulls than expected

**Solution**:
- Check ETL logic
- Adjust `max_null_percent` if acceptable
- Investigate data quality issues

### Issue: Pattern Match Failures

**Problem**: Values don't match regex pattern

**Solution**:
- Verify pattern is correct
- Check for whitespace/case issues
- Lower `min_pattern_match_percent` temporarily

### Issue: Default Value Not Found

**Problem**: Default value percentage too low

**Solution**:
- Check ETL default value logic
- Adjust `min_default_percent` threshold
- Verify default value spelling

## Integration with CI/CD

```bash
# Run validation
python scripts/run_validation.py run config/new_column_validation.yaml

# Exit codes:
# 0 = All validations passed
# 1 = One or more validations failed
# 2 = Validations passed with warnings

# In CI/CD pipeline
if [ $? -eq 0 ]; then
  echo "New columns validated successfully"
  # Proceed with deployment
else
  echo "New column validation failed"
  # Block deployment
  exit 1
fi
```

## Advanced Usage

### Dynamic Column Lists

Load column configurations from external source:

```python
from src.validation_framework.validators.new_column_validator import NewColumnValidator

# Load columns from database/API
new_columns = fetch_column_metadata_from_catalog()

config = {
    'name': 'Dynamic Column Validation',
    'type': 'new_column',
    'source': 'source',
    'target': 'target',
    'source_table': 'table',
    'target_table': 'table',
    'metadata': {
        'new_columns': new_columns
    }
}

validator = NewColumnValidator(config)
```

### Custom Validation Logic

Extend the validator for custom checks:

```python
class CustomNewColumnValidator(NewColumnValidator):
    def _validate_new_column(self, col_name, col_config,
                            source_schema, target_schema, target_data):
        result = super()._validate_new_column(
            col_name, col_config, source_schema, target_schema, target_data
        )

        # Add custom checks
        if col_name == "special_column":
            # Custom validation logic
            pass

        return result
```

## Summary

The New Column Validator provides:
- ✅ Comprehensive column validation
- ✅ 8 different check types
- ✅ Flexible configuration options
- ✅ Detailed pass/fail reporting
- ✅ Support for all data types
- ✅ Pattern and range validation
- ✅ Production-ready error handling

Perfect for:
- Schema evolution validation
- ETL enhancement verification
- Data enrichment quality checks
- Compliance column validation
- Feature engineering validation
