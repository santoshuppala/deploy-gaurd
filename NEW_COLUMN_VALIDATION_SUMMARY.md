# New Column Validation - Implementation Summary

## Overview

Successfully added **comprehensive new column validation** capabilities to the validation framework. This validator is specifically designed for scenarios where columns are added during ETL processes, schema evolution, or data enrichment.

## What Was Added

### 1. New Validator Implementation
**File**: `src/validation_framework/validators/new_column_validator.py`

A complete validator that performs **8 different validation checks** on newly added columns:

1. ✅ **Column Existence Check** - Verifies column is absent from source, present in target
2. ✅ **Data Type Validation** - Checks expected vs actual data types
3. ✅ **Nullability Validation** - Enforces NOT NULL constraints and max null percentage
4. ✅ **Default Value Validation** - Validates default value presence and frequency
5. ✅ **Value Range Validation** - Checks min/max boundaries for numeric columns
6. ✅ **Allowed Values Validation** - Validates categorical column whitelists
7. ✅ **Pattern Validation** - Regex pattern matching for formatted strings
8. ✅ **Metadata Validation** - Custom checks based on configuration

### 2. Framework Integration
- ✅ Updated `ValidationType` enum to include `NEW_COLUMN`
- ✅ Registered validator in `ValidatorFactory`
- ✅ Updated Pydantic config model to accept `new_column` type
- ✅ Full integration with existing validation engine

### 3. Configuration Examples
**File**: `config/examples/new_column_validation.yaml`

5 comprehensive validation scenarios:
1. **Basic New Column Validation** - Simple column additions
2. **New Columns with Defaults** - Status columns, audit fields
3. **Derived Column Validation** - Calculated/computed fields
4. **Schema Evolution Validation** - Multiple related columns
5. **ETL Pipeline Metadata** - Technical/audit columns

### 4. Documentation
**File**: `docs/NEW_COLUMN_VALIDATION.md`

Complete 400+ line documentation including:
- 8 common validation scenarios with examples
- Full configuration reference
- Best practices and troubleshooting
- CI/CD integration guide
- Advanced usage patterns

## Validation Scenarios Implemented

### Scenario 1: Simple Column Addition
```yaml
- name: "customer_segment"
  expected_type: "string"
  nullable: false
  description: "Customer segmentation category"
```
**Use Case**: Basic column added during ETL
**Checks**: Existence, type, nullability

---

### Scenario 2: Column with Default Value
```yaml
- name: "processing_status"
  expected_type: "string"
  nullable: false
  default_value: "PENDING"
  min_default_percent: 80.0
  allowed_values: ["PENDING", "PROCESSING", "COMPLETED", "FAILED"]
```
**Use Case**: Status/state columns
**Checks**: Default value presence, allowed values, frequency

---

### Scenario 3: Numeric Column with Range
```yaml
- name: "data_quality_score"
  expected_type: "double"
  nullable: true
  min_value: 0.0
  max_value: 100.0
  max_null_percent: 10.0
```
**Use Case**: Score/rating columns
**Checks**: Value range, null percentage

---

### Scenario 4: Categorical Column
```yaml
- name: "account_tier"
  expected_type: "integer"
  nullable: false
  default_value: 1
  min_value: 1
  max_value: 5
  allowed_values: [1, 2, 3, 4, 5]
```
**Use Case**: Enum-like tier/level columns
**Checks**: Range, allowed values, default

---

### Scenario 5: String Pattern Validation
```yaml
# UUID format validation
- name: "pipeline_run_id"
  expected_type: "string"
  nullable: false
  pattern: "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
  min_pattern_match_percent: 100.0

# Country code validation
- name: "country_code"
  expected_type: "string"
  nullable: true
  pattern: "^[A-Z]{2}$"
  min_pattern_match_percent: 98.0

# Product category format
- name: "product_category"
  expected_type: "string"
  pattern: "^[A-Z]{3}-[0-9]{4}$"
  min_pattern_match_percent: 95.0
```
**Use Case**: Formatted codes, IDs, standardized strings
**Checks**: Regex pattern matching, format compliance

---

### Scenario 6: Multiple New Columns (Schema Evolution)
```yaml
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
  allowed_values: ["UTC", "America/New_York", "Europe/London", "Asia/Tokyo"]
```
**Use Case**: Related columns added together (GDPR, geolocation)
**Checks**: All checks combined across multiple columns

---

### Scenario 7: ETL Pipeline Metadata Columns
```yaml
- name: "etl_batch_id"
  expected_type: "string"
  nullable: false
  pattern: "^batch_[0-9]{8}_[0-9]{6}$"

- name: "etl_processed_timestamp"
  expected_type: "timestamp"
  nullable: false

- name: "data_source"
  expected_type: "string"
  nullable: false
  allowed_values: ["API", "File", "Stream", "Manual"]

- name: "record_hash"
  expected_type: "string"
  nullable: false
  pattern: "^[a-f0-9]{64}$"
  min_pattern_match_percent: 100.0

- name: "is_duplicate"
  expected_type: "boolean"
  nullable: false
  default_value: false
```
**Use Case**: Technical/audit columns added by ETL
**Checks**: Patterns, formats, technical constraints

---

### Scenario 8: Derived/Calculated Columns
```yaml
- name: "total_with_tax"
  expected_type: "decimal"
  nullable: false
  min_value: 0.0

- name: "is_high_value"
  expected_type: "boolean"
  nullable: false
  default_value: false

- name: "age_group"
  expected_type: "string"
  nullable: true
  allowed_values: ["0-18", "19-35", "36-50", "51-65", "66+"]

- name: "loyalty_tier"
  expected_type: "integer"
  nullable: false
  default_value: 1
  allowed_values: [1, 2, 3, 4, 5]
```
**Use Case**: Columns computed from source data
**Checks**: Calculated values, derived categories, business logic

---

## Configuration Options Reference

| Parameter | Type | Required | Purpose | Example |
|-----------|------|----------|---------|---------|
| `name` | string | ✅ Yes | Column name | `"customer_segment"` |
| `expected_type` | string | ❌ No | Expected data type | `"string"`, `"integer"`, `"double"` |
| `nullable` | boolean | ❌ No | Allow nulls | `true` / `false` |
| `max_null_percent` | float | ❌ No | Max % of nulls | `5.0` (means 5%) |
| `default_value` | any | ❌ No | Expected default | `"PENDING"`, `0`, `false` |
| `min_default_percent` | float | ❌ No | Min % with default | `80.0` (means 80%) |
| `min_value` | number | ❌ No | Minimum value | `0.0` |
| `max_value` | number | ❌ No | Maximum value | `100.0` |
| `allowed_values` | list | ❌ No | Value whitelist | `["A", "B", "C"]` |
| `pattern` | string | ❌ No | Regex pattern | `"^[A-Z]{2}$"` |
| `min_pattern_match_percent` | float | ❌ No | Min % matching | `95.0` (means 95%) |
| `description` | string | ❌ No | Documentation | `"Customer tier level"` |

## Usage Examples

### Quick Start

```bash
# Generate configuration template
python scripts/run_validation.py generate-config config/new_col.yaml --type simple

# Edit config to add new_column validation
# Then run
python scripts/run_validation.py run config/new_col.yaml
```

### Command Line

```bash
# Run new column validation
python scripts/run_validation.py run config/examples/new_column_validation.yaml

# With debug logging
python scripts/run_validation.py run config/examples/new_column_validation.yaml --log-level DEBUG

# List available validators (will show new_column)
python scripts/run_validation.py list-validators
```

### Python API

```python
from src.validation_framework.validators.new_column_validator import NewColumnValidator

config = {
    'name': 'My New Column Validation',
    'type': 'new_column',
    'source': 'source_conn',
    'target': 'target_conn',
    'source_table': 'customers',
    'target_table': 'customers_enriched',
    'metadata': {
        'new_columns': [
            {
                'name': 'customer_segment',
                'expected_type': 'string',
                'nullable': False,
                'allowed_values': ['Premium', 'Standard', 'Basic']
            }
        ]
    }
}

validator = NewColumnValidator(config)
result = validator.validate(source_connector, target_connector)

print(f"Status: {result.status}")
print(f"Passed: {result.metadata['passed_count']}/{result.metadata['new_columns_count']}")
```

## Output Examples

### Console Output
```
✓ Validation: Validate New Customer Enrichment Columns - PASSED

Column: customer_segment
  ✓ Column 'customer_segment' correctly absent from source
  ✓ Column 'customer_segment' exists in target
  ✓ Data type matches: string
  ✓ Nullability check passed: 0 nulls (0.0%)
  ✓ All values are from allowed set (3 unique values)
  Total: 5/5 checks passed

Column: lifetime_value
  ✓ Column 'lifetime_value' correctly absent from source
  ✓ Column 'lifetime_value' exists in target
  ✓ Data type matches: decimal
  ✓ Nullability check passed: 156 nulls (15.6%)
  ✓ Min value 0.00 >= 0.0
  Total: 5/5 checks passed

Summary: All 5 new columns validated successfully
```

### JSON Output
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
          "All values are from allowed set (3 unique values)"
        ],
        "checks_failed": [],
        "total_checks": 4
      }
    ]
  }
}
```

## Real-World Use Cases

### 1. Post-Migration Validation
After migrating data and adding enrichment columns:
```yaml
- Validate all new calculated fields exist
- Check data types match specifications
- Verify default values are applied
- Ensure no unexpected nulls
```

### 2. Feature Engineering Pipeline
After ML feature generation:
```yaml
- Validate feature columns added
- Check value ranges for scaled features
- Verify categorical encodings
- Ensure feature completeness
```

### 3. Compliance Column Addition
After adding GDPR/regulatory columns:
```yaml
- Validate consent flags exist
- Check timestamp columns present
- Verify retention period values
- Ensure proper defaults applied
```

### 4. Data Enrichment Validation
After enriching with external data:
```yaml
- Validate enrichment columns added
- Check data quality of enriched fields
- Verify format compliance
- Ensure enrichment coverage
```

### 5. Schema Version Upgrade
After schema version change:
```yaml
- Validate all v2 columns present
- Check backward compatibility
- Verify migration completeness
- Ensure data consistency
```

## Benefits

✅ **Comprehensive Validation**
- 8 different check types
- Flexible configuration
- Detailed reporting

✅ **Production Ready**
- Full error handling
- Comprehensive logging
- CI/CD integration

✅ **Easy to Use**
- YAML configuration
- Clear documentation
- Example scenarios

✅ **Extensible**
- Custom validators possible
- Plugin architecture
- Additional checks easy to add

✅ **Integration**
- Works with all existing connectors
- Compatible with all reporters
- Part of validation engine

## Files Modified/Created

### New Files
1. ✅ `src/validation_framework/validators/new_column_validator.py` (320 lines)
2. ✅ `config/examples/new_column_validation.yaml` (200 lines)
3. ✅ `docs/NEW_COLUMN_VALIDATION.md` (400+ lines)
4. ✅ `NEW_COLUMN_VALIDATION_SUMMARY.md` (this file)

### Modified Files
1. ✅ `src/validation_framework/models/enums.py` - Added `NEW_COLUMN` type
2. ✅ `src/validation_framework/validators/validator_factory.py` - Registered new validator
3. ✅ `src/validation_framework/models/validation_config.py` - Added to valid types

## Testing

### Unit Test Example
```python
def test_new_column_validation():
    config = {
        'name': 'Test',
        'type': 'new_column',
        'source_table': 'source',
        'target_table': 'target',
        'metadata': {
            'new_columns': [{
                'name': 'new_col',
                'expected_type': 'string',
                'nullable': False
            }]
        }
    }

    validator = NewColumnValidator(config)
    result = validator.validate(source_conn, target_conn)

    assert result.status == ValidationStatus.PASSED
    assert result.metadata['passed_count'] == 1
```

### Integration Test
```bash
# Run full validation with new column check
python scripts/run_validation.py run config/examples/new_column_validation.yaml

# Check exit code
echo $?  # Should be 0 for success
```

## Next Steps for Users

1. **Review Documentation**
   - Read `docs/NEW_COLUMN_VALIDATION.md`
   - Check examples in `config/examples/new_column_validation.yaml`

2. **Create Configuration**
   - Identify newly added columns
   - Define validation rules
   - Set appropriate thresholds

3. **Run Validation**
   ```bash
   python scripts/run_validation.py run your_config.yaml
   ```

4. **Review Results**
   - Check console output
   - Review JSON/HTML reports
   - Address any failures

5. **Integrate with CI/CD**
   - Add to deployment pipeline
   - Set appropriate exit code handling
   - Monitor validation trends

## Summary

The New Column Validator provides comprehensive validation for newly added columns with:
- ✅ **8 validation checks** covering all scenarios
- ✅ **Flexible configuration** with 12+ options per column
- ✅ **8 common scenarios** with examples
- ✅ **Complete documentation** with best practices
- ✅ **Production-ready** with full error handling
- ✅ **Fully integrated** with existing framework

Perfect for validating schema evolution, ETL enhancements, data enrichment, and any scenario where new columns are added to data pipelines!
