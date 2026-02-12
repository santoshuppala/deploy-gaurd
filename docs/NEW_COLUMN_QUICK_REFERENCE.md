# New Column Validation - Quick Reference

## Quick Start (30 seconds)

```yaml
validations:
  - name: "Validate New Columns"
    type: new_column
    enabled: true
    source: source_connection
    target: target_connection
    source_table: "original_table"
    target_table: "enhanced_table"
    metadata:
      new_columns:
        - name: "new_column_name"
          expected_type: "string"
          nullable: false
```

## Common Patterns

### 1. String Column (with allowed values)
```yaml
- name: "status"
  expected_type: "string"
  nullable: false
  default_value: "PENDING"
  allowed_values: ["PENDING", "ACTIVE", "COMPLETED"]
```

### 2. Numeric Column (with range)
```yaml
- name: "score"
  expected_type: "double"
  nullable: true
  min_value: 0.0
  max_value: 100.0
  max_null_percent: 5.0
```

### 3. Boolean Flag
```yaml
- name: "is_active"
  expected_type: "boolean"
  nullable: false
  default_value: true
```

### 4. Timestamp Column
```yaml
- name: "created_at"
  expected_type: "timestamp"
  nullable: false
```

### 5. String with Pattern (UUID)
```yaml
- name: "transaction_id"
  expected_type: "string"
  nullable: false
  pattern: "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
  min_pattern_match_percent: 100.0
```

### 6. String with Pattern (Country Code)
```yaml
- name: "country_code"
  expected_type: "string"
  nullable: true
  pattern: "^[A-Z]{2}$"
  min_pattern_match_percent: 98.0
```

### 7. Integer with Allowed Values
```yaml
- name: "priority"
  expected_type: "integer"
  nullable: false
  default_value: 3
  allowed_values: [1, 2, 3, 4, 5]
  min_value: 1
  max_value: 5
```

### 8. Decimal with Range
```yaml
- name: "amount"
  expected_type: "decimal"
  nullable: false
  min_value: 0.0
```

## Configuration Options Cheat Sheet

| Option | Type | Example | Use When |
|--------|------|---------|----------|
| `expected_type` | string | `"string"`, `"integer"`, `"double"` | Always recommended |
| `nullable` | bool | `false` | Column should never be null |
| `max_null_percent` | float | `5.0` | Some nulls OK but limited |
| `default_value` | any | `"PENDING"`, `0`, `false` | Has default value |
| `min_default_percent` | float | `80.0` | Check default frequency |
| `min_value` | number | `0.0` | Has minimum boundary |
| `max_value` | number | `100.0` | Has maximum boundary |
| `allowed_values` | list | `["A", "B", "C"]` | Categorical/enum column |
| `pattern` | regex | `"^[A-Z]{2}$"` | Formatted strings |
| `min_pattern_match_percent` | float | `95.0` | Pattern compliance check |

## Data Types

| Type | Aliases | Example Values |
|------|---------|----------------|
| `string` | varchar, char, text | `"hello"` |
| `integer` | int, long, bigint | `42` |
| `double` | float, decimal | `3.14` |
| `boolean` | bool | `true`, `false` |
| `date` | - | `2024-01-01` |
| `timestamp` | datetime | `2024-01-01 12:00:00` |

## Common Patterns (Regex)

```yaml
# UUID
pattern: "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"

# ISO Country Code (2-letter)
pattern: "^[A-Z]{2}$"

# ISO Country Code (3-letter)
pattern: "^[A-Z]{3}$"

# Email
pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"

# Phone (US)
pattern: "^\\+?1?[0-9]{10}$"

# Zip Code (US)
pattern: "^[0-9]{5}(-[0-9]{4})?$"

# Product Code (e.g., CAT-1234)
pattern: "^[A-Z]{3}-[0-9]{4}$"

# Hex Color
pattern: "^#[0-9A-Fa-f]{6}$"

# ISO Date
pattern: "^[0-9]{4}-[0-9]{2}-[0-9]{2}$"

# SHA256 Hash
pattern: "^[a-f0-9]{64}$"

# Alphanumeric only
pattern: "^[a-zA-Z0-9]+$"
```

## Real Examples

### GDPR Compliance Columns
```yaml
new_columns:
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
```

### ETL Audit Columns
```yaml
new_columns:
  - name: "etl_batch_id"
    expected_type: "string"
    nullable: false
    pattern: "^batch_[0-9]{8}_[0-9]{6}$"

  - name: "etl_timestamp"
    expected_type: "timestamp"
    nullable: false

  - name: "data_source"
    expected_type: "string"
    nullable: false
    allowed_values: ["API", "File", "Stream"]
```

### Customer Segmentation
```yaml
new_columns:
  - name: "customer_segment"
    expected_type: "string"
    nullable: false
    allowed_values: ["Premium", "Standard", "Basic"]

  - name: "segment_score"
    expected_type: "double"
    nullable: false
    min_value: 0.0
    max_value: 100.0

  - name: "segment_date"
    expected_type: "date"
    nullable: false
```

### Geolocation Enrichment
```yaml
new_columns:
  - name: "country_code"
    expected_type: "string"
    nullable: true
    pattern: "^[A-Z]{2}$"
    max_null_percent: 5.0

  - name: "timezone"
    expected_type: "string"
    nullable: true
    allowed_values: ["UTC", "America/New_York", "Europe/London", "Asia/Tokyo"]

  - name: "latitude"
    expected_type: "double"
    nullable: true
    min_value: -90.0
    max_value: 90.0

  - name: "longitude"
    expected_type: "double"
    nullable: true
    min_value: -180.0
    max_value: 180.0
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| "Column exists in source" | Column not truly new, remove from new_columns list |
| "Type mismatch" | Check actual type in target, update expected_type |
| "Too many nulls" | Adjust max_null_percent or fix ETL |
| "Pattern not matching" | Verify regex, check data format, lower min_pattern_match_percent |
| "Default not found" | Check ETL default logic, adjust min_default_percent |
| "Values out of range" | Adjust min/max values or fix data |

## CLI Commands

```bash
# Run validation
python scripts/run_validation.py run config/new_column_config.yaml

# List validators (includes new_column)
python scripts/run_validation.py list-validators

# Generate sample config
python scripts/run_validation.py generate-config config/my_config.yaml
```

## Complete Minimal Example

```yaml
connections:
  - name: source
    type: spark
    enabled: true
    config:
      master: "local[*]"

  - name: target
    type: hive
    enabled: true
    config:
      host: "localhost"
      port: 10000

validations:
  - name: "New Column Check"
    type: new_column
    enabled: true
    source: source
    target: target
    source_table: "customers"
    target_table: "customers_v2"
    metadata:
      new_columns:
        - name: "customer_tier"
          expected_type: "integer"
          nullable: false
          default_value: 1
          allowed_values: [1, 2, 3, 4, 5]

reporters:
  - type: console
    enabled: true
```

## Status Indicators

- ✅ **PASSED**: All checks passed
- ⚠️ **WARNING**: Issues found but within thresholds
- ❌ **FAILED**: Validation checks failed
- ⚕️ **ERROR**: Validation couldn't execute

## Best Practices

1. ✅ Always set `expected_type`
2. ✅ Be explicit with `nullable`
3. ✅ Use realistic thresholds
4. ✅ Add descriptions for documentation
5. ✅ Test with sample data first
6. ✅ Start with relaxed thresholds, tighten later
7. ✅ Combine with other validation types
8. ✅ Monitor results over time

## More Information

- **Full Documentation**: `docs/NEW_COLUMN_VALIDATION.md`
- **Examples**: `config/examples/new_column_validation.yaml`
- **Summary**: `NEW_COLUMN_VALIDATION_SUMMARY.md`
