# Complete Validation Types Reference

## All 5 Validation Types

The framework now includes **5 comprehensive validation types**:

| # | Type | Purpose | Key Features |
|---|------|---------|--------------|
| 1 | **Row Count** | Compare record counts | Percentage/absolute thresholds, zero detection |
| 2 | **Data Quality** | Check data cleanliness | Nulls, duplicates, invalid records |
| 3 | **Schema** | Verify structure | Column presence, type matching |
| 4 | **Business Rule** | Custom logic | Aggregations, row comparison |
| 5 | **New Column** ✨ | Validate new columns | 8 comprehensive checks |

---

## 1. Row Count Validation

**File**: `row_count_validator.py`

Compare row counts between source and target.

### Configuration
```yaml
- name: "Row Count Check"
  type: row_count
  source_query: "SELECT COUNT(*) FROM customers"
  target_query: "SELECT COUNT(*) FROM customers_processed"
  thresholds:
    max_difference_percent: 0.1
    max_difference_absolute: 100
    fail_on_zero_source: true
    fail_on_zero_target: true
```

### Use Cases
- Post-ETL completeness check
- Data migration verification
- Pipeline output validation

---

## 2. Data Quality Validation

**File**: `data_quality_validator.py`

Check data quality metrics.

### Configuration
```yaml
- name: "Data Quality Check"
  type: data_quality
  source_query: "SELECT * FROM customers"
  target_query: "SELECT * FROM customers_processed"
  thresholds:
    max_null_percent: 5.0
    max_duplicate_percent: 1.0
    max_invalid_percent: 2.0
  metadata:
    check_columns: ["customer_id", "email"]
    primary_key: "customer_id"
```

### Use Cases
- Data quality monitoring
- Cleanliness validation
- Null/duplicate detection

---

## 3. Schema Validation

**File**: `schema_validator.py`

Verify schema compatibility.

### Configuration
```yaml
- name: "Schema Check"
  type: schema
  source_table: "orders"
  target_table: "orders_processed"
  metadata:
    critical_columns: ["order_id", "customer_id", "amount"]
```

### Use Cases
- Schema migration validation
- Structure compatibility check
- Type verification

---

## 4. Business Rule Validation

**File**: `business_rule_validator.py`

Validate custom business logic.

### Configuration
```yaml
- name: "Revenue Check"
  type: business_rule
  source_query: "SELECT SUM(amount) FROM orders"
  target_query: "SELECT SUM(revenue) FROM daily_summary"
  thresholds:
    max_difference_percent: 0.01
  metadata:
    rule_type: "aggregation"
```

### Use Cases
- Aggregation validation
- Business rule compliance
- Financial calculations

---

## 5. New Column Validation ✨

**File**: `new_column_validator.py`

Comprehensive validation for newly added columns.

### Configuration
```yaml
- name: "New Column Check"
  type: new_column
  source_table: "customers"
  target_table: "customers_enhanced"
  metadata:
    new_columns:
      - name: "customer_segment"
        expected_type: "string"
        nullable: false
        allowed_values: ["Premium", "Standard", "Basic"]

      - name: "loyalty_score"
        expected_type: "double"
        nullable: true
        min_value: 0.0
        max_value: 100.0
        max_null_percent: 10.0
```

### 8 Validation Checks
1. ✅ Column existence (absent in source, present in target)
2. ✅ Data type validation
3. ✅ Nullability constraints
4. ✅ Default value verification
5. ✅ Value range checking (numeric)
6. ✅ Allowed values (categorical)
7. ✅ Pattern matching (regex)
8. ✅ Custom metadata checks

### Use Cases
- Schema evolution validation
- ETL enhancement verification
- Data enrichment checks
- Compliance column validation (GDPR, etc.)
- Feature engineering validation
- Audit trail column verification

### Quick Examples

**Simple Column:**
```yaml
- name: "status"
  expected_type: "string"
  nullable: false
```

**With Default:**
```yaml
- name: "is_active"
  expected_type: "boolean"
  nullable: false
  default_value: true
  min_default_percent: 90.0
```

**With Range:**
```yaml
- name: "score"
  expected_type: "double"
  min_value: 0.0
  max_value: 100.0
```

**With Pattern:**
```yaml
- name: "country_code"
  expected_type: "string"
  pattern: "^[A-Z]{2}$"
  min_pattern_match_percent: 98.0
```

**With Allowed Values:**
```yaml
- name: "priority"
  expected_type: "integer"
  allowed_values: [1, 2, 3, 4, 5]
```

### Documentation
- **Quick Reference**: `docs/NEW_COLUMN_QUICK_REFERENCE.md`
- **Full Guide**: `docs/NEW_COLUMN_VALIDATION.md`
- **Examples**: `config/examples/new_column_validation.yaml`

---

## Comparison Matrix

| Feature | Row Count | Data Quality | Schema | Business Rule | New Column |
|---------|-----------|--------------|--------|---------------|------------|
| **Checks row counts** | ✅ | ❌ | ❌ | ❌ | ❌ |
| **Checks nulls** | ❌ | ✅ | ❌ | ❌ | ✅ |
| **Checks duplicates** | ❌ | ✅ | ❌ | ❌ | ❌ |
| **Checks schema** | ❌ | ❌ | ✅ | ❌ | ✅ |
| **Checks types** | ❌ | ❌ | ✅ | ❌ | ✅ |
| **Checks aggregations** | ❌ | ❌ | ❌ | ✅ | ❌ |
| **Checks new columns** | ❌ | ❌ | ❌ | ❌ | ✅ |
| **Checks ranges** | ❌ | ❌ | ❌ | ❌ | ✅ |
| **Checks patterns** | ❌ | ❌ | ❌ | ❌ | ✅ |
| **Checks defaults** | ❌ | ❌ | ❌ | ❌ | ✅ |

## Combined Validation Example

Use multiple validation types together:

```yaml
validations:
  # 1. Check row counts match
  - name: "Row Count"
    type: row_count
    source_table: "customers"
    target_table: "customers_enhanced"
    thresholds:
      max_difference_percent: 0.1

  # 2. Check data quality
  - name: "Data Quality"
    type: data_quality
    source_query: "SELECT * FROM customers"
    target_query: "SELECT * FROM customers_enhanced"
    thresholds:
      max_null_percent: 5.0

  # 3. Check schema compatibility
  - name: "Schema"
    type: schema
    source_table: "customers"
    target_table: "customers_enhanced"
    metadata:
      critical_columns: ["customer_id", "email"]

  # 4. Validate new columns
  - name: "New Columns"
    type: new_column
    source_table: "customers"
    target_table: "customers_enhanced"
    metadata:
      new_columns:
        - name: "customer_segment"
          expected_type: "string"
          nullable: false
          allowed_values: ["Premium", "Standard", "Basic"]

        - name: "lifetime_value"
          expected_type: "decimal"
          nullable: true
          min_value: 0.0

  # 5. Check business rules
  - name: "Total Value"
    type: business_rule
    source_query: "SELECT SUM(order_total) FROM orders"
    target_query: "SELECT SUM(lifetime_value) FROM customers_enhanced"
    thresholds:
      max_difference_percent: 1.0
```

## When to Use Each Type

### Use Row Count When:
- ✅ Verifying ETL completeness
- ✅ Checking data migration
- ✅ Ensuring no data loss

### Use Data Quality When:
- ✅ Monitoring data cleanliness
- ✅ Detecting null/duplicate issues
- ✅ Validating data standards

### Use Schema When:
- ✅ Validating migrations
- ✅ Checking structure compatibility
- ✅ Verifying column types

### Use Business Rule When:
- ✅ Validating aggregations
- ✅ Checking calculations
- ✅ Enforcing business logic

### Use New Column When:
- ✅ Adding columns during ETL
- ✅ Schema evolution
- ✅ Data enrichment
- ✅ Feature engineering
- ✅ Compliance columns
- ✅ Audit fields

## CLI Usage

```bash
# List all available validators
python scripts/run_validation.py list-validators

# Output:
# Supported Validation Types:
#   • row_count
#   • data_quality
#   • schema
#   • business_rule
#   • new_column

# Run validation with specific type
python scripts/run_validation.py run config/my_validation.yaml
```

## All Documentation

1. **Main README**: `README.md`
2. **Quick Start**: `docs/QUICKSTART.md`
3. **New Column Guide**: `docs/NEW_COLUMN_VALIDATION.md`
4. **New Column Reference**: `docs/NEW_COLUMN_QUICK_REFERENCE.md`
5. **This Document**: `VALIDATION_TYPES_COMPLETE.md`
6. **Examples**: `config/examples/`

## Summary

✅ **5 Validation Types** fully implemented
✅ **All scenarios covered** from simple to complex
✅ **Production-ready** with comprehensive error handling
✅ **Well-documented** with examples and guides
✅ **Extensible** architecture for custom validators

The framework now provides complete coverage for all data validation needs in big data pipelines!
