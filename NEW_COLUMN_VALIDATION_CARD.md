# New Column Validation - Quick Card

## 🎯 What It Does
Validates newly added columns with 8 comprehensive checks:
1. Column existence, 2. Data type, 3. Nullability, 4. Defaults, 5. Ranges, 6. Allowed values, 7. Patterns, 8. Metadata

## ⚡ Quick Usage
```yaml
validations:
  - name: "New Column Check"
    type: new_column
    source: source_conn
    target: target_conn
    source_table: "customers"
    target_table: "customers_v2"
    metadata:
      new_columns:
        - name: "segment"
          expected_type: "string"
          nullable: false
          allowed_values: ["A", "B", "C"]
```

## 📁 Key Files
- **Validator**: `src/validation_framework/validators/new_column_validator.py`
- **Config Example**: `config/examples/new_column_validation.yaml`
- **Quick Guide**: `docs/NEW_COLUMN_QUICK_REFERENCE.md`
- **Full Docs**: `docs/NEW_COLUMN_VALIDATION.md`

## 🔧 Configuration Options
| Option | Example | Use For |
|--------|---------|---------|
| `expected_type` | `"string"` | Type validation |
| `nullable` | `false` | NOT NULL check |
| `max_null_percent` | `5.0` | Null threshold |
| `default_value` | `"PENDING"` | Default check |
| `min_value` / `max_value` | `0.0`, `100.0` | Range validation |
| `allowed_values` | `[1,2,3,4,5]` | Categorical |
| `pattern` | `"^[A-Z]{2}$"` | Format validation |

## 💡 Common Patterns
```yaml
# Boolean flag
- name: "is_active"
  expected_type: "boolean"
  nullable: false
  default_value: true

# Score with range
- name: "score"
  expected_type: "double"
  min_value: 0.0
  max_value: 100.0

# Country code
- name: "country"
  expected_type: "string"
  pattern: "^[A-Z]{2}$"
  min_pattern_match_percent: 98.0

# Category
- name: "tier"
  expected_type: "integer"
  allowed_values: [1, 2, 3, 4, 5]
```

## 🎨 Use Cases
✓ Schema evolution  
✓ ETL enhancements  
✓ Data enrichment  
✓ GDPR compliance  
✓ Feature engineering  
✓ Audit columns  

## 🚀 Run It
```bash
python scripts/run_validation.py run config/examples/new_column_validation.yaml
```

## 📚 More Info
- Quick Reference: `docs/NEW_COLUMN_QUICK_REFERENCE.md`
- Full Guide: `docs/NEW_COLUMN_VALIDATION.md`
- All Types: `VALIDATION_TYPES_COMPLETE.md`
