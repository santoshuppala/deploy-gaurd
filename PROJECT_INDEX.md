# Project File Index

Complete index of all files in the Pre/Post Deployment Validation Framework.

## 📁 Project Structure

```
PrePost_Validation_generic/
├── 📂 src/validation_framework/        # Source code
├── 📂 config/                          # Configuration files
├── 📂 scripts/                         # CLI scripts
├── 📂 tests/                           # Test suite
├── 📂 output/                          # Generated outputs
├── 📂 docs/                            # Documentation
└── 📄 Root files                       # README, setup, etc.
```

---

## 🔧 Source Code (`src/validation_framework/`)

### Core (`core/`)
| File | Lines | Purpose |
|------|-------|---------|
| `base_connector.py` | 170 | Abstract connector interface |
| `base_validator.py` | 160 | Abstract validator interface |
| `config_loader.py` | 180 | YAML configuration parser |
| `exceptions.py` | 100 | Custom exception hierarchy |
| `validation_engine.py` | 300 | Main orchestration engine |

### Connectors (`connectors/`)
| File | Lines | Purpose |
|------|-------|---------|
| `spark_connector.py` | 200 | Apache Spark connector |
| `hive_connector.py` | 180 | Apache Hive connector |
| `s3_connector.py` | 220 | AWS S3 connector |
| `adls_connector.py` | 230 | Azure ADLS connector |
| `gcs_connector.py` | 220 | Google Cloud Storage connector |
| `connector_factory.py` | 130 | Connector factory pattern |

### Validators (`validators/`)
| File | Lines | Purpose |
|------|-------|---------|
| `row_count_validator.py` | 140 | Row count validation |
| `data_quality_validator.py` | 180 | Data quality checks |
| `schema_validator.py` | 170 | Schema validation |
| `business_rule_validator.py` | 220 | Business rule validation |
| `new_column_validator.py` ⭐ | 320 | **New column validation** |
| `validator_factory.py` | 140 | Validator factory pattern |

### Models (`models/`)
| File | Lines | Purpose |
|------|-------|---------|
| `enums.py` | 65 | Type-safe enumerations |
| `validation_result.py` | 150 | Result data models |
| `validation_config.py` | 130 | Pydantic config models |

### Reporters (`reporters/`)
| File | Lines | Purpose |
|------|-------|---------|
| `base_reporter.py` | 110 | Abstract reporter interface |
| `json_reporter.py` | 70 | JSON report generator |
| `console_reporter.py` | 200 | Console output with colors |
| `html_reporter.py` | 350 | HTML report with charts |
| `email_reporter.py` | 250 | Email notification sender |
| `database_reporter.py` | 280 | Database result storage |
| `reporter_factory.py` | 120 | Reporter factory pattern |

### Utilities (`utils/`)
| File | Lines | Purpose |
|------|-------|---------|
| `logger.py` | 100 | Centralized logging |
| `metrics.py` | 90 | Metrics collection |
| `helpers.py` | 130 | Utility functions |

---

## ⚙️ Configuration (`config/`)

### Main Configuration
| File | Lines | Purpose |
|------|-------|---------|
| `validation_config.yaml` | 180 | Main configuration template |

### Examples (`config/examples/`)
| File | Lines | Purpose |
|------|-------|---------|
| `simple_spark_validation.yaml` | 40 | Simple Spark example |
| `s3_to_hive_validation.yaml` | 80 | S3 to Hive migration |
| `new_column_validation.yaml` ⭐ | 200 | **New column examples** |

---

## 📜 Scripts (`scripts/`)

| File | Lines | Purpose |
|------|-------|---------|
| `run_validation.py` | 250 | CLI entry point with Click |

---

## 🧪 Tests (`tests/`)

### Unit Tests (`tests/unit/`)
| File | Lines | Purpose |
|------|-------|---------|
| `test_validators.py` | 150 | Validator unit tests |
| `test_config_loader.py` | 120 | Config loader tests |

---

## 📚 Documentation (`docs/`)

| File | Lines | Purpose |
|------|-------|---------|
| `QUICKSTART.md` | 250 | Quick start guide |
| `NEW_COLUMN_VALIDATION.md` ⭐ | 400+ | **New column complete guide** |
| `NEW_COLUMN_QUICK_REFERENCE.md` ⭐ | 150+ | **New column quick reference** |

---

## 📄 Root Files

### Documentation
| File | Lines | Purpose |
|------|-------|---------|
| `README.md` | 450 | Main documentation |
| `IMPLEMENTATION_SUMMARY.md` | 300 | Implementation summary |
| `NEW_COLUMN_VALIDATION_SUMMARY.md` ⭐ | 350 | **New column summary** |
| `VALIDATION_TYPES_COMPLETE.md` ⭐ | 400 | **All validation types** |
| `PROJECT_INDEX.md` | - | This file |

### Configuration
| File | Purpose |
|------|---------|
| `requirements.txt` | Python dependencies |
| `setup.py` | Package installation |
| `pytest.ini` | Test configuration |
| `.gitignore` | Git ignore rules |

### Docker
| File | Purpose |
|------|---------|
| `Dockerfile` | Container definition |

### License
| File | Purpose |
|------|---------|
| `LICENSE` | MIT License |

---

## 📊 Statistics by Category

### Source Code
- **Core**: 5 files, ~910 lines
- **Connectors**: 6 files, ~1,180 lines
- **Validators**: 6 files, ~1,170 lines (including new_column_validator ⭐)
- **Models**: 3 files, ~345 lines
- **Reporters**: 7 files, ~1,380 lines
- **Utils**: 3 files, ~320 lines
- **Total**: 30 files, ~5,305 lines

### Configuration
- **Main**: 1 file, 180 lines
- **Examples**: 3 files, ~320 lines
- **Total**: 4 files, ~500 lines

### Documentation
- **Main docs**: 4 files, ~1,000 lines
- **New column docs**: 4 files, ~1,300 lines ⭐
- **Total**: 8 files, ~2,300 lines

### Tests
- **Unit tests**: 2 files, ~270 lines
- **Integration**: (fixtures available)

### Overall Project
- **Total files**: 45+
- **Total lines**: 8,000+
- **Documentation pages**: 8
- **Configuration examples**: 4

---

## 🆕 New Files Added (New Column Validation)

### Implementation
1. ✅ `src/validation_framework/validators/new_column_validator.py` (320 lines)

### Configuration
2. ✅ `config/examples/new_column_validation.yaml` (200 lines)

### Documentation
3. ✅ `docs/NEW_COLUMN_VALIDATION.md` (400+ lines)
4. ✅ `docs/NEW_COLUMN_QUICK_REFERENCE.md` (150+ lines)
5. ✅ `NEW_COLUMN_VALIDATION_SUMMARY.md` (350 lines)
6. ✅ `VALIDATION_TYPES_COMPLETE.md` (400 lines)
7. ✅ `PROJECT_INDEX.md` (this file)

### Modified Files
8. ✅ `src/validation_framework/models/enums.py` (added NEW_COLUMN type)
9. ✅ `src/validation_framework/validators/validator_factory.py` (registered validator)
10. ✅ `src/validation_framework/models/validation_config.py` (added to valid types)

**Total additions**: 7 new files, 3 modified files, ~1,820 new lines of code/documentation

---

## 📖 Quick Access Guide

### For Users
**Start here**: `README.md`
**Quick setup**: `docs/QUICKSTART.md`
**New columns**: `docs/NEW_COLUMN_QUICK_REFERENCE.md`

### For Developers
**Core abstractions**: `src/validation_framework/core/`
**Adding connectors**: `src/validation_framework/connectors/connector_factory.py`
**Adding validators**: `src/validation_framework/validators/validator_factory.py`

### For Configuration
**Main template**: `config/validation_config.yaml`
**Simple example**: `config/examples/simple_spark_validation.yaml`
**New columns**: `config/examples/new_column_validation.yaml` ⭐

### For Testing
**Run tests**: `pytest tests/`
**Unit tests**: `tests/unit/`
**Fixtures**: `tests/fixtures/`

---

## 🎯 Key Files by Use Case

### Schema Evolution / New Columns
1. `src/validation_framework/validators/new_column_validator.py`
2. `config/examples/new_column_validation.yaml`
3. `docs/NEW_COLUMN_VALIDATION.md`
4. `docs/NEW_COLUMN_QUICK_REFERENCE.md`

### Row Count Validation
1. `src/validation_framework/validators/row_count_validator.py`
2. `config/validation_config.yaml` (Row Count section)

### Data Quality
1. `src/validation_framework/validators/data_quality_validator.py`
2. `config/validation_config.yaml` (Data Quality section)

### Schema Validation
1. `src/validation_framework/validators/schema_validator.py`
2. `config/validation_config.yaml` (Schema section)

### Business Rules
1. `src/validation_framework/validators/business_rule_validator.py`
2. `config/validation_config.yaml` (Business Rule section)

### S3 to Hive Migration
1. `config/examples/s3_to_hive_validation.yaml`
2. `src/validation_framework/connectors/s3_connector.py`
3. `src/validation_framework/connectors/hive_connector.py`

---

## 🔍 Finding Files

### By Validation Type
- **Row Count**: `validators/row_count_validator.py`
- **Data Quality**: `validators/data_quality_validator.py`
- **Schema**: `validators/schema_validator.py`
- **Business Rule**: `validators/business_rule_validator.py`
- **New Column**: `validators/new_column_validator.py` ⭐

### By Connector
- **Spark**: `connectors/spark_connector.py`
- **Hive**: `connectors/hive_connector.py`
- **S3**: `connectors/s3_connector.py`
- **ADLS**: `connectors/adls_connector.py`
- **GCS**: `connectors/gcs_connector.py`

### By Reporter
- **JSON**: `reporters/json_reporter.py`
- **HTML**: `reporters/html_reporter.py`
- **Console**: `reporters/console_reporter.py`
- **Email**: `reporters/email_reporter.py`
- **Database**: `reporters/database_reporter.py`

---

## ✨ Navigation Tips

1. **Start with README.md** for overview
2. **Use QUICKSTART.md** for setup
3. **Check VALIDATION_TYPES_COMPLETE.md** for all validation types
4. **Refer to config/examples/** for configuration patterns
5. **Read docs/** for detailed guides
6. **Check src/** for implementation details

---

**Last Updated**: After adding New Column Validation feature
**Framework Version**: 1.0.0 with New Column Validation ⭐
