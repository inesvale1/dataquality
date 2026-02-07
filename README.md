# dq_package

This folder is a refactoring of the original `DataModelQuality.ipynb` into a package-oriented structure.

## Structure (current phase)
- `domain`: business rules (validators + config)
- `infrastructure`: CSV loader (to be replaced by Oracle metadata loader later)
- `adapters`: exporters (Excel)
- `app`: use cases / orchestration

## Run
```bash
python run_model_quality.py --base-folder /path/to/Analise\ Esquemas
```

The base folder must contain subfolders with files like `metadados_<scheme>.csv`.
The files generated with the data model issues are located in the same folder and have the prefix 'issues_metadata_<scheme>.csv`.

## Output (Excel sheets)
- `0_SCHEMA_METADATA`: raw input metadata (full CSV content)
- `SCHEME_MEASURES`: totals for tables, columns, and key counts
- `METADATA_ISSUES`: consolidated list of rule violations
- `METADATA_MEASURES`: metadata-specific totals used as denominators
- `METADATA_METRICS`: quality indicators calculated from measures
