# dq_package

This folder is a refactoring of the original `DataModelQuality.ipynb` into a package-oriented structure.

## Structure (current phase)
- `dq/domain`: business rules (validators + config)
- `dq/infrastructure`: CSV loader (to be replaced by Oracle metadata loader later)
- `dq/adapters`: exporters (Excel)
- `dq/app`: use cases / orchestration

## Run
```bash
python run_quality.py --config-json config/run_quality.example.json
```

The base folder must contain subfolders with files like `metadata_<schema>.csv`.
