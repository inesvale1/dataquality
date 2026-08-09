# dq_package

This folder is a refactoring of the original `DataModelQuality.ipynb` into a package-oriented structure.

## Structure (current phase)
- `domain`: business rules (validators + config)
- `infrastructure`: CSV loader (to be replaced by Oracle metadata loader later)
- `adapters`: exporters (Excel)
- `app`: use cases / orchestration

## Run
```bash
python run_quality.py --config-json config/run_quality.example.json
```

The base folder must contain subfolders with files like `metadata_<schema>.csv`.
The files generated with the data model issues are located in the same folder and have the prefix `issues_metadata_<schema>.csv`.

## Output (Excel sheets)
- `SCHEMA_METADATA`: raw input metadata (full CSV content)
- `METADATA_MEASURE`: totals used as denominators (includes schema totals like tables, columns, and key counts)
- `METADATA_ISSUE`: consolidated list of rule violations
- `METADATA_METRIC`: quality indicators calculated from measures

## Technical catalog context (`metadata_context_<schema>.json`)

The extraction of physical metadata (Oracle/CSV/S3/Athena) and the construction of
`metadata_context_<schema>.json` now have their source of truth in the sibling
project `../technicalcatalogpipeline` (Fase 1 — Pipeline de Catálogo Técnico).
`config/run_quality.config.json` defaults to `"regenerate_context": false`, so
`run_quality.py` reuses the `metadata_context_<schema>.json` already produced by
`technicalcatalogpipeline` under `schema/<schema>/inputs/` instead of rebuilding it.

The local copy of that logic (`app/orchestration/metadata_context_builder.py`,
`infrastructure/io/metadata_sources.py`) is kept as-is for backward compatibility
(e.g. running `dataquality` standalone, before `technicalcatalogpipeline` has run
for a schema) — it is a fallback, not the primary path going forward.
