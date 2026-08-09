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

## dataquality does not generate context anymore

`dataquality` no longer contains any code that builds `metadata_context_<schema>.json`
or `sources_context_<schema>.json` — that responsibility belongs entirely to the
sibling projects `../technicalcatalogpipeline` (Fase 1) and `../businessglossarypipeline`
(Fase 2). `dataquality` only *reads* those files from `schema/<schema>/inputs/`.

When a file is missing, `dataquality/infrastructure/io/pipeline_bridge.py` decides
what happens next, controlled by two flags:

- **`require_metadata_context`** (`config/run_quality.config.json`, default `true`):
  if `metadata_context_<schema>.json` is missing, `run_quality.py` shells out to
  `technicalcatalogpipeline/scripts/build_technical_catalog.py --schema <schema>`
  and re-reads the file it wrote. If that also fails (e.g. `metadata_<schema>.csv`
  itself doesn't exist), the run fails with a clear error. Set to `false` to instead
  proceed with an empty context (degrades MDDQ/LLM-suggestion quality but never blocks).
- **`--require-sources-context`** (only relevant to `scripts/build_denodo_catalog_input.py`,
  default `false`): same idea for `sources_context_<schema>.json`, but shelling out to
  `businessglossarypipeline/scripts/generate_sources_context.py` instead — **this can
  trigger a real LLM API call and cost money/time**, which is why it defaults to off.

Both projects are expected to be siblings of `dataquality/` (same parent folder).
