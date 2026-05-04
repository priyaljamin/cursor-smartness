# AGENTS.md

## Cursor Cloud specific instructions

### Overview

**MVW Catalog Addition Pipeline (v3)** — a 4-stage Python CLI tool for Master Value Wholesale that automates adding new products to a Shopify store via Matrixify import. See `README.md` for full stage descriptions and CLI usage.

### Dependencies

Dependencies are listed in `requirements.txt`. Install into a virtualenv:

```bash
pip install virtualenv  # system-level, needed if python3.12-venv is unavailable
virtualenv .venv && source .venv/bin/activate && pip install -r requirements.txt
```

### Running the pipeline

```bash
source .venv/bin/activate
python pipeline.py --stage all --pilot 10
```

Stages can run individually: `--stage 1`, `--stage 2`, `--stage 3`, `--stage 4`, or `--stage all`.
When `--stage all`, execution order is 1 → 3 → 2 → 4 (Claude before images).

### Required credentials

- `ANTHROPIC_API_KEY` — required for Stage 3 (Claude enrichment). Without it the pipeline exits with an error.
- `UPCITEMDB_KEY` — hardcoded default in `pipeline.py` but can be overridden via env var.

### Input file

The pipeline expects an xlsx at `input/MVW_QK_CATALOG_ADDITION_05_01_2026.xlsx`. Required columns: `TITLE`, `Variant Barcode`, `Variant SKU`, `Metafield: custom.case_size [number_integer]`, `Metafield: custom.master_sku [single_line_text_field]`, `Variant Cost`, `Variant Price`.

### Gotchas

- There are **no automated tests** and **no linter** configured. Validation is manual.
- Stages 1 and 2 work without any API key (UPCitemdb key is hardcoded). Stage 3 requires `ANTHROPIC_API_KEY`.
- Caches persist in `cache/` dir. Delete `cache/*.json` to force a full re-run.
- `--resume` flag skips items already cached/downloaded.
- If `python3 -m venv` fails, use `pip install virtualenv && virtualenv .venv` as an alternative.
- The `input/` directory and xlsx file are not in the repo — they must be provided separately.
