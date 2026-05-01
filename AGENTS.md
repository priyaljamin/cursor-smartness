# AGENTS.md

## Cursor Cloud specific instructions

### Overview

This is a single-script Python tool (**Shopify Pack Image Pipeline**) for Master Value Wholesale. It generates composite "pack fan" product images from Shopify product photos. The main script is `Curve.py` (the README references it as `test_2products.py`—the file has been renamed).

### Running the application

1. Activate the virtualenv: `source .venv/bin/activate`
2. Run the pipeline: `python Curve.py`
3. The script defaults to `DRY_RUN = True` and `TEST_LIMIT = 10`, so it generates local preview images in `./test_output/` without uploading to Shopify.
4. Running the full pipeline requires `SHOPIFY_TOKEN` and `SHOPIFY_STORE_URL` in a `.env` file (see README).

### Testing without Shopify credentials

The core image generation functions (`generate_composite`, `remove_edge_white_background`, `get_fan_configs`, etc.) can be tested locally by importing from `Curve.py` and passing synthetic `PIL.Image` objects. No API credentials are needed for image generation logic.

### Gotchas

- The script uses `python-dotenv` to load `.env` at import time (`load_dotenv()` runs at module level). If importing functions from `Curve.py` for testing, this is harmless when `.env` is missing—the env vars will just be `None`.
- Font rendering uses DejaVu Sans Bold (`fonts-dejavu-core` package, pre-installed). The script falls back to Pillow's default bitmap font if not found, but badge text will look degraded.
- There are no automated tests or linting configured in this repository. Validation is manual via visual inspection of generated images in `./test_output/`.
- `python3.12-venv` must be installed on the system for creating the virtualenv (`apt-get install python3.12-venv`).
