# Shopify Pack Image Pipeline

This repository contains a single Python script, `test_2products.py`, that:

- pulls products from Shopify,
- reads `custom.case_size` metafield values,
- builds composite "pack fan" images from each product's 2nd image,
- saves local previews (dry run), and
- can optionally upload/rewrite the 1st Shopify image.

## Development environment setup

1. Create and activate a virtual environment:

   - `python3 -m virtualenv .venv`
   - `source .venv/bin/activate`

2. Install dependencies:

   - `pip install -r requirements.txt`

3. Create a `.env` file with:

   - `SHOPIFY_TOKEN=...`
   - `SHOPIFY_STORE_URL=your-store.myshopify.com`

4. Run the pipeline:

   - `python test_2products.py`

## Notes

- Keep `DRY_RUN = True` in `test_2products.py` when verifying image output.
- Preview images are written to `./test_output` by default.
- Set `DRY_RUN = False` only when ready to upload to Shopify.
