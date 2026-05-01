# MVW Catalog Addition Pipeline (v3 - simplified)

UPCitemdb only. One image per product. Padded to 1200x1200. Saved as `{SKU}_front.jpg`.

## Stages

| Stage | Action | Output |
|---|---|---|
| 1 | UPCitemdb lookup | `cache/upcitemdb.json` |
| 2 | Download one image, pad, save as `{sku}_front.jpg` | `images/single/M104661_front.jpg` |
| 3 | Claude generates title/brand/weight/description/category | `cache/enrichment.json` |
| 4 | Build Matrixify xlsx + auto-zip images | `output/matrixify_pilot.xlsx`, `output/images.zip` |

When you run `--stage all`, the order is 1 -> 3 -> 2 -> 4 (Claude before images, so weights and metadata are computed even for products UPCitemdb missed).

## Setup

```powershell
pip install pandas openpyxl requests anthropic tqdm pillow
```

```powershell
$env:ANTHROPIC_API_KEY = "sk-ant-..."
$env:UPCITEMDB_KEY     = "3865ad495b695f896e86e175685045ca"
```

## Run the pilot

```powershell
cd C:\Users\pri83\OneDrive\Desktop\MasterValue Wholesale\mvw_pipeline
python pipeline.py --stage all --pilot 10
```

About 1-2 minutes. Output is `output/matrixify_pilot.xlsx` and `output/images.zip`.

## Run stages individually

```powershell
python pipeline.py --stage 1 --pilot 10              # UPCitemdb
python pipeline.py --stage 3 --pilot 10              # Claude (faster if 1 already done)
python pipeline.py --stage 2 --pilot 10              # Images
python pipeline.py --stage 2 --pilot 10 --resume     # only retry products with missing images
python pipeline.py --stage 4 --pilot 10              # rebuild xlsx (no API calls)
```

## Output files

| File | What |
|---|---|
| `output/matrixify_pilot.xlsx` | Single import file. First column "Review Reason" - filter blank to see ready products |
| `output/images.zip` | All `{sku}_front.jpg` images, ready to upload alongside the xlsx |
| `output/pipeline_status.csv` | Per-UPC status: stage1 OK/MISS, stage2 OK/MISS, stage3 OK/MISS, ready Y/N |
| `output/images_missing.csv` | UPCs UPCitemdb has no image for - source manually from brand portals |

## Importing to Shopify via Matrixify

1. Apps -> Matrixify -> New Import
2. Drop `output/matrixify_pilot.xlsx` in the upload area
3. Drop `output/images.zip` - Matrixify auto-detects and pulls images from it
4. Click Import

Products import as **draft status**. Review in Shopify Admin, then bulk-set to Active when ready.

## Move pilot to full batch

When the 10-product pilot looks good:

```powershell
python pipeline.py --stage all --pilot 0     # 0 = process the entire input file
```

Existing caches are reused, so this only does work for new UPCs.

## Cost summary

| | Pilot (10) | Full (5,000) |
|---|---|---|
| UPCitemdb | included | included |
| Claude Haiku | ~$0.02 | ~$10 |
| Time | ~1 min | ~30 min |

## Troubleshooting

| Problem | Fix |
|---|---|
| `ANTHROPIC_API_KEY not set` | `$env:ANTHROPIC_API_KEY = "..."` then re-run |
| Stage 2 missing many images | UPCitemdb doesn't have images for those UPCs. See `images_missing.csv`. Run `python diag.py` for per-product breakdown |
| Image looks too zoomed/cropped | Adjust `IMG_PADDING_PCT` in pipeline.py (currently 0.10 = 10%) |
| Need to redo from scratch | `Remove-Item cache\*.json, images\single\*.jpg, output\*` then re-run |
