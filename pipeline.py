#!/usr/bin/env python3
"""
=============================================================
  Master Value Wholesale - Catalog Addition Pipeline (v4)
  Multi-image support: front + back per product
  Fallback sources: UPCitemdb -> Open Food Facts -> Open Beauty Facts
=============================================================

WHAT THIS DOES:
  Stage 1: UPCitemdb lookup + fallback to Open Food/Beauty Facts
  Stage 2: Download FRONT + BACK product images from all sources,
           pad to 1200x1200 with 10% white margin,
           save as images/single/{sku}_front.jpg and {sku}_back.jpg
  Stage 3: Claude generates title/brand/weight/description/category
  Stage 4: Build Matrixify xlsx + auto-zip the images folder

USAGE:
  python pipeline.py --stage all --pilot 10
  python pipeline.py --stage 1 --pilot 50
  python pipeline.py --stage 2 --pilot 50 --resume
  python pipeline.py --stage all --pilot 0          # process whole file

OUTPUTS:
  output/matrixify_pilot.xlsx     <- main deliverable
  output/images.zip               <- upload alongside xlsx in Matrixify
  output/images_missing.csv       <- UPCs with missing images
  output/pipeline_status.csv      <- per-UPC status tracker
  cache/upcitemdb.json            <- Stage 1 cache
  cache/openfacts.json            <- Open Food/Beauty Facts cache
  cache/enrichment.json           <- Stage 3 cache
  images/single/{sku}_front.jpg   <- downloaded + padded front images
  images/single/{sku}_back.jpg    <- downloaded + padded back images
=============================================================
"""

import os
import sys
import json
import re
import time
import argparse
import csv
import zipfile
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from PIL import Image
from tqdm import tqdm

try:
    from anthropic import Anthropic
except ImportError:
    print("ERROR: pip install anthropic")
    sys.exit(1)


# ============================================================
# CONFIG
# ============================================================
SCRIPT_DIR     = Path(__file__).parent
INPUT_XLSX     = SCRIPT_DIR / "input" / "MVW_QK_CATALOG_ADDITION_05_01_2026.xlsx"
OUTPUT_XLSX    = SCRIPT_DIR / "output" / "matrixify_pilot.xlsx"
IMAGES_ZIP     = SCRIPT_DIR / "output" / "images.zip"
MISSING_CSV    = SCRIPT_DIR / "output" / "images_missing.csv"
STATUS_CSV     = SCRIPT_DIR / "output" / "pipeline_status.csv"
CACHE_DIR      = SCRIPT_DIR / "cache"
IMAGES_DIR     = SCRIPT_DIR / "images" / "single"

UPCITEMDB_KEY  = os.environ.get("UPCITEMDB_KEY", "3865ad495b695f896e86e175685045ca")
ANTHROPIC_KEY  = os.environ.get("ANTHROPIC_API_KEY", "")
CLAUDE_MODEL   = "claude-haiku-4-5-20251001"

PILOT_LIMIT_DEFAULT = 50
PARALLEL_WORKERS    = 8
SAVE_EVERY          = 10
TITLE_MAX_CHARS     = 80
WEIGHT_MIN_LB       = 0.05
WEIGHT_MAX_LB       = 150.0
IMG_CANVAS_SIZE     = 1200    # final image dimensions in px
IMG_PADDING_PCT     = 0.10    # white space on each side

OFF_API_URL    = "https://world.openfoodfacts.org/api/v2/product"
OBF_API_URL    = "https://world.openbeautyfacts.org/api/v2/product"


# ============================================================
# CATEGORY TREE  (top -> [subcategories])
# ============================================================
CATEGORY_TREE = {
    # General (7)
    "Hair Care":               ["Hair Color", "Styling Tools", "Spray & Gel", "Hair Removal"],
    "Medcare":                 ["First Aid", "General Medicine", "Family Planning", "Vision & Hearing"],
    "Supplements & Fitness":   ["Vitamins"],
    "Bath & Body":             ["Body Wash", "Shampoo & Conditioner", "Deodorants", "Oral Care", "Soaps"],
    "Baby & Mother":           ["Baby Needs", "Diapers & Wipes"],
    "Skin & Beauty":           ["Skin Care", "Cosmetics", "Lip Care", "Nail Care", "Sun Care"],
    "Household":               ["Cleaning Supplies", "Air Fresheners", "Stationery", "Grocery",
                                "Home Goods", "Pet Supplies", "Automotive"],
    # Multi-cultural (7)
    "Cultural Cosmetics":      ["Cosmetics & Makeup", "Fragrance", "Hair Removal & Waxing",
                                "Lashes & Brows", "Nail Care"],
    "Essential Hair Treatment":["Hair Oils & Treatments", "Kids Hair Care", "Leave-In & Detanglers",
                                "Masks & Relaxers", "Shampoo & Conditioner"],
    "Ethnic Skin Care":        ["Bath & Body Care", "Body Lotions & Creams", "Body Oils & Butters",
                                "Face & Skin Care"],
    "Men's Textured Grooming": ["Beard Care", "Men's Hair Care", "Men's Skin & Body",
                                "Shaving & Bump Care"],
    "Multiethnic Hair Color":  ["Color Care & Touch-Ups", "Developers & Lighteners",
                                "Henna & Natural Color", "Permanent Color",
                                "Semi-Permanent & Temporary Color"],
    "Natural Hair Styling":    ["Braiding & Locs", "Curl Definers & Creams", "Gels & Edge Control",
                                "Mousse", "Pomade & Wax"],
    "Specialty Hair Tools":    ["Bonnets", "Brushes & Combs", "Hair Accessories",
                                "Rollers & Clips", "Wig & Weave Care"],
}


# ============================================================
# CACHE HELPERS
# ============================================================
def load_cache(name):
    p = CACHE_DIR / f"{name}.json"
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"[cache] WARN: failed to load {name}: {e}")
    return {}

def save_cache(name, data):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    (CACHE_DIR / f"{name}.json").write_text(
        json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8"
    )


# ============================================================
# INPUT
# ============================================================
def load_products(pilot_limit=None):
    if not INPUT_XLSX.exists():
        sys.exit(f"ERROR: input file not found: {INPUT_XLSX}")

    df = pd.read_excel(INPUT_XLSX, dtype={
        "Variant Barcode": str,
        "Variant SKU": str,
        "Metafield: custom.master_sku [single_line_text_field]": str,
    })

    df = df.rename(columns={
        "Metafield: custom.case_size [number_integer]":         "case_size",
        "Metafield: custom.master_sku [single_line_text_field]":"master_sku",
        "Variant Barcode":                                       "upc",
        "Variant SKU":                                           "variant_sku",
    })

    df["upc"] = df["upc"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)

    if pilot_limit and pilot_limit > 0:
        df = df.head(pilot_limit).copy()
        print(f"[input] PILOT: {len(df)} rows")
    else:
        print(f"[input] FULL: {len(df)} rows")
    return df


# ============================================================
# STAGE 1: UPCITEMDB
# ============================================================
def upcitemdb_lookup(upc):
    headers = {
        "Accept": "application/json",
        "user_key": UPCITEMDB_KEY,
        "key_type": "3scale",
    }
    try:
        r = requests.get("https://api.upcitemdb.com/prod/v1/lookup",
                         params={"upc": upc}, headers=headers, timeout=15)
        if r.status_code != 200:
            return {"_error": f"http_{r.status_code}"}
        items = r.json().get("items", []) or []
        if not items:
            return {"_error": "no_items"}
        it = items[0]
        return {
            "title":        it.get("title", "") or "",
            "brand":        it.get("brand", "") or "",
            "description":  it.get("description", "") or "",
            "category":     it.get("category", "") or "",
            "size":         it.get("size", "") or "",
            "weight":       it.get("weight", "") or "",
            "upc":          it.get("upc", upc) or upc,
            "images":       it.get("images", []) or [],
        }
    except Exception as e:
        return {"_error": "exception", "_msg": str(e)[:200]}


def openfacts_lookup(upc, api_url):
    """Lookup a UPC on Open Food Facts or Open Beauty Facts (public domain, no key needed)."""
    try:
        r = requests.get(f"{api_url}/{upc}.json",
                         headers={"User-Agent": "MVW-Catalog/1.0"},
                         timeout=15)
        if r.status_code != 200:
            return None
        data = r.json()
        if data.get("status") != 1:
            return None
        p = data.get("product", {})
        images = []
        if p.get("image_front_url"):
            images.append(p["image_front_url"])
        if p.get("image_url"):
            images.append(p["image_url"])
        front_images = []
        back_images = []
        for key in sorted(p.keys()):
            if key.startswith("image_front_") and key.endswith("_url") and p[key]:
                front_images.append(p[key])
            if key.startswith("image_back_") and key.endswith("_url") and p[key]:
                back_images.append(p[key])
        if not images and front_images:
            images = front_images
        return {
            "title":        p.get("product_name", "") or "",
            "brand":        p.get("brands", "") or "",
            "description":  p.get("generic_name", "") or "",
            "category":     p.get("categories", "") or "",
            "size":         p.get("quantity", "") or "",
            "weight":       p.get("product_quantity", "") or "",
            "upc":          upc,
            "images":       images,
            "front_images": front_images,
            "back_images":  back_images,
            "_source":      "openfoodfacts" if "openfood" in api_url else "openbeautyfacts",
        }
    except Exception:
        return None


def stage1_enrich(df, resume=False):
    cache  = load_cache("upcitemdb") if resume else load_cache("upcitemdb")
    failed = load_cache("upcitemdb_failed") if resume else {}
    off_cache = load_cache("openfacts")

    if not resume:
        cache = load_cache("upcitemdb")

    todo = [u for u in df["upc"].tolist() if u not in cache and u not in failed]
    print(f"[stage1] {len(todo)} UPCs to lookup ({len(cache)} cached, {len(failed)} previously failed)")

    if not todo:
        return cache, failed

    save_counter = 0
    with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as ex:
        futures = {ex.submit(upcitemdb_lookup, u): u for u in todo}
        for fut in tqdm(as_completed(futures), total=len(futures), desc="UPCitemdb"):
            upc = futures[fut]
            try:
                res = fut.result()
            except Exception as e:
                res = {"_error": "future", "_msg": str(e)[:200]}
            if res and "_error" not in res:
                cache[upc] = res
            else:
                failed[upc] = res or {"_error": "unknown"}
            save_counter += 1
            if save_counter >= SAVE_EVERY:
                save_cache("upcitemdb", cache)
                save_cache("upcitemdb_failed", failed)
                save_counter = 0

    save_cache("upcitemdb", cache)
    save_cache("upcitemdb_failed", failed)

    # Fallback: try Open Food Facts + Open Beauty Facts for UPCs that UPCitemdb missed
    fallback_todo = [u for u in df["upc"].tolist()
                     if u not in off_cache and (u in failed or u not in cache)]
    if fallback_todo:
        print(f"[stage1] Trying {len(fallback_todo)} UPCs on Open Food/Beauty Facts...")
        for upc in tqdm(fallback_todo, desc="OpenFacts"):
            for api_url in [OFF_API_URL, OBF_API_URL]:
                result = openfacts_lookup(upc, api_url)
                if result:
                    off_cache[upc] = result
                    if upc in failed:
                        del failed[upc]
                    if upc not in cache:
                        cache[upc] = result
                    break
            else:
                off_cache[upc] = {"_error": "not_found"}
            time.sleep(0.5)
        save_cache("openfacts", off_cache)
        save_cache("upcitemdb", cache)
        save_cache("upcitemdb_failed", failed)

    total_hit = sum(1 for u in df["upc"].tolist() if u in cache and "_error" not in cache.get(u, {}))
    total_miss = len(df) - total_hit
    print(f"[stage1] DONE - {total_hit} hit, {total_miss} miss (across all sources)")
    return cache, failed


# ============================================================
# STAGE 2: IMAGE DOWNLOAD + PADDING (UPCitemdb only)
# ============================================================
_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; MVW-Catalog/1.0)"}


def process_image_with_padding(input_path, output_path=None,
                                canvas_size=IMG_CANVAS_SIZE,
                                padding_pct=IMG_PADDING_PCT):
    """
    Open image, find non-white bbox of product, crop tight,
    paste centered on white canvas with padding on all sides,
    resize to canvas_size x canvas_size. Saves as JPEG quality 92.
    """
    if output_path is None:
        output_path = input_path
    try:
        img = Image.open(input_path).convert("RGB")
    except Exception:
        return False

    # Detect product silhouette: threshold near-white pixels out
    gray = img.convert("L")
    mask = gray.point(lambda p: 255 if p < 245 else 0, mode="L")
    bbox = mask.getbbox()

    if bbox is None:
        canvas = Image.new("RGB", (canvas_size, canvas_size), (255, 255, 255))
        canvas.save(output_path, "JPEG", quality=92)
        return True

    cropped = img.crop(bbox)
    cw, ch = cropped.size

    inner = int(canvas_size * (1 - 2 * padding_pct))
    scale = min(inner / cw, inner / ch)
    new_w, new_h = max(1, int(cw * scale)), max(1, int(ch * scale))
    cropped = cropped.resize((new_w, new_h), Image.LANCZOS)

    canvas = Image.new("RGB", (canvas_size, canvas_size), (255, 255, 255))
    px = (canvas_size - new_w) // 2
    py = (canvas_size - new_h) // 2
    canvas.paste(cropped, (px, py))
    canvas.save(output_path, "JPEG", quality=92)
    return True


def try_download_url(url, out_path):
    if not url or not url.startswith(("http://", "https://")):
        return False
    try:
        r = requests.get(url, headers=_HEADERS, timeout=15)
        if r.status_code != 200:
            return False
        content = r.content
        if len(content) < 1000:
            return False
        ctype = r.headers.get("content-type", "").lower()
        if "image" not in ctype and not url.lower().endswith((".jpg", ".jpeg", ".png", ".webp")):
            return False
        out_path.write_bytes(content)
        return True
    except Exception:
        return False


def _pick_image_urls(udb_data, off_cache_entry=None):
    """Gather front and back image URLs from all available sources."""
    front_urls = []
    back_urls = []

    for url in (udb_data.get("images") or [])[:10]:
        front_urls.append(url)

    if off_cache_entry and "_error" not in off_cache_entry:
        for url in (off_cache_entry.get("front_images") or []):
            if url not in front_urls:
                front_urls.append(url)
        for url in (off_cache_entry.get("back_images") or []):
            back_urls.append(url)
        for url in (off_cache_entry.get("images") or []):
            if url not in front_urls:
                front_urls.append(url)

    return front_urls[:10], back_urls[:10]


def fetch_product_images(upc, sku, udb_data, off_data=None):
    """Download front + back images. Returns dict with status per image type."""
    result = {"front": False, "back": False, "front_source": "", "back_source": ""}
    front_urls, back_urls = _pick_image_urls(udb_data, off_data)

    front_path = IMAGES_DIR / f"{sku}_front.jpg"
    back_path = IMAGES_DIR / f"{sku}_back.jpg"

    if front_path.exists() and front_path.stat().st_size > 1000:
        result["front"] = True
        result["front_source"] = "cached"
    else:
        for url in front_urls:
            if try_download_url(url, front_path):
                process_image_with_padding(front_path)
                result["front"] = True
                result["front_source"] = "downloaded"
                break

    if back_path.exists() and back_path.stat().st_size > 1000:
        result["back"] = True
        result["back_source"] = "cached"
    else:
        for url in back_urls:
            if try_download_url(url, back_path):
                process_image_with_padding(back_path)
                result["back"] = True
                result["back_source"] = "downloaded"
                break

    return result


def stage2_images(df, upcdb_cache, resume=False):
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    off_cache = load_cache("openfacts")
    missing = []
    front_ok = 0
    back_ok = 0

    todo = []
    for _, row in df.iterrows():
        upc = row["upc"]
        sku = row["variant_sku"]
        front_path = IMAGES_DIR / f"{sku}_front.jpg"
        back_path = IMAGES_DIR / f"{sku}_back.jpg"
        if resume and front_path.exists() and front_path.stat().st_size > 1000:
            front_ok += 1
            if back_path.exists() and back_path.stat().st_size > 1000:
                back_ok += 1
            continue
        todo.append((upc, sku, upcdb_cache.get(upc, {}), off_cache.get(upc)))

    print(f"[stage2] {len(todo)} products to fetch images for ({front_ok} front cached)")

    with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as ex:
        futures = {ex.submit(fetch_product_images, u, s, udb, off): (u, s, udb)
                   for u, s, udb, off in todo}
        for fut in tqdm(as_completed(futures), total=len(futures), desc="Images"):
            upc, sku, udb = futures[fut]
            res = fut.result()
            if res["front"]:
                front_ok += 1
            if res["back"]:
                back_ok += 1
            if not res["front"]:
                missing.append({
                    "upc": upc,
                    "variant_sku": sku,
                    "reason": "no_front_image",
                    "has_back": "yes" if res["back"] else "no",
                    "title": udb.get("title", ""),
                    "brand": udb.get("brand", ""),
                })

    if missing:
        MISSING_CSV.parent.mkdir(parents=True, exist_ok=True)
        with MISSING_CSV.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["upc", "variant_sku", "reason",
                                              "has_back", "title", "brand"])
            w.writeheader()
            w.writerows(missing)
        print(f"[stage2] {len(missing)} missing front images -> {MISSING_CSV}")

    print(f"[stage2] DONE - front: {front_ok}, back: {back_ok}, missing: {len(missing)}")


# ============================================================
# STAGE 3: CLAUDE ENRICHMENT
# ============================================================
def build_category_block():
    lines = []
    for top, subs in CATEGORY_TREE.items():
        lines.append(f"  {top}:")
        for s in subs:
            lines.append(f"    - {s}")
    return "\n".join(lines)


CATEGORY_BLOCK = build_category_block()


PROMPT_TEMPLATE = """You are a product catalog editor for Master Value Wholesale, a US health & beauty wholesaler. Generate clean retail-ready data for ONE product. Return ONLY a single valid JSON object - no markdown fences, no commentary.

WAREHOUSE TITLE (abbreviated, internal):
  {warehouse_title}

UPCITEMDB DATA:
  Title:       {udb_title}
  Brand:       {udb_brand}
  Size:        {udb_size}
  Weight:      {udb_weight}
  Category:    {udb_category}
  Description: {udb_desc}

PACK INFO:
  Case size:   {case_size}  (units per case)
  UPC:         {upc}

CATEGORY TREE - you MUST pick exactly one top category and one subcategory FROM THIS LIST:
{cat_block}

OUTPUT JSON SCHEMA (all fields required):
{{
  "title":               "...",        // "{{Brand}} {{Product}} {{Size}} (Pack of {{N}})"
  "brand":               "...",
  "product_name":        "...",        // just the product, no brand/size/pack
  "single_unit_size":    "...",        // e.g. "2.6 Oz"
  "single_unit_weight_lb": 0.0,        // weight of ONE unit in lb
  "description_html":    "...",        // 4 <p> paragraphs
  "top_category":        "...",
  "subcategory":         "...",
  "category_confidence": "high|medium|low",
  "meta_title":          "...",        // <= 60 chars
  "meta_description":    "..."         // <= 160 chars
}}

TITLE RULES:
- Format: "{{Brand}} {{Product}} {{Size}} (Pack of {{case_size}})"
- Max {title_max} chars - abbreviate Product if needed, NEVER drop brand/size/pack
- Title Case, no ALL CAPS
- No promo words: free, best, premium, deluxe, sale, discount
- No retailer names, no years
- Example: "Burt's Bees Honey & Grapeseed Hand Cream 2.6 Oz (Pack of 18)"

WEIGHT RULES:
- Weight of ONE unit in pounds (numeric)
- Between {wmin} and {wmax}
- Convert oz/g/kg to lb if needed
- Estimate from product type if no source data

DESCRIPTION RULES (CRITICAL):
- ORIGINAL content in your own words. Do NOT copy UPCitemdb description.
- 4 short <p> paragraphs (no lists, no headers): purpose; features/benefits; who/how; case-pack value for resellers
- 130-220 words total
- No prohibited words: hemp, CBD, THC, cannabis, marijuana, kratom, opioid, narcotic, free, best
- No medical claims (cure, treat, heal, prevent)

CATEGORY RULES:
- Use Multi-cultural top categories ONLY for ethnic/textured-hair/cultural beauty products (Cantu, SheaMoisture, Ambi, Magic Shaving, ORS, etc.)
- Mainstream brands (CoverGirl, Tic Tac, Dove, Colgate) -> General categories

Return ONLY the JSON object.
"""


def call_claude_for_product(client, product_data):
    prompt = PROMPT_TEMPLATE.format(
        warehouse_title=product_data["warehouse_title"],
        udb_title=product_data["udb"].get("title", "(none)"),
        udb_brand=product_data["udb"].get("brand", "(none)"),
        udb_size=product_data["udb"].get("size", "(none)"),
        udb_weight=product_data["udb"].get("weight", "(none)"),
        udb_category=product_data["udb"].get("category", "(none)"),
        udb_desc=(product_data["udb"].get("description", "") or "(none)")[:600],
        case_size=product_data["case_size"],
        upc=product_data["upc"],
        cat_block=CATEGORY_BLOCK,
        title_max=TITLE_MAX_CHARS,
        wmin=WEIGHT_MIN_LB,
        wmax=WEIGHT_MAX_LB,
    )

    raw = ""
    for attempt in range(3):
        try:
            resp = client.messages.create(
                model=CLAUDE_MODEL,
                max_tokens=1500,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = resp.content[0].text.strip()
            raw = re.sub(r"^```(?:json)?\s*", "", raw)
            raw = re.sub(r"\s*```$", "", raw)
            return json.loads(raw)
        except json.JSONDecodeError as e:
            if attempt == 2:
                return {"_error": f"json_parse: {e}", "_raw": raw[:300]}
            time.sleep(1)
        except Exception as e:
            if attempt == 2:
                return {"_error": f"api: {str(e)[:200]}"}
            time.sleep(2 ** attempt)
    return {"_error": "max_retries"}


def validate_enrichment(data, case_size):
    warnings = []

    if len(data.get("title", "")) > TITLE_MAX_CHARS:
        warnings.append(f"title_too_long_{len(data['title'])}")
        data["title"] = data["title"][:TITLE_MAX_CHARS].rstrip()

    try:
        w = float(data.get("single_unit_weight_lb", 0))
        if not (WEIGHT_MIN_LB <= w <= WEIGHT_MAX_LB):
            warnings.append(f"weight_oob_{w}")
            data["single_unit_weight_lb"] = 0.5
    except (TypeError, ValueError):
        warnings.append("weight_not_numeric")
        data["single_unit_weight_lb"] = 0.5

    top = data.get("top_category", "")
    sub = data.get("subcategory", "")
    if top not in CATEGORY_TREE:
        warnings.append(f"top_cat_invalid_{top}")
        data["top_category"] = "Household"
        data["subcategory"] = "Home Goods"
        data["category_confidence"] = "low"
    elif sub not in CATEGORY_TREE[top]:
        warnings.append(f"sub_invalid_{sub}_for_{top}")
        data["subcategory"] = CATEGORY_TREE[top][0]
        data["category_confidence"] = "low"

    try:
        unit_w = float(data["single_unit_weight_lb"])
        cs = int(case_size) if case_size else 1
        data["case_weight_lb"] = round(unit_w * cs, 2)
    except Exception:
        data["case_weight_lb"] = 0.5
        warnings.append("case_weight_calc_failed")

    if warnings:
        data["_warnings"] = warnings
    return data


def stage3_claude(df, upcdb_cache, resume=False):
    if not ANTHROPIC_KEY:
        sys.exit("ERROR: ANTHROPIC_API_KEY env var not set")
    client = Anthropic(api_key=ANTHROPIC_KEY)

    cache = load_cache("enrichment") if resume else load_cache("enrichment")

    todo = []
    for _, row in df.iterrows():
        upc = row["upc"]
        if upc in cache and "_error" not in cache[upc]:
            continue
        todo.append({
            "upc":             upc,
            "warehouse_title": str(row.get("TITLE", "")),
            "case_size":       row.get("case_size", 1),
            "udb":             upcdb_cache.get(upc, {}),
        })

    print(f"[stage3] {len(todo)} products to enrich ({len(cache)} cached)")

    save_counter = 0
    with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as ex:
        futures = {ex.submit(call_claude_for_product, client, p): p for p in todo}
        for fut in tqdm(as_completed(futures), total=len(futures), desc="Claude"):
            p = futures[fut]
            upc = p["upc"]
            try:
                res = fut.result()
            except Exception as e:
                res = {"_error": f"thread: {str(e)[:200]}"}
            if "_error" not in res:
                res = validate_enrichment(res, p["case_size"])
            cache[upc] = res
            save_counter += 1
            if save_counter >= SAVE_EVERY:
                save_cache("enrichment", cache)
                save_counter = 0

    save_cache("enrichment", cache)
    ok = sum(1 for v in cache.values() if "_error" not in v)
    err = len(cache) - ok
    print(f"[stage3] DONE - {ok} ok, {err} errored")


# ============================================================
# STAGE 4: BUILD MATRIXIFY XLSX + ZIP IMAGES
# ============================================================
def make_handle(title):
    h = re.sub(r"[^a-zA-Z0-9\s-]", "", title)
    h = re.sub(r"\s+", "-", h).strip("-").lower()
    return h[:120]


def assess_review_reasons(e, u, front_exists, back_exists):
    reasons = []
    if "_error" in e or not e:
        reasons.append("enrichment_failed")
        return reasons
    if not front_exists:
        reasons.append("no_front_image")
    if not back_exists:
        reasons.append("no_back_image")
    if not u or "_error" in u:
        reasons.append("upcitemdb_miss")
    if e.get("category_confidence") == "low":
        reasons.append("category_low_confidence")
    if e.get("_warnings"):
        for w in e["_warnings"]:
            if w.startswith("weight_oob") or w == "weight_not_numeric":
                reasons.append("weight_estimated")
            if w.startswith("title_too_long"):
                reasons.append("title_truncated")
            if w.startswith("top_cat_invalid") or w.startswith("sub_invalid"):
                reasons.append("category_fallback")
    if not e.get("brand"):
        reasons.append("no_brand")
    if len(e.get("title", "")) < 15:
        reasons.append("title_too_short")
    return reasons


def stage4_build(df, upcdb_cache, enrichment_cache):
    rows_out = []
    status_rows = []

    for _, row in df.iterrows():
        upc = row["upc"]
        sku = row["variant_sku"]
        case_size = row.get("case_size", 1)
        cost = row.get("Variant Cost", "")
        price = row.get("Variant Price", "")

        e = enrichment_cache.get(upc, {})
        u = upcdb_cache.get(upc, {})
        front_path = IMAGES_DIR / f"{sku}_front.jpg"
        back_path = IMAGES_DIR / f"{sku}_back.jpg"
        front_exists = front_path.exists() and front_path.stat().st_size > 1000
        back_exists = back_path.exists() and back_path.stat().st_size > 1000

        review_reasons = assess_review_reasons(e, u, front_exists, back_exists)
        is_ready = all(r not in ("enrichment_failed", "no_front_image") for r in review_reasons)

        status_rows.append({
            "upc": upc,
            "variant_sku": sku,
            "stage1_upcitemdb": "OK" if u and "_error" not in u else "MISS",
            "stage2_front":     "OK" if front_exists else "MISS",
            "stage2_back":      "OK" if back_exists else "MISS",
            "stage3_claude":    "OK" if e and "_error" not in e else "MISS",
            "ready":            "YES" if is_ready else "NO",
            "review_reason":    ",".join(review_reasons),
            "warnings":         ",".join(e.get("_warnings", [])) if e else "",
        })

        if "_error" in e or not e:
            continue

        title = e.get("title", "")
        handle = make_handle(title)

        tags = [
            e.get("top_category", ""),
            e.get("subcategory", ""),
            "Case Pack",
            "Wholesale",
        ]
        if e.get("top_category") in {
            "Cultural Cosmetics", "Essential Hair Treatment", "Ethnic Skin Care",
            "Men's Textured Grooming", "Multiethnic Hair Color",
            "Natural Hair Styling", "Specialty Hair Tools"
        }:
            tags.append("Multi-Cultural")

        rows_out.append({
            "Review Reason":           ", ".join(review_reasons),

            "Handle":                  handle,
            "Command":                 "NEW",
            "Title":                   title,
            "Body HTML":               e.get("description_html", ""),
            "Vendor":                  e.get("brand", ""),
            "Type":                    e.get("top_category", ""),
            "Tags":                    ", ".join([t for t in tags if t]),
            "Tags Command":            "REPLACE",
            "Published":               "TRUE",
            "Status":                  "draft",
            "SEO Title":               e.get("meta_title", title)[:60],
            "SEO Description":         e.get("meta_description", "")[:160],

            "Variant SKU":             sku,
            "Variant Barcode":         upc,
            "Variant Weight":          e.get("case_weight_lb", 0),
            "Variant Weight Unit":     "lb",
            "Variant Cost":            cost,
            "Variant Price":           price,
            "Variant Inventory Tracker": "shopify",
            "Variant Inventory Policy":  "deny",
            "Variant Fulfillment Service": "manual",
            "Variant Requires Shipping":   "TRUE",
            "Variant Taxable":             "TRUE",

            "Image Src":               f"{sku}_front.jpg" if front_exists else "",
            "Image Position":          1 if front_exists else "",
            "Image Alt Text":          title if front_exists else "",

            "Metafield: custom.case_size [number_integer]":           case_size,
            "Metafield: custom.master_sku [single_line_text_field]":  row.get("master_sku", ""),
            "Metafield: custom.subcategory [single_line_text_field]": e.get("subcategory", ""),
            "Metafield: custom.category_confidence [single_line_text_field]": e.get("category_confidence", ""),
        })

        if back_exists:
            rows_out.append({
                "Handle":          handle,
                "Image Src":       f"{sku}_back.jpg",
                "Image Position":  2,
                "Image Alt Text":  f"{title} - Back",
            })

    # Status tracker
    STATUS_CSV.parent.mkdir(parents=True, exist_ok=True)
    with STATUS_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(status_rows[0].keys()))
        w.writeheader()
        w.writerows(status_rows)
    print(f"[stage4] Status -> {STATUS_CSV}")

    OUTPUT_XLSX.parent.mkdir(parents=True, exist_ok=True)

    if not rows_out:
        print("[stage4] WARN: no enriched products. Run stage 3 first.")
        return

    pd.DataFrame(rows_out).to_excel(OUTPUT_XLSX, index=False)

    ready_count  = sum(1 for r in rows_out if not r["Review Reason"])
    review_count = sum(1 for r in rows_out if r["Review Reason"])
    skipped      = len(status_rows) - len(rows_out)
    print(f"[stage4] {len(rows_out)} products -> {OUTPUT_XLSX}")
    print(f"[stage4]   ready: {ready_count}  needs review: {review_count}  skipped: {skipped}")

    # Auto-zip the images folder
    try:
        all_imgs = sorted([p for p in IMAGES_DIR.glob("*.jpg") if p.stat().st_size > 1000])
        if all_imgs:
            with zipfile.ZipFile(IMAGES_ZIP, "w", zipfile.ZIP_DEFLATED) as zf:
                for p in all_imgs:
                    zf.write(p, arcname=p.name)
            print(f"[stage4] images.zip: {len(all_imgs)} files -> {IMAGES_ZIP}")
            print(f"[stage4] NOTE: in Matrixify, upload BOTH the xlsx AND images.zip together.")
    except Exception as ex:
        print(f"[stage4] WARN: could not create images.zip: {ex}")


# ============================================================
# CLI
# ============================================================
def main():
    ap = argparse.ArgumentParser(description="MVW catalog addition pipeline (v3)")
    ap.add_argument("--stage", choices=["1", "2", "3", "4", "all"], default="all")
    ap.add_argument("--pilot", type=int, default=PILOT_LIMIT_DEFAULT,
                    help="Limit to first N rows (0 = all)")
    ap.add_argument("--resume", action="store_true",
                    help="Skip items already in cache / on disk")
    args = ap.parse_args()

    print("=" * 60)
    print(f"  MVW Pipeline v3 | stage={args.stage} pilot={args.pilot} resume={args.resume}")
    print("=" * 60)

    df = load_products(pilot_limit=args.pilot)

    if args.stage in ("1", "all"):
        stage1_enrich(df, resume=args.resume)
    if args.stage in ("3", "all"):
        upcdb_cache = load_cache("upcitemdb")
        stage3_claude(df, upcdb_cache, resume=args.resume)
    if args.stage in ("2", "all"):
        upcdb_cache = load_cache("upcitemdb")
        stage2_images(df, upcdb_cache, resume=args.resume)
    if args.stage in ("4", "all"):
        upcdb_cache = load_cache("upcitemdb")
        enrich_cache = load_cache("enrichment")
        stage4_build(df, upcdb_cache, enrich_cache)

    print("\nDone.")


if __name__ == "__main__":
    main()
