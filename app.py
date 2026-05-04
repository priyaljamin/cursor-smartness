#!/usr/bin/env python3
"""
MVW Pipeline Web Dashboard
Flask app for managing the product import pipeline with a visual UI.
"""

import os
import json
import threading
import time
from pathlib import Path
from datetime import datetime

from flask import Flask, jsonify, request, send_from_directory, send_file
import pandas as pd

import pipeline

app = Flask(__name__, static_folder="static")

SCRIPT_DIR = Path(__file__).parent
UPLOAD_DIR = SCRIPT_DIR / "input"
IMAGES_DIR = SCRIPT_DIR / "images" / "single"
OUTPUT_DIR = SCRIPT_DIR / "output"
CACHE_DIR  = SCRIPT_DIR / "cache"

_pipeline_status = {
    "running": False,
    "stage": None,
    "progress": "",
    "log": [],
    "started_at": None,
    "finished_at": None,
}
_pipeline_lock = threading.Lock()


def _log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    with _pipeline_lock:
        _pipeline_status["log"].append(f"[{ts}] {msg}")
        if len(_pipeline_status["log"]) > 500:
            _pipeline_status["log"] = _pipeline_status["log"][-300:]


def _run_pipeline_thread(stage, pilot, resume):
    """Run pipeline stages in a background thread."""
    with _pipeline_lock:
        _pipeline_status["running"] = True
        _pipeline_status["stage"] = stage
        _pipeline_status["started_at"] = datetime.now().isoformat()
        _pipeline_status["finished_at"] = None
        _pipeline_status["log"] = []

    try:
        _log(f"Starting pipeline: stage={stage}, pilot={pilot}, resume={resume}")

        input_file = _find_input_file()
        if not input_file:
            _log("ERROR: No input xlsx found in input/ directory")
            return

        pipeline.INPUT_XLSX = Path(input_file)
        df = pipeline.load_products(pilot_limit=pilot)
        _log(f"Loaded {len(df)} products")

        if stage in ("1", "all"):
            _log("Stage 1: UPCitemdb + fallback lookups...")
            pipeline.stage1_enrich(df, resume=resume)
            _log("Stage 1 complete")

        if stage in ("3", "all"):
            if not pipeline.ANTHROPIC_KEY:
                _log("WARN: ANTHROPIC_API_KEY not set, skipping Stage 3")
            else:
                _log("Stage 3: Claude enrichment...")
                upcdb_cache = pipeline.load_cache("upcitemdb")
                pipeline.stage3_claude(df, upcdb_cache, resume=resume)
                _log("Stage 3 complete")

        if stage in ("2", "all"):
            _log("Stage 2: Downloading images (front + back)...")
            upcdb_cache = pipeline.load_cache("upcitemdb")
            pipeline.stage2_images(df, upcdb_cache, resume=resume)
            _log("Stage 2 complete")

        if stage in ("4", "all"):
            _log("Stage 4: Building Matrixify xlsx...")
            upcdb_cache = pipeline.load_cache("upcitemdb")
            enrich_cache = pipeline.load_cache("enrichment")
            pipeline.stage4_build(df, upcdb_cache, enrich_cache)
            _log("Stage 4 complete")

        _log("Pipeline finished successfully")

    except Exception as e:
        _log(f"ERROR: {str(e)[:500]}")
    finally:
        with _pipeline_lock:
            _pipeline_status["running"] = False
            _pipeline_status["finished_at"] = datetime.now().isoformat()


def _find_input_file():
    """Find the first xlsx in the input directory."""
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    for f in sorted(UPLOAD_DIR.glob("*.xlsx")):
        return str(f)
    return None


def _get_products_data():
    """Build a list of product dicts from caches + images on disk."""
    upcdb = pipeline.load_cache("upcitemdb")
    enrichment = pipeline.load_cache("enrichment")
    off_cache = pipeline.load_cache("openfacts")

    input_file = _find_input_file()
    if not input_file:
        return []

    pipeline.INPUT_XLSX = Path(input_file)
    try:
        df = pipeline.load_products(pilot_limit=0)
    except SystemExit:
        return []

    products = []
    for _, row in df.iterrows():
        upc = row["upc"]
        sku = row["variant_sku"]
        u = upcdb.get(upc, {})
        e = enrichment.get(upc, {})
        off = off_cache.get(upc, {})

        front_path = IMAGES_DIR / f"{sku}_front.jpg"
        back_path = IMAGES_DIR / f"{sku}_back.jpg"
        has_front = front_path.exists() and front_path.stat().st_size > 1000
        has_back = back_path.exists() and back_path.stat().st_size > 1000

        stage1_ok = bool(u and "_error" not in u)
        stage3_ok = bool(e and "_error" not in e)

        title = e.get("title", "") or u.get("title", "") or str(row.get("TITLE", ""))
        brand = e.get("brand", "") or u.get("brand", "")

        products.append({
            "upc": upc,
            "sku": sku,
            "warehouse_title": str(row.get("TITLE", "")),
            "title": title,
            "brand": brand,
            "case_size": int(row.get("case_size", 1)),
            "cost": float(row.get("Variant Cost", 0) or 0),
            "price": float(row.get("Variant Price", 0) or 0),
            "master_sku": row.get("master_sku", ""),

            "stage1_ok": stage1_ok,
            "stage3_ok": stage3_ok,
            "has_front": has_front,
            "has_back": has_back,

            "upcitemdb": {k: v for k, v in u.items() if not k.startswith("_")} if stage1_ok else None,
            "enrichment": {k: v for k, v in e.items() if not k.startswith("_")} if stage3_ok else None,
            "enrichment_warnings": e.get("_warnings", []) if stage3_ok else [],

            "weight": e.get("single_unit_weight_lb") if stage3_ok else None,
            "category": e.get("top_category", "") if stage3_ok else "",
            "subcategory": e.get("subcategory", "") if stage3_ok else "",
            "description_html": e.get("description_html", "") if stage3_ok else "",

            "ready": stage1_ok and stage3_ok and has_front,
        })

    return products


# ─── API ROUTES ───────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory("static", "index.html")


@app.route("/static/<path:filename>")
def static_files(filename):
    return send_from_directory("static", filename)


@app.route("/api/status")
def api_status():
    """Pipeline run status."""
    with _pipeline_lock:
        return jsonify(_pipeline_status)


@app.route("/api/dashboard")
def api_dashboard():
    """Dashboard summary stats."""
    products = _get_products_data()
    total = len(products)
    if total == 0:
        return jsonify({"total": 0, "has_input": bool(_find_input_file())})

    return jsonify({
        "total": total,
        "has_input": True,
        "stage1_done": sum(1 for p in products if p["stage1_ok"]),
        "stage3_done": sum(1 for p in products if p["stage3_ok"]),
        "has_front": sum(1 for p in products if p["has_front"]),
        "has_back": sum(1 for p in products if p["has_back"]),
        "ready": sum(1 for p in products if p["ready"]),
        "missing_front": sum(1 for p in products if not p["has_front"]),
        "missing_back": sum(1 for p in products if not p["has_back"]),
        "has_anthropic_key": bool(pipeline.ANTHROPIC_KEY),
    })


@app.route("/api/products")
def api_products():
    """List all products with optional filters."""
    products = _get_products_data()
    filt = request.args.get("filter", "all")
    if filt == "ready":
        products = [p for p in products if p["ready"]]
    elif filt == "missing_front":
        products = [p for p in products if not p["has_front"]]
    elif filt == "missing_back":
        products = [p for p in products if not p["has_back"]]
    elif filt == "needs_enrichment":
        products = [p for p in products if not p["stage3_ok"]]
    elif filt == "needs_review":
        products = [p for p in products if not p["ready"]]

    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 50))
    start = (page - 1) * per_page

    return jsonify({
        "total": len(products),
        "page": page,
        "per_page": per_page,
        "products": products[start:start + per_page],
    })


@app.route("/api/products/<sku>")
def api_product_detail(sku):
    """Single product detail."""
    products = _get_products_data()
    for p in products:
        if p["sku"] == sku:
            return jsonify(p)
    return jsonify({"error": "not found"}), 404


@app.route("/api/images/<filename>")
def api_image(filename):
    """Serve product images."""
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    return send_from_directory(str(IMAGES_DIR), filename)


@app.route("/api/run", methods=["POST"])
def api_run():
    """Start pipeline in background."""
    with _pipeline_lock:
        if _pipeline_status["running"]:
            return jsonify({"error": "Pipeline already running"}), 409

    data = request.get_json() or {}
    stage = data.get("stage", "all")
    pilot = int(data.get("pilot", 0))
    resume = bool(data.get("resume", False))

    t = threading.Thread(target=_run_pipeline_thread, args=(stage, pilot, resume), daemon=True)
    t.start()

    return jsonify({"message": "Pipeline started", "stage": stage, "pilot": pilot})


@app.route("/api/upload", methods=["POST"])
def api_upload():
    """Upload input xlsx."""
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    f = request.files["file"]
    if not f.filename.endswith(".xlsx"):
        return jsonify({"error": "File must be .xlsx"}), 400

    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    save_path = UPLOAD_DIR / f.filename
    f.save(str(save_path))

    return jsonify({"message": f"Uploaded {f.filename}", "path": str(save_path)})


@app.route("/api/upload/image", methods=["POST"])
def api_upload_image():
    """Manually upload front or back image for a product."""
    sku = request.form.get("sku")
    img_type = request.form.get("type", "front")
    if not sku or "file" not in request.files:
        return jsonify({"error": "sku and file required"}), 400

    IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    f = request.files["file"]
    out_path = IMAGES_DIR / f"{sku}_{img_type}.jpg"
    f.save(str(out_path))

    pipeline.process_image_with_padding(str(out_path))

    return jsonify({"message": f"Uploaded {img_type} image for {sku}"})


@app.route("/api/export")
def api_export():
    """Download the Matrixify xlsx."""
    xlsx = OUTPUT_DIR / "matrixify_pilot.xlsx"
    if not xlsx.exists():
        return jsonify({"error": "No export file. Run stage 4 first."}), 404
    return send_file(str(xlsx), as_attachment=True, download_name="matrixify_import.xlsx")


@app.route("/api/export/images")
def api_export_images():
    """Download the images zip."""
    zf = OUTPUT_DIR / "images.zip"
    if not zf.exists():
        return jsonify({"error": "No images zip. Run stage 4 first."}), 404
    return send_file(str(zf), as_attachment=True, download_name="images.zip")


@app.route("/api/clear-cache", methods=["POST"])
def api_clear_cache():
    """Clear all caches to force re-run."""
    data = request.get_json() or {}
    which = data.get("which", "all")
    cleared = []

    for name in ["upcitemdb", "upcitemdb_failed", "openfacts", "enrichment"]:
        if which == "all" or which == name:
            p = CACHE_DIR / f"{name}.json"
            if p.exists():
                p.unlink()
                cleared.append(name)

    return jsonify({"cleared": cleared})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"MVW Pipeline Dashboard: http://localhost:{port}")
    app.run(host="0.0.0.0", port=port, debug=True)
