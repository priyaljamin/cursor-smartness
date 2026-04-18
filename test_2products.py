"""
=============================================================
  Shopify Pack Image — TEST (2 products only)
  Master Value Wholesale
=============================================================
LOGIC:
  - SOURCE image  = product's 2nd image (single product photo)
  - OUTPUT image  = composite fan arrangement (replaces 1st image)

SETUP:
  1. pip install pillow requests
  2. Fill in SHOPIFY_TOKEN below (reuse from mastervalue-tax-exempt Vercel app)
     Go to: Shopify Admin → Settings → Apps → Develop apps → your app → API credentials
     Make sure scopes include: read_products + write_products
  3. Run with DRY_RUN = True first → check ./test_output/ folder for previews
  4. If images look good → set DRY_RUN = False → run again to upload
=============================================================
"""

import requests, time, io, math, os, base64, re
from PIL import Image, ImageDraw, ImageFont, ImageFilter
from dotenv import load_dotenv
import os

def remove_white_background(img, threshold=245, soften=8):
    img = img.convert("RGBA")
    pixels = img.load()
    w, h = img.size

    for y in range(h):
        for x in range(w):
            r, g, b, a = pixels[x, y]

            # fully remove very white pixels
            if r >= threshold and g >= threshold and b >= threshold:
                pixels[x, y] = (255, 255, 255, 0)
            else:
                # soften near-white edge pixels instead of leaving harsh halo
                avg = (r + g + b) / 3
                if avg > threshold - 25:
                    new_alpha = max(0, min(255, int(a * 0.65)))
                    pixels[x, y] = (r, g, b, new_alpha)

    alpha = img.split()[-1].filter(ImageFilter.GaussianBlur(soften * 0.25))
    img.putalpha(alpha)
    return img


def trim_transparent(img):
    bbox = img.getbbox()
    if bbox:
        return img.crop(bbox)
    return img


def load_font(size):
    font_paths = [
        "C:/Windows/Fonts/arialbd.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf",
    ]
    for path in font_paths:
        try:
            return ImageFont.truetype(path, size)
        except:
            pass
    return ImageFont.load_default()

# ─── CONFIG ───────────────────────────────────────────────
load_dotenv()  # loads variables from .env
SHOPIFY_TOKEN = os.getenv("SHOPIFY_TOKEN")
SHOPIFY_STORE_URL = os.getenv("SHOPIFY_STORE_URL")  # ← paste your token here

METAFIELD_NAMESPACE = "custom"
METAFIELD_KEY       = "case_size"     # your metafield key

TEST_LIMIT          = 10              # only 2 products for test
DRY_RUN             = True            # True = generate images only, no upload to Shopify
LOCAL_OUTPUT_DIR    = "./test_output"
# ─────────────────────────────────────────────────────────

API_BASE = f"https://{SHOPIFY_STORE_URL}/admin/api/2026-04"
HEADERS  = {"X-Shopify-Access-Token": SHOPIFY_TOKEN, "Content-Type": "application/json"}
CANVAS_SIZE = (1200, 1200)


# ─── Shopify helpers ──────────────────────────────────────

def shopify_get(endpoint, params=None):
    url = f"{API_BASE}/{endpoint}"
    for _ in range(5):
        r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        if r.status_code == 429:
            time.sleep(int(r.headers.get("Retry-After", 4)))
            continue
        r.raise_for_status()
        return r.json()
    raise Exception(f"Failed GET {endpoint}")

def shopify_post(endpoint, payload):
    url = f"{API_BASE}/{endpoint}"
    for _ in range(5):
        r = requests.post(url, headers=HEADERS, json=payload, timeout=60)
        if r.status_code == 429:
            time.sleep(int(r.headers.get("Retry-After", 4)))
            continue
        r.raise_for_status()
        return r.json()
    raise Exception(f"Failed POST {endpoint}")

def shopify_delete(endpoint):
    url = f"{API_BASE}/{endpoint}"
    r = requests.delete(url, headers=HEADERS, timeout=30)
    if r.status_code not in (200, 204):
        print(f"  Warning: DELETE returned {r.status_code}")

def get_products(limit=2):
    data = shopify_get("products.json", params={
        "limit": limit,
        "fields": "id,title,images,handle"
    })
    return data.get("products", [])

def get_case_size(product_id):
    """Read pack quantity from custom.case_size metafield."""
    try:
        data = shopify_get(
            f"products/{product_id}/metafields.json",
            params={"namespace": METAFIELD_NAMESPACE, "key": METAFIELD_KEY}
        )
        mfs = data.get("metafields", [])
        if mfs:
            val = str(mfs[0].get("value", ""))
            nums = re.findall(r'\d+', val)
            if nums:
                return int(nums[0])
    except Exception as e:
        print(f"  ⚠ Metafield error: {e}")
    return None

def download_image(url):
    r = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    return Image.open(io.BytesIO(r.content)).convert("RGBA")

def image_to_base64(img):
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=92)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")

def load_font(size):
    font_paths = [
        "C:/Windows/Fonts/arialbd.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf",
    ]
    for path in font_paths:
        try:
            return ImageFont.truetype(path, size)
        except:
            pass
    return ImageFont.load_default()


def resize_to_height(img, target_height):
    w, h = img.size
    ratio = target_height / h
    return img.resize((int(w * ratio), target_height), Image.LANCZOS)


def make_shadow_from_alpha(img, blur_radius=18, opacity=55):
    alpha = img.split()[-1]
    shadow_alpha = alpha.filter(ImageFilter.GaussianBlur(blur_radius))

    shadow = Image.new("RGBA", img.size, (0, 0, 0, 0))
    shadow.putalpha(shadow_alpha)

    px = shadow.load()
    w, h = shadow.size
    for y in range(h):
        for x in range(w):
            r, g, b, a = px[x, y]
            px[x, y] = (0, 0, 0, min(a, opacity))
    return shadow


def paste_clean(canvas, img, x, y):
    canvas.alpha_composite(img, (x, y))


def resize_to_height(img, target_height):
    w, h = img.size
    ratio = target_height / h
    return img.resize((int(w * ratio), target_height), Image.LANCZOS)


def make_shadow(img, blur_radius=16, opacity=90):
    alpha = img.split()[-1]
    shadow_mask = alpha.filter(ImageFilter.GaussianBlur(blur_radius))

    shadow = Image.new("RGBA", img.size, (0, 0, 0, 0))
    shadow.putalpha(shadow_mask)

    px = shadow.load()
    for y in range(shadow.size[1]):
        for x in range(shadow.size[0]):
            r, g, b, a = px[x, y]
            px[x, y] = (0, 0, 0, min(a, opacity))
    return shadow


def paste_with_shadow(canvas, img, x, y, shadow_dx=10, shadow_dy=14):
    shadow = make_shadow(img)
    canvas.alpha_composite(shadow, (x + shadow_dx, y + shadow_dy))
    canvas.alpha_composite(img, (x, y))


# ─── Image generation ─────────────────────────────────────

def get_fan_configs(pack_qty):
    """
    Clean fan layout.
    For large packs, show a designed representation instead of all units.
    """
    if pack_qty <= 1:
        visible = 1
    elif pack_qty <= 6:
        visible = pack_qty
    elif pack_qty <= 12:
        visible = 7
    else:
        visible = 9

    configs = []
    center = (visible - 1) / 2

    for i in range(visible):
        dist = i - center
        abs_dist = abs(dist)

        scale = max(0.70, 1.0 - abs_dist * 0.08)
        x_offset = int(dist * 120)
        y_shift = int(abs_dist * 18)
        angle = int(dist * 4)

        configs.append((x_offset, y_shift, scale, angle))

    return configs

def generate_composite(product_img, pack_qty):
    bg = (245, 245, 245, 255)
    canvas = Image.new("RGBA", CANVAS_SIZE, bg)
    cw, ch = CANVAS_SIZE

    # clean product cutout
    product = remove_white_background(product_img)
    product = trim_transparent(product)

    configs = get_fan_configs(pack_qty)

    # Bigger hero, cleaner composition
    hero_height = int(ch * 0.58)
    hero = resize_to_height(product, hero_height)

    back_height_base = int(hero_height * 0.82)

    anchor_x = cw // 2
    anchor_y = int(ch * 0.84)
    center_idx = len(configs) // 2

    prepared = []
    for idx, (ox, oy, scale, angle) in enumerate(configs):
        if idx == center_idx:
            item = hero.copy()
        else:
            target_h = int(back_height_base * scale)
            item = resize_to_height(product, target_h)

        if angle != 0:
            item = item.rotate(-angle, expand=True, resample=Image.BICUBIC)

        prepared.append((item, ox, oy))

    # draw from farthest to center
    draw_order = sorted(range(len(prepared)), key=lambda i: abs(i - center_idx), reverse=True)

    for i in draw_order:
        item, ox, oy = prepared[i]
        iw, ih = item.size
        x = anchor_x + ox - iw // 2
        y = anchor_y - ih + oy
        paste_clean(canvas, item, x, y)

    # clean orange badge only
    draw = ImageDraw.Draw(canvas)
    bx, by = int(cw * 0.80), int(ch * 0.72)
    br = 92

    # subtle badge shadow
    draw.ellipse((bx - br, by - br, bx + br, by + br), fill=(245, 136, 27, 255))

    font_num = load_font(64)
    font_pack = load_font(34)

    num_text = str(pack_qty)
    pack_text = "Pack"

    b1 = draw.textbbox((0, 0), num_text, font=font_num)
    b2 = draw.textbbox((0, 0), pack_text, font=font_pack)

    w1 = b1[2] - b1[0]
    h1 = b1[3] - b1[1]
    w2 = b2[2] - b2[0]
    h2 = b2[3] - b2[1]

    total_h = h1 + h2 - 8
    top_y = by - total_h // 2

    draw.text((bx - w1 // 2, top_y), num_text, font=font_num, fill=(255, 255, 255, 255))
    draw.text((bx - w2 // 2, top_y + h1 - 8), pack_text, font=font_pack, fill=(255, 255, 255, 255))

    # NO bottom text
    final = Image.new("RGB", CANVAS_SIZE, (245, 245, 245))
    final.paste(canvas, mask=canvas.split()[-1])
    return final


# ─── Upload helpers ───────────────────────────────────────

def upload_and_replace_first_image(product_id, handle, new_img, first_image_id):
    """Upload composite as new image, set position=1, delete old first image."""
    img_b64 = image_to_base64(new_img)
    payload = {
        "image": {
            "attachment": img_b64,
            "filename": f"{handle}_pack_composite.jpg",
            "position": 1
        }
    }
    result = shopify_post(f"products/{product_id}/images.json", payload)
    new_id = result["image"]["id"]

    if first_image_id and first_image_id != new_id:
        shopify_delete(f"products/{product_id}/images/{first_image_id}.json")
        print(f"  Deleted old first image (ID {first_image_id})")

    return new_id


# ─── Main ─────────────────────────────────────────────────

def run():
    os.makedirs(LOCAL_OUTPUT_DIR, exist_ok=True)

    print("=" * 60)
    print("  Shopify Pack Image Pipeline — TEST (2 products)")
    print(f"  Metafield : custom.case_size")
    print(f"  Source    : Image position 2 (single product photo)")
    print(f"  Replaces  : Image position 1 (pack shot)")
    print(f"  DRY RUN   : {DRY_RUN}")
    print("=" * 60)

    products = get_products(limit=TEST_LIMIT)
    print(f"\nFetched {len(products)} products\n")

    for i, product in enumerate(products):
        pid    = product["id"]
        handle = product["handle"]
        title  = product["title"]
        images = product.get("images", [])

        print(f"[{i+1}] {title[:70]}")
        print(f"  Handle: {handle} | ID: {pid}")
        print(f"  Total images on product: {len(images)}")

        # ── Pack size ──
        case_size = get_case_size(pid)
        if not case_size:
            print(f"  ⚠ No case_size metafield — defaulting to 6 for test")
            case_size = 6
        print(f"  Case size: {case_size}")

        # ── Identify source (2nd) and target (1st) images ──
        if len(images) < 2:
            print(f"  ⚠ Product only has {len(images)} image — need at least 2. Skipping.")
            print()
            continue

        images_sorted   = sorted(images, key=lambda img: img.get("position", 99))
        first_image_id  = images_sorted[0]["id"]
        first_image_url = images_sorted[0]["src"]
        second_image_url = images_sorted[1]["src"]

        print(f"  → 1st image (will be REPLACED): ...{first_image_url[-50:]}")
        print(f"  → 2nd image (SOURCE):           ...{second_image_url[-50:]}")

        # ── Download 2nd image ──
        try:
            source_img = download_image(second_image_url)
            print(f"  ✓ Downloaded source image: {source_img.size}")
        except Exception as e:
            print(f"  ✗ Failed to download 2nd image: {e}")
            print()
            continue

        # ── Generate composite ──
        try:
            composite = generate_composite(source_img, case_size)
            print(f"  ✓ Composite generated ({case_size}-pack fan layout)")
        except Exception as e:
            print(f"  ✗ Image generation failed: {e}")
            print()
            continue

        # ── Save local preview ──
        preview_path = os.path.join(LOCAL_OUTPUT_DIR, f"{handle}_composite.jpg")
        composite.save(preview_path, "JPEG", quality=92)
        print(f"  ✓ Preview saved → {preview_path}")

        # ── Upload (only when DRY_RUN = False) ──
        if not DRY_RUN:
            try:
                new_id = upload_and_replace_first_image(pid, handle, composite, first_image_id)
                print(f"  ✓ Uploaded! New image ID: {new_id}")
            except Exception as e:
                print(f"  ✗ Upload failed: {e}")
        else:
            print(f"  [DRY RUN] Not uploading — review preview image first")

        print()
        time.sleep(1)

    print("=" * 60)
    print(f"  Done! Check ./{LOCAL_OUTPUT_DIR}/ for previews.")
    if DRY_RUN:
        print("  Happy with the images? Set DRY_RUN = False and re-run.")
    print("=" * 60)

if __name__ == "__main__":
    run()
