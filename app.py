from flask import Flask, request, jsonify
import requests
import json
import time
import os
import re
from rapidfuzz import fuzz
from datetime import datetime, timedelta, timezone

app = Flask(__name__)

# =================== CONFIG ===================
SERVICETITAN_TENANT_ID = os.getenv("SERVICETITAN_TENANT_ID")
SERVICETITAN_APP_KEY = os.getenv("SERVICETITAN_APP_KEY")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
POLL_SECRET = os.getenv("POLL_SECRET", "my-secret-key")
TOKEN_FILE = "token_cache.json"

# =================== GLOBAL STATE ===================
token_data = {"access_token": None, "expires_at": 0}
materials_cache = {"data": [], "last_updated": 0, "cache_duration": 3600}

# =================== SYNONYMS ===================
SYNONYMS = {
    "flex": ["flex duct", "flexible duct"],
    "silvertape": ["foil tape", "silver tape"],
    "insulation wrap": ["duct wrap", "duct insulation"],
    "wrap": ["duct wrap"],
    "90": ["elbow", "90 degree", "90°"],
    "elbow": ["90", "elbow fitting"],
}

# =================== TOKEN MANAGEMENT ===================
def save_token_to_file():
    """Persist token data locally for reuse between runs."""
    try:
        with open(TOKEN_FILE, "w") as f:
            json.dump(token_data, f)
    except Exception as e:
        print(f"⚠️ Could not save token cache: {e}")

def load_token_from_file():
    """Load token from cache if still valid."""
    if os.path.exists(TOKEN_FILE):
        try:
            with open(TOKEN_FILE, "r") as f:
                data = json.load(f)
            if time.time() < data.get("expires_at", 0):
                token_data.update(data)
                print("✅ Loaded token from cache")
        except Exception as e:
            print(f"⚠️ Could not read cached token: {e}")

def fetch_new_token():
    """Fetch a new ServiceTitan OAuth2 token."""
    print("🔐 Fetching new ServiceTitan token...")
    url = "https://auth-integration.servicetitan.io/connect/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    try:
        response = requests.post(url, data=payload, headers=headers, timeout=15)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"❌ Token fetch failed: {e}")
        raise

    data = response.json()
    token_data["access_token"] = f"Bearer {data['access_token']}"
    token_data["expires_at"] = time.time() + data.get("expires_in", 900) - 30
    save_token_to_file()
    print("✅ Token refreshed successfully")

def get_token():
    """Return a valid ServiceTitan token, refreshing if expired."""
    if not token_data["access_token"] or time.time() > token_data["expires_at"]:
        fetch_new_token()
    return token_data["access_token"]

# =================== MATERIALS FETCH ===================
def fetch_materials_pricebook():
    """Fetch and cache materials list from ServiceTitan Pricebook."""
    now = time.time()
    if now - materials_cache["last_updated"] < materials_cache["cache_duration"]:
        return materials_cache["data"]

    print("🔄 Fetching materials from Pricebook...")
    url = f"https://api-integration.servicetitan.io/pricebook/v2/tenant/{SERVICETITAN_TENANT_ID}/materials"
    headers = {"Authorization": get_token(), "ST-App-Key": SERVICETITAN_APP_KEY}

    all_materials = []
    page = 1

    while True:
        params = {"page": page, "pageSize": 500}
        try:
            response = requests.get(url, headers=headers, params=params, timeout=20)
            if response.status_code == 401:
                fetch_new_token()
                headers["Authorization"] = get_token()
                response = requests.get(url, headers=headers, params=params, timeout=20)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"❌ Error fetching materials: {e}")
            break

        data = response.json()
        items = data.get("data", [])
        if not items:
            break

        for item in items:
            all_materials.append({
                "id": item.get("id"),
                "displayName": item.get("displayName"),
                "description": item.get("description"),
                "code": item.get("code"),
            })

        if not data.get("hasMore", False):
            break
        page += 1

    materials_cache.update({
        "data": all_materials,
        "last_updated": now
    })
    print(f"✅ Cached {len(all_materials)} materials")
    return all_materials

# =================== TEXT PROCESSING HELPERS ===================
def normalize_material_text(text):
    """Standardize and clean material text."""
    if not text:
        return ""
    text = text.lower()
    text = (text.replace("”", '"')
                 .replace("“", '"')
                 .replace("–", "-")
                 .replace("—", "-")
                 .replace("inch", "in")
                 .replace("in.", "in")
                 .replace('"', 'in')
                 .replace("feet", "ft")
                 .replace("ft.", "ft"))
    text = re.sub(r'\b(roll|bag|pcs?|each|ea|unit|piece|per)\b', '', text)
    return re.sub(r'\s+', ' ', text).strip()

def expand_synonyms(text):
    """Replace known synonyms with consistent base forms."""
    if not text:
        return text
    for key, vals in SYNONYMS.items():
        pattern = r'\b' + re.escape(key) + r'\b'
        text = re.sub(pattern, vals[0], text, flags=re.IGNORECASE)
    return text

def parse_materials_text(text):
    """Extract structured material entries (quantity + description)."""
    if not text:
        return []

    materials = []
    for line in text.strip().splitlines():
        line = line.strip()
        if not line:
            continue

        # Match formats like "1x item", "2 - item"
        m = re.match(r"^(\d+)\s*[-xX]\s*(.+)$", line)
        if m:
            materials.append({"quantity": int(m.group(1)), "description": m.group(2).strip()})
            continue

        # Match leading size/length like "10ft flex duct"
        m2 = re.match(r"^(\d+(?:\.\d+)?)(\s*(in|ft|\"|inch|inches|feet)\b)(.*)$", line, flags=re.IGNORECASE)
        if m2:
            desc = f"{m2.group(1)}{m2.group(2).strip()} {m2.group(4).strip()}".strip()
            materials.append({"quantity": 1, "description": desc})
            continue

        materials.append({"quantity": 1, "description": line})
    return materials

def extract_numbers_with_units(text):
    """Extract numeric values with their measurement units."""
    if not text:
        return []
    text = text.lower().replace('"', 'in')
    results = []
    for part in re.split(r'[,\s]+', text):
        m = re.match(r'(\d+(?:\.\d+)?)(in|ft)?', part)
        if m:
            val, unit = m.groups()
            unit = (unit or '').replace('inch', 'in').replace('feet', 'ft')
            results.append(f"{val}{unit}".strip())
    return results

# =================== MATCHING LOGIC ===================
def numeric_sequence_score(desc_nums, field_nums):
    """Give bonus score if numeric sequences are close."""
    if not desc_nums or not field_nums:
        return 0
    matches = 0
    for dn in desc_nums:
        dn_val = float(re.findall(r'\d+\.?\d*', dn)[0])
        for fn in field_nums:
            fn_val = float(re.findall(r'\d+\.?\d*', fn)[0])
            if abs(dn_val - fn_val) / max(dn_val, fn_val) < 0.15:
                matches += 1
                break
    return matches / max(len(desc_nums), len(field_nums))

def match_material(description, materials):
    """Find the best matching material from the pricebook."""
    desc_norm = normalize_material_text(expand_synonyms(description))
    desc_numbers = extract_numbers_with_units(desc_norm)
    keywords = ["flex", "elbow", "wrap", "tape", "wye"]

    filtered = [
        m for m in materials
        if any(k in (m.get("displayName", "") + m.get("description", "")).lower() for k in keywords)
        or any(n in (m.get("displayName", "") + m.get("description", "")).lower() for n in desc_numbers)
    ]

    best_match = None
    best_score = 0

    for m in filtered:
        for field in [m.get("displayName", ""), m.get("description", ""), m.get("code", "")]:
            field_norm = normalize_material_text(expand_synonyms(field))
            score = fuzz.partial_ratio(desc_norm, field_norm) / 100
            score += numeric_sequence_score(desc_numbers, extract_numbers_with_units(field_norm)) * 0.2
            score = min(score, 1.0)

            if score > best_score:
                best_score = score
                best_match = m

    if best_match and best_score >= 0.55:
        return best_match["id"], best_match["displayName"], best_score
    return None, None, 0

# =================== SERVICE TITAN API ===================
def get_invoice_id_from_job(job_id):
    """Retrieve invoice ID from a given job."""
    url = f"https://api-integration.servicetitan.io/jpm/v2/tenant/{SERVICETITAN_TENANT_ID}/jobs/{job_id}"
    headers = {"Authorization": get_token(), "ST-App-Key": SERVICETITAN_APP_KEY}
    response = requests.get(url, headers=headers)
    if response.status_code == 401:
        fetch_new_token()
        headers["Authorization"] = get_token()
        response = requests.get(url, headers=headers)
    if response.status_code == 200:
        job = response.json()
        invoices = job.get("invoices") or []
        if invoices:
            return invoices[0].get("id")
        return job.get("invoice", {}).get("id")
    print(f"❌ Could not get invoice for job {job_id}: {response.status_code}")
    return None

def add_materials_to_invoice(invoice_id, materials):
    """Add matched materials to a ServiceTitan invoice."""
    url = f"https://api-integration.servicetitan.io/sales/v2/tenant/{SERVICETITAN_TENANT_ID}/invoices/{invoice_id}"
    headers = {"Authorization": get_token(), "ST-App-Key": SERVICETITAN_APP_KEY, "Content-Type": "application/json"}
    payload = {
        "items": [{"skuId": m["skuId"], "quantity": m["quantity"], "description": m["description"]} for m in materials]
    }

    response = requests.patch(url, headers=headers, json=payload)
    if response.status_code == 401:
        fetch_new_token()
        headers["Authorization"] = get_token()
        response = requests.patch(url, headers=headers, json=payload)
    if 200 <= response.status_code < 300:
        print(f"✅ Added {len(materials)} materials to invoice {invoice_id}")
        return True
    print(f"❌ Failed to add materials: {response.status_code} {response.text}")
    return False

# =================== POLL ENDPOINT ===================
@app.route("/poll", methods=["GET", "POST"])
def poll_endpoint():
    """Trigger a sync: fetch last ServiceTitan form submission and process materials."""
    if request.args.get("secret") != POLL_SECRET:
        return jsonify({"error": "Unauthorized"}), 401

    print("🔍 Poll triggered (fetching last form submission)")

    try:
        url = f"https://api-integration.servicetitan.io/forms/v2/tenant/{SERVICETITAN_TENANT_ID}/submissions"
        headers = {"Authorization": get_token(), "ST-App-Key": SERVICETITAN_APP_KEY}
        params = {"page": 1, "pageSize": 1, "modifiedOnOrAfter": (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")}

        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 401:
            fetch_new_token()
            headers["Authorization"] = get_token()
            response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()

        forms = response.json().get("data", [])
        if not forms:
            print("📄 No forms found")
            return jsonify({"status": "success", "message": "No forms found"}), 200

        form = forms[0]
        form_id = form.get("id")
        job_id = next((o["id"] for o in form.get("owners", []) if o.get("type") == "Job"), None)
        print(f"\n➡️ Most recent Form ID: {form_id}\n   Linked Job ID: {job_id}")

        materials_text = next((u["value"] for u in form.get("units", []) if u.get("name") and "materials used" in u["name"].lower()), None)
        if not materials_text:
            print("⚠️ No 'materials used' field found")
            return jsonify({"status": "success", "message": "No materials field"}), 200

        print(f"   Materials Used:\n{materials_text}")

        materials_data = fetch_materials_pricebook()
        parsed = parse_materials_text(materials_text)

        invoice_items = []
        for item in parsed:
            sku_id, name, score = match_material(item["description"], materials_data)
            if sku_id:
                print(f"✅ Matched '{item['description']}' → {name} (score {score:.2f})")
                invoice_items.append({"skuId": sku_id, "quantity": item["quantity"], "description": name})
            else:
                print(f"⚠️ Could not match '{item['description']}'")

        if job_id and invoice_items:
            invoice_id = get_invoice_id_from_job(job_id)
            if invoice_id:
                add_materials_to_invoice(invoice_id, invoice_items)

        return jsonify({"status": "success", "form_id": form_id}), 200

    except Exception as e:
        print(f"❌ Polling failed: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

# =================== APP START ===================
if __name__ == "__main__":
    load_token_from_file()
    app.run(host="0.0.0.0", port=5000)
