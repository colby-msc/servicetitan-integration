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
    try:
        with open(TOKEN_FILE, "w") as f:
            json.dump(token_data, f)
    except Exception as e:
        print(f"⚠️ Could not save token: {e}")

def load_token_from_file():
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
    print("🔐 Fetching new ServiceTitan token...")
    url = "https://auth-integration.servicetitan.io/connect/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    response = requests.post(url, data=payload, headers=headers)
    if response.status_code == 200:
        data = response.json()
        token_data["access_token"] = f"Bearer {data['access_token']}"
        token_data["expires_at"] = time.time() + data.get("expires_in", 900) - 30
        save_token_to_file()
        print("✅ Token refreshed")
    else:
        print(f"❌ Failed to fetch token: {response.status_code} {response.text}")
        raise Exception("ServiceTitan token fetch failed")

def get_token():
    if not token_data["access_token"] or time.time() > token_data["expires_at"]:
        fetch_new_token()
    return token_data["access_token"]

# =================== MATERIALS FETCH ===================
def fetch_materials_pricebook():
    if time.time() - materials_cache["last_updated"] < materials_cache["cache_duration"]:
        return materials_cache["data"]

    print("🔄 Fetching materials from pricebook...")
    url = f"https://api-integration.servicetitan.io/pricebook/v2/tenant/{SERVICETITAN_TENANT_ID}/materials"
    headers = {"Authorization": get_token(), "ST-App-Key": SERVICETITAN_APP_KEY}
    all_materials = []
    page = 1

    while True:
        params = {"page": page, "pageSize": 500}
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 401:
            fetch_new_token()
            headers["Authorization"] = get_token()
            response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            print(f"❌ Error fetching materials: {response.status_code}")
            break
        data = response.json()
        items = data.get("data", [])
        if not items:
            break
        all_materials.extend(items)
        if not data.get("hasMore", False):
            break
        page += 1

    materials_cache["data"] = all_materials
    materials_cache["last_updated"] = time.time()
    print(f"✅ Cached {len(all_materials)} materials")
    return all_materials

# =================== TEXT PARSING HELPERS ===================
def normalize_material_text(text):
    text = text.lower()
    text = text.replace("”", '"').replace("“", '"').replace("–", "-").replace("—", "-")
    text = text.replace("inch", "in").replace("in.", "in").replace('"', 'in')
    text = text.replace("feet", "ft").replace("ft.", "ft")
    text = re.sub(r'\b(roll|bag|pcs?|each|ea|unit|piece|per|of)\b', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def expand_synonyms(text):
    """Replace keywords with synonyms using regex for whole words."""
    for key, vals in SYNONYMS.items():
        pattern = r'\b' + re.escape(key) + r'\b'
        text = re.sub(pattern, vals[0], text)
    return text

def parse_materials_text(text):
    materials = []
    for raw_line in text.strip().splitlines():
        line = raw_line.strip()
        if not line:
            continue
        match = re.match(r"^(\d+)\s*[-xX]?\s*(.+)$", line)
        qty = int(match.group(1)) if match else 1
        desc = match.group(2).strip() if match else line
        materials.append({"quantity": qty, "description": desc})
    return materials

def extract_numbers_with_units(text):
    """Extract numeric-unit pairs like 6in, 25ft, 10in, 90, etc."""
    text = text.lower().replace('"', 'in')
    matches = re.findall(r'(\d+(?:\.\d+)?)\s*(in|ft)?', text)
    results = []
    for num, unit in matches:
        if unit:
            results.append(f"{num}{unit}")
        else:
            results.append(num)
    return results

# =================== MATERIAL MATCHING ===================
def match_material(description, materials):
    """Improved material matcher with weighted numeric + fuzzy logic."""
    desc_expanded = expand_synonyms(description)
    desc_norm = normalize_material_text(desc_expanded)
    desc_numbers = extract_numbers_with_units(desc_norm)

    best = {"id": None, "name": None, "score": 0}

    for m in materials:
        name = m.get("displayName", "")
        code = m.get("code", "")
        desc = m.get("description", "")
        fields = [name, code, desc]

        for field in fields:
            if not field:
                continue
            field_expanded = expand_synonyms(field)
            field_norm = normalize_material_text(field_expanded)
            field_numbers = extract_numbers_with_units(field_norm)

            fuzzy_score = max(
                fuzz.partial_ratio(desc_norm, field_norm),
                fuzz.token_sort_ratio(desc_norm, field_norm)
            ) / 100.0

            num_matches = sum(dn == fn for dn in desc_numbers for fn in field_numbers)
            if num_matches:
                fuzzy_score += 0.35 * num_matches
            elif desc_numbers:
                fuzzy_score -= 0.1

            for token in ["flex", "elbow", "wrap", "tape"]:
                if token in desc_norm and token in field_norm:
                    fuzzy_score += 0.15

            fuzzy_score = min(fuzzy_score, 1.0)

            if fuzzy_score > best["score"]:
                best = {
                    "id": m["id"],
                    "name": name,
                    "score": fuzzy_score
                }

    return (best["id"], best["name"], best["score"]) if best["score"] >= 0.65 else (None, None, 0)

# =================== SERVICE TITAN OPERATIONS ===================
def get_invoice_id_from_job(job_id):
    url = f"https://api-integration.servicetitan.io/jpm/v2/tenant/{SERVICETITAN_TENANT_ID}/jobs/{job_id}"
    headers = {"Authorization": get_token(), "ST-App-Key": SERVICETITAN_APP_KEY}
    response = requests.get(url, headers=headers)
    if response.status_code == 401:
        fetch_new_token()
        headers["Authorization"] = get_token()
        response = requests.get(url, headers=headers)
    if response.status_code == 200:
        job = response.json()
        invoices = job.get("invoices", [])
        if invoices:
            return invoices[0].get("id")
        return job.get("invoice", {}).get("id")
    print(f"❌ Failed to get job {job_id}: {response.status_code}")
    return None

def add_materials_to_invoice(invoice_id, materials):
    url = f"https://api-integration.servicetitan.io/sales/v2/tenant/{SERVICETITAN_TENANT_ID}/invoices/{invoice_id}"
    headers = {
        "Authorization": get_token(),
        "ST-App-Key": SERVICETITAN_APP_KEY,
        "Content-Type": "application/json"
    }
    payload = {"items": [
        {"skuId": m["skuId"], "quantity": m["quantity"], "description": m["description"]}
        for m in materials
    ]}
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

# =================== FLASK ENDPOINT ===================
@app.route("/poll", methods=["GET", "POST"])
def poll_endpoint():
    secret = request.args.get("secret")
    if secret != POLL_SECRET:
        return jsonify({"error": "Unauthorized"}), 401

    print("🔍 Poll triggered (fetching last form submission)")
    try:
        url = f"https://api-integration.servicetitan.io/forms/v2/tenant/{SERVICETITAN_TENANT_ID}/submissions"
        headers = {"Authorization": get_token(), "ST-App-Key": SERVICETITAN_APP_KEY}
        params = {
            "page": 1,
            "pageSize": 1,
            "modifiedOnOrAfter": (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
        }
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 401:
            fetch_new_token()
            headers["Authorization"] = get_token()
            response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            print(f"❌ Failed to fetch forms: {response.status_code}")
            return jsonify({"status": "error"}), 500

        data = response.json().get("data", [])
        if not data:
            print("📄 No forms found")
            return jsonify({"status": "success", "message": "No forms found"}), 200

        form = data[0]
        form_id = form.get("id")
        job_id = next((o.get("id") for o in form.get("owners", []) if o.get("type") == "Job"), None)
        print(f"\n➡️ Most recent Form ID: {form_id}")
        print(f"   Linked Job ID: {job_id}")

        materials_text = next(
            (u.get("value") for u in form.get("units", []) if u.get("name") and "materials used" in u.get("name").lower()),
            None
        )
        if not materials_text:
            print("⚠️ No 'materials used' field found")
            return jsonify({"status": "success", "message": "No materials field"}), 200

        print(f"   Materials Used:\n{materials_text}")

        materials_data = fetch_materials_pricebook()
        parsed_materials = parse_materials_text(materials_text)
        invoice_items = []
        for m in parsed_materials:
            sku_id, name, score = match_material(m["description"], materials_data)
            if sku_id:
                print(f"✅ Matched '{m['description']}' → {name} (score {score:.2f})")
                invoice_items.append({
                    "skuId": sku_id,
                    "quantity": m["quantity"],
                    "description": name
                })
            else:
                print(f"⚠️ Could not match '{m['description']}'")

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
