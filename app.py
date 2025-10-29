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
        # Store only essential fields to reduce memory
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

    materials_cache["data"] = all_materials
    materials_cache["last_updated"] = time.time()
    print(f"✅ Cached {len(all_materials)} materials (essential fields only)")
    return all_materials

# =================== TEXT PARSING HELPERS ===================
def normalize_material_text(text):
    text = (text or "").lower()
    text = text.replace("”", '"').replace("“", '"').replace("–", "-").replace("—", "-")
    text = text.replace("inch", "in").replace("in.", "in").replace('"', 'in')
    text = text.replace("feet", "ft").replace("ft.", "ft")
    text = re.sub(r'\b(roll|bag|pcs?|each|ea|unit|piece|per)\b', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def expand_synonyms(text):
    if not text:
        return text
    for key, vals in SYNONYMS.items():
        pattern = r'\b' + re.escape(key) + r'\b'
        text = re.sub(pattern, vals[0], text, flags=re.IGNORECASE)
    return text

def parse_materials_text(text):
    materials = []
    if not text:
        return materials
    for line in text.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        # explicit count
        m = re.match(r"^(\d+)\s*[-xX]\s*(.+)$", line)
        if m:
            qty = int(m.group(1))
            desc = m.group(2).strip()
            materials.append({"quantity": qty, "description": desc})
            continue
        # leading size/length
        m2 = re.match(r"^(\d+(?:\.\d+)?)(\s*(in|ft|\"|inch|inches|feet)\b)(.*)$", line, flags=re.IGNORECASE)
        if m2:
            size_token = (m2.group(1) + m2.group(2)).strip()
            remainder = m2.group(4).strip()
            desc = f"{size_token} {remainder}" if remainder else size_token
            materials.append({"quantity": 1, "description": desc})
            continue
        # fallback
        materials.append({"quantity": 1, "description": line})
    return materials

def extract_numbers_with_units(text):
    if not text:
        return []
    t = text.lower().replace('"', 'in')
    matches = re.findall(r'(\d+(?:\.\d+)?)(?:\s*(in|ft|inch|inches|feet))?', t)
    results = []
    for num, unit in matches:
        if unit:
            unit = unit.replace('inches', 'in').replace('inch', 'in').replace('feet', 'ft')
            results.append(f"{num}{unit}")
        else:
            results.append(num)
    return results

# =================== MATERIAL MATCHING ===================
def match_material(description, materials):
    desc_norm = normalize_material_text(expand_synonyms(description))
    desc_numbers = extract_numbers_with_units(desc_norm)
    keywords = ["flex", "elbow", "wrap", "tape"]

    # filter materials by numbers or keywords to reduce memory
    filtered = []
    for m in materials:
        text = " ".join(filter(None, [m.get("displayName"), m.get("description"), m.get("code")])).lower()
        if any(k in text for k in keywords) or any(n in text for n in desc_numbers):
            filtered.append(m)

    best_score = 0
    best_material = None
    for m in filtered:
        for field in [m.get("displayName") or "", m.get("description") or "", m.get("code") or ""]:
            field_norm = normalize_material_text(expand_synonyms(field))
            score = fuzz.partial_ratio(desc_norm, field_norm) / 100.0
            # numeric bonus
            field_numbers = extract_numbers_with_units(field_norm)
            numeric_bonus = sum(0.1 if dn == fn else 0.05 for dn in desc_numbers for fn in field_numbers if dn in fn or fn in dn)
            score = min(score + numeric_bonus, 1.0)
            if score > best_score:
                best_score = score
                best_material = m
    if best_score >= 0.55 and best_material:
        return best_material["id"], best_material["displayName"], best_score
    return None, None, 0

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
    headers = {"Authorization": get_token(), "ST-App-Key": SERVICETITAN_APP_KEY, "Content-Type": "application/json"}
    payload = {"items": [{"skuId": m["skuId"], "quantity": m["quantity"], "description": m["description"]} for m in materials]}
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
        params = {"page": 1, "pageSize": 1, "modifiedOnOrAfter": (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")}
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
        print(f"\n➡️ Most recent Form ID: {form_id}\n   Linked Job ID: {job_id}")

        materials_text = next((u.get("value") for u in form.get("units", []) if u.get("name") and "materials used" in u.get("name").lower()), None)
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
                invoice_items.append({"skuId": sku_id, "quantity": m["quantity"], "description": name})
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
