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
    "flex": ["duct", "flexible duct"],
    "silvertape": ["foil tape", "silver tape"],
    "insulation wrap": ["duct insulation", "duct wrap"],
    "elbow": ["pipe elbow", "hard pipe elbow"]
}

# =================== TOKEN MANAGEMENT ===================
def save_token(token_info):
    try:
        with open(TOKEN_FILE, "w") as f:
            json.dump(token_info, f)
    except Exception as e:
        print(f"⚠️ Could not save token: {e}")

def load_token():
    if os.path.exists(TOKEN_FILE):
        try:
            with open(TOKEN_FILE, "r") as f:
                data = json.load(f)
            if time.time() < data.get("expires_at", 0):
                token_data.update(data)
                print("✅ Loaded token from cache")
        except Exception as e:
            print(f"⚠️ Could not read cached token: {e}")

def fetch_token():
    print("🔐 Fetching new ServiceTitan token...")
    url = "https://auth-integration.servicetitan.io/connect/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    resp = requests.post(url, data=payload, headers=headers)
    if resp.status_code == 200:
        data = resp.json()
        token_data["access_token"] = f"Bearer {data['access_token']}"
        token_data["expires_at"] = time.time() + data.get("expires_in", 900) - 30
        save_token(token_data)
        print("✅ Token refreshed")
    else:
        raise Exception(f"ServiceTitan token fetch failed: {resp.status_code} {resp.text}")

def get_token():
    if not token_data["access_token"] or time.time() > token_data["expires_at"]:
        fetch_token()
    return token_data["access_token"]

# =================== MATERIALS FETCH ===================
def fetch_materials():
    if time.time() - materials_cache["last_updated"] < materials_cache["cache_duration"]:
        return materials_cache["data"]

    print("🔄 Fetching materials from pricebook...")
    url = f"https://api-integration.servicetitan.io/pricebook/v2/tenant/{SERVICETITAN_TENANT_ID}/materials"
    headers = {"Authorization": get_token(), "ST-App-Key": SERVICETITAN_APP_KEY}
    all_materials, page = [], 1

    while True:
        params = {"page": page, "pageSize": 500}
        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code == 401:
            fetch_token()
            headers["Authorization"] = get_token()
            resp = requests.get(url, headers=headers, params=params)
        if resp.status_code != 200:
            print(f"❌ Error fetching materials: {resp.status_code}")
            break
        data = resp.json()
        items = data.get("data", [])
        if not items:
            break
        all_materials.extend(items)
        if not data.get("hasMore", False):
            break
        page += 1

    materials_cache.update({"data": all_materials, "last_updated": time.time()})
    print(f"✅ Cached {len(all_materials)} materials")
    return all_materials

# =================== TEXT PARSING ===================
def normalize_text(text):
    text = text.lower()
    text = text.replace("”", '"').replace("“", '"').replace("–", "-").replace("—", "-")
    text = text.replace("inch", "in").replace("in.", "in").replace('"', 'in')
    text = re.sub(r'\b(roll|bag|pcs?|each|ea|unit|piece|per|of)\b', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def expand_synonyms(text):
    for key, vals in SYNONYMS.items():
        for val in vals:
            text = re.sub(r'\b' + re.escape(key) + r'\b', val, text)
    return text

def parse_material_lines(text):
    materials = []
    for line in text.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        match = re.match(r"^(\d+)\s*[-xX]?\s*(.+)$", line)
        qty = int(match.group(1)) if match else 1
        desc = match.group(2).strip() if match else line
        materials.append({"quantity": qty, "description": desc})
    return materials

def extract_numbers(text):
    return re.findall(r'\d+\s*(in|ft)?', text.lower())

# =================== MATERIAL MATCHING ===================
def match_material(desc, materials):
    desc_exp = expand_synonyms(desc)
    desc_norm = normalize_text(desc_exp)
    desc_nums = extract_numbers(desc)

    best = {"id": None, "name": None, "score": 0}
    for m in materials:
        for field in [m.get("displayName", ""), m.get("description", ""), m.get("code", "")]:
            if not field:
                continue
            field_norm = normalize_text(expand_synonyms(field))
            score = max(fuzz.token_sort_ratio(desc_norm, field_norm),
                        fuzz.partial_ratio(desc_norm, field_norm)) / 100.0

            field_nums = extract_numbers(field)
            for dn in desc_nums:
                if dn in field_nums:
                    score += 0.25
            if desc_nums and not any(dn in field_nums for dn in desc_nums):
                score -= 0.1

            score = min(max(score, 0), 1.0)
            if score > best["score"]:
                best = {"id": m["id"], "name": m.get("displayName", ""), "score": score}

    return (best["id"], best["name"], best["score"]) if best["score"] > 0.6 else (None, None, 0)

# =================== SERVICE TITAN HELPERS ===================
def get_invoice_id(job_id):
    url = f"https://api-integration.servicetitan.io/jpm/v2/tenant/{SERVICETITAN_TENANT_ID}/jobs/{job_id}"
    headers = {"Authorization": get_token(), "ST-App-Key": SERVICETITAN_APP_KEY}
    resp = requests.get(url, headers=headers)
    if resp.status_code == 401:
        fetch_token()
        headers["Authorization"] = get_token()
        resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        job = resp.json()
        invoices = job.get("invoices", [])
        if invoices:
            return invoices[0].get("id")
        return job.get("invoice", {}).get("id")
    print(f"❌ Failed to get job {job_id}: {resp.status_code}")
    return None

def add_to_invoice(invoice_id, items):
    url = f"https://api-integration.servicetitan.io/sales/v2/tenant/{SERVICETITAN_TENANT_ID}/invoices/{invoice_id}"
    headers = {
        "Authorization": get_token(),
        "ST-App-Key": SERVICETITAN_APP_KEY,
        "Content-Type": "application/json"
    }
    payload = {"items": items}
    resp = requests.patch(url, headers=headers, json=payload)
    if resp.status_code == 401:
        fetch_token()
        headers["Authorization"] = get_token()
        resp = requests.patch(url, headers=headers, json=payload)
    if 200 <= resp.status_code < 300:
        print(f"✅ Added {len(items)} items to invoice {invoice_id}")
        return True
    print(f"❌ Failed to add materials: {resp.status_code} {resp.text}")
    return False

# =================== FLASK ENDPOINT ===================
@app.route("/poll", methods=["GET", "POST"])
def poll():
    secret = request.args.get("secret")
    if secret != POLL_SECRET:
        return jsonify({"error": "Unauthorized"}), 401

    print("🔍 Poll triggered")
    try:
        headers = {"Authorization": get_token(), "ST-App-Key": SERVICETITAN_APP_KEY}
        url = f"https://api-integration.servicetitan.io/forms/v2/tenant/{SERVICETITAN_TENANT_ID}/submissions"
        params = {
            "page": 1,
            "pageSize": 1,
            "modifiedOnOrAfter": (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
        }
        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code == 401:
            fetch_token()
            headers["Authorization"] = get_token()
            resp = requests.get(url, headers=headers, params=params)
        if resp.status_code != 200:
            return jsonify({"status": "error"}), 500

        forms = resp.json().get("data", [])
        if not forms:
            return jsonify({"status": "success", "message": "No forms found"}), 200

        form = forms[0]
        job_id = next((o.get("id") for o in form.get("owners", []) if o.get("type") == "Job"), None)

        materials_text = next(
            (u.get("value") for u in form.get("units", []) if u.get("name") and "materials used" in u.get("name").lower()),
            None
        )
        if not materials_text:
            return jsonify({"status": "success", "message": "No materials field"}), 200

        materials_data = fetch_materials()
        parsed_materials = parse_material_lines(materials_text)
        invoice_items = []
        for m in parsed_materials:
            sku_id, name, score = match_material(m["description"], materials_data)
            if sku_id:
                invoice_items.append({
                    "skuId": sku_id,
                    "quantity": m["quantity"],
                    "description": name
                })

        if job_id and invoice_items:
            invoice_id = get_invoice_id(job_id)
            if invoice_id:
                add_to_invoice(invoice_id, invoice_items)

        return jsonify({"status": "success", "form_id": form.get("id")}), 200

    except Exception as e:
        print(f"❌ Polling failed: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

# =================== APP START ===================
if __name__ == "__main__":
    load_token()
    app.run(host="0.0.0.0", port=5000)
