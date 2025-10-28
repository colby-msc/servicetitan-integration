from flask import Flask, request, jsonify
import requests
import json
import time
import os
from difflib import SequenceMatcher
from datetime import datetime, timedelta, timezone

app = Flask(__name__)

# =================== CONFIG ===================
SERVICETITAN_TENANT_ID = os.getenv("SERVICETITAN_TENANT_ID")
SERVICETITAN_APP_KEY = os.getenv("SERVICETITAN_APP_KEY")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
POLL_SECRET = os.getenv("POLL_SECRET", "my-secret-key")

TOKEN_FILE = "token_cache.json"
PROCESSED_FORMS_FILE = "processed_forms.json"

# =================== GLOBAL STATE ===================
token_data = {"access_token": None, "expires_at": 0}
materials_cache = {"data": [], "last_updated": 0, "cache_duration": 3600}
processed_forms = set()

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
            else:
                print("⚠️ Cached token expired")
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

# =================== PROCESSED FORMS ===================
def load_processed_forms():
    global processed_forms
    if os.path.exists(PROCESSED_FORMS_FILE):
        try:
            with open(PROCESSED_FORMS_FILE, "r") as f:
                processed_forms = set(json.load(f))
            print(f"📋 Loaded {len(processed_forms)} processed forms")
        except Exception as e:
            print(f"⚠️ Could not load processed forms: {e}")

def save_processed_forms():
    try:
        with open(PROCESSED_FORMS_FILE, "w") as f:
            json.dump(list(processed_forms), f)
    except Exception as e:
        print(f"⚠️ Could not save processed forms: {e}")

# =================== MATERIALS ===================
MAX_MATERIALS_TEST = 500  # limit materials fetched for testing

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

        # ✅ STOP EARLY FOR TESTING
        if len(all_materials) >= MAX_MATERIALS_TEST:
            break

        if not data.get("hasMore", False):
            break
        page += 1

    materials_cache["data"] = all_materials
    materials_cache["last_updated"] = time.time()
    print(f"✅ Cached {len(all_materials)} materials")
    return all_materials

# =================== MATCHING HELPERS ===================
def similarity(a, b):
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def match_material(description, materials):
    desc = description.lower().strip()
    best_id, best_name, best_score = None, None, 0
    for m in materials:
        for field in [m.get("displayName", ""), m.get("description", ""), m.get("code", "")]:
            if not field:
                continue
            score = similarity(desc, field)
            if score > best_score:
                best_score, best_id, best_name = score, m["id"], m.get("displayName", "")
    return (best_id, best_name, best_score) if best_score > 0.6 else (None, None, 0)

def parse_materials_text(text):
    materials = []
    for line in text.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        for part in line.split(","):
            part = part.strip()
            if not part:
                continue
            pieces = part.split(None, 1)
            if len(pieces) == 2 and pieces[0].isdigit():
                materials.append({"quantity": int(pieces[0]), "description": pieces[1]})
            else:
                materials.append({"quantity": 1, "description": part})
    return materials

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

# =================== FORMS POLLING ===================
MAX_FORMS_TEST = 5  # process at most 5 forms per polling cycle

def poll_forms(debug=False):
    print("🔍 Checking for new form submissions...")
    url = f"https://api-integration.servicetitan.io/forms/v2/tenant/{SERVICETITAN_TENANT_ID}/submissions"
    headers = {"Authorization": get_token(), "ST-App-Key": SERVICETITAN_APP_KEY}

    lookback = timedelta(hours=1) if debug else timedelta(minutes=10)
    modified_since = (datetime.now(timezone.utc) - lookback).strftime("%Y-%m-%dT%H:%M:%SZ")

    params = {"page": 1, "pageSize": 10, "modifiedOnOrAfter": modified_since}  # ✅ limit forms
    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 401:
        fetch_new_token()
        headers["Authorization"] = get_token()
        response = requests.get(url, headers=headers, params=params)

    if response.status_code != 200:
        print(f"❌ Failed to fetch forms: {response.status_code}")
        return []

    forms = []
    data = response.json().get("data", [])
    print(f"📄 Fetched {len(data)} forms from API")

    for s in data:
        form_id = s.get("id")
        print(f"\n➡️ Form ID: {form_id}")
        for unit in s.get("units", []):
            print(f"   Unit: {unit.get('name')} = {unit.get('value')}")
        job_id = next((o.get("id") for o in s.get("owners", []) if o.get("type") == "Job"), None)
        print(f"   Linked Job ID: {job_id}")

        if not job_id or form_id in processed_forms:
            continue

        materials_text = next(
            (u.get("value") for u in s.get("units", []) if u.get("name") and "materials used" in u.get("name").lower()),
            None
        )

        if materials_text and materials_text.strip():
            forms.append({"form_id": form_id, "job_id": job_id, "materials_text": materials_text})
        else:
            print("⚠️ No 'materials used' field found")

        if len(forms) >= MAX_FORMS_TEST:  # ✅ limit forms processed
            break

    return forms

# =================== POLLING CYCLE ===================
def run_polling_cycle(debug=False):
    materials_data = fetch_materials_pricebook()
    forms_to_process = poll_forms(debug=debug)

    for f in forms_to_process:
        parsed_materials = parse_materials_text(f["materials_text"])
        matched_materials = []

        for m in parsed_materials:
            sku_id, name, score = match_material(m["description"], materials_data)
            if sku_id:
                matched_materials.append({"skuId": sku_id, "quantity": m["quantity"], "description": m["description"]})
            else:
                print(f"⚠️ Could not match '{m['description']}'")

        invoice_id = get_invoice_id_from_job(f["job_id"])
        if invoice_id:
            add_materials_to_invoice(invoice_id, matched_materials)
        processed_forms.add(f["form_id"])

    save_processed_forms()

# =================== FLASK ENDPOINT ===================
@app.route("/poll", methods=["GET", "POST"])
def poll_endpoint():
    secret = request.args.get("secret")
    if secret != POLL_SECRET:
        return jsonify({"error": "Unauthorized"}), 401

    print("🔍 Poll triggered (testing last form only)")

    try:
        # =================== FETCH MOST RECENT FORM ===================
        url = f"https://api-integration.servicetitan.io/forms/v2/tenant/{SERVICETITAN_TENANT_ID}/submissions"
        headers = {"Authorization": get_token(), "ST-App-Key": SERVICETITAN_APP_KEY}
        params = {"page": 1, "pageSize": 1, "modifiedOnOrAfter": (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")}  # 30-day lookback for testing

        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 401:
            fetch_new_token()
            headers["Authorization"] = get_token()
            response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            print(f"❌ Failed to fetch forms: {response.status_code}")
            return jsonify({"status": "error", "message": "Failed to fetch forms"}), 500

        data = response.json().get("data", [])
        if not data:
            print("📄 No forms found")
            return jsonify({"status": "success", "message": "No forms found"}), 200

        # Take the most recent form only
        form = data[0]
        form_id = form.get("id")
        print(f"\n➡️ Most recent Form ID: {form_id}")

        job_id = next((o.get("id") for o in form.get("owners", []) if o.get("type") == "Job"), None)
        print(f"   Linked Job ID: {job_id}")

        materials_text = next(
            (u.get("value") for u in form.get("units", []) if u.get("name") and "materials used" in u.get("name").lower()),
            None
        )

        if materials_text and materials_text.strip():
            print(f"   Materials Used:\n{materials_text}")
            # Parse materials but don't add to invoice for now
            parsed_materials = parse_materials_text(materials_text)
            for m in parsed_materials:
                print(f"   Parsed Material: {m['quantity']} x {m['description']}")
        else:
            print("⚠️ No 'materials used' field found")

        return jsonify({"status": "success", "form_id": form_id}), 200

    except Exception as e:
        print(f"❌ Polling failed: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


# =================== APP START ===================
if __name__ == "__main__":
    load_token_from_file()
    load_processed_forms()
    app.run(host="0.0.0.0", port=5000)
