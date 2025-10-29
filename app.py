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
    text = (text or "").lower()
    text = text.replace("”", '"').replace("“", '"').replace("–", "-").replace("—", "-")
    text = text.replace("inch", "in").replace("in.", "in").replace('"', 'in')
    text = text.replace("feet", "ft").replace("ft.", "ft")
    # remove some common noise words but keep numeric units like '15ft'
    text = re.sub(r'\b(roll|bag|pcs?|each|ea|unit|piece|per)\b', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def expand_synonyms(text):
    out = text
    for pattern, vals in SYNONYMS.items():
        try:
            # Escape the pattern so commas, quotes, etc. don’t break regex
            safe_pattern = re.escape(pattern)
            out = re.sub(safe_pattern, vals[0], out, flags=re.IGNORECASE)
        except re.error as e:
            print(f"⚠️ Regex error in pattern '{pattern}': {e}")
    return out


def parse_materials_text(text):
    """
    Improved parsing:
    - Recognize explicit counts like '1 x 17ft ...' -> qty=1, desc='17ft ...'
    - Recognize leading length/size like '15ft of ...' -> qty=1, desc='15ft ...'
    - Fallback -> qty=1, desc=line
    """
    materials = []
    if not text:
        return materials

    for raw_line in text.strip().splitlines():
        line = raw_line.strip()
        if not line:
            continue

        # 1) explicit count patterns: "1 x 17ft ...", "2- 6in elbow", "3 x something"
        m = re.match(r"^(\d+)\s*[-xX]\s*(.+)$", line)
        if m:
            qty = int(m.group(1))
            desc = m.group(2).strip()
            materials.append({"quantity": qty, "description": desc})
            continue

        # 2) leading size/length: "15ft of ...", "6in something", '6" flex'
        m2 = re.match(r"^(\d+(?:\.\d+)?)(\s*(in|ft|\"|inch|inches|feet)\b)(.*)$", line, flags=re.IGNORECASE)
        if m2:
            qty = 1
            size_token = (m2.group(1) + m2.group(2)).strip()
            remainder = m2.group(4).strip()
            if remainder:
                desc = f"{size_token} {remainder}"
            else:
                desc = size_token
            materials.append({"quantity": qty, "description": desc})
            continue

        # 3) fallback: single item, full line is description
        materials.append({"quantity": 1, "description": line})

    return materials

def extract_numbers_with_units(text):
    """
    Extracts normalized number+unit tokens from text:
    e.g. ["17ft", "6in", "10in", "90"]
    """
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
            # include plain numbers like 90 (useful for 90-degree elbows)
            results.append(num)
    return results

# =================== MATERIAL MATCHING ===================
def match_material(description, materials):
    """Improved matcher with numeric proximity and HVAC category weighting."""
    desc_expanded = expand_synonyms(description)
    desc_norm = normalize_material_text(desc_expanded)
    desc_numbers = extract_numbers_with_units(desc_norm)

    categories = {
        "flex": ["flex", "flexible", "duct"],
        "elbow": ["elbow", "90"],
        "wrap": ["wrap", "insulation"],
        "tape": ["tape", "foil"],
    }
    desc_tokens = set(desc_norm.split())
    desc_category = {cat for cat, keys in categories.items() if any(k in desc_tokens for k in keys)}

    def numeric_proximity_score(desc_nums, field_nums):
        """Returns closeness score between numeric values (e.g. 17 vs 25 = 0.68)."""
        score = 0
        for dn in desc_nums:
            try:
                dn_val = float("".join([c for c in dn if c.isdigit() or c == "."]))
            except:
                continue
            for fn in field_nums:
                try:
                    fn_val = float("".join([c for c in fn if c.isdigit() or c == "."]))
                    ratio = min(dn_val, fn_val) / max(dn_val, fn_val)
                    score = max(score, ratio)
                except:
                    continue
        return score * 0.2  # weight up to +0.2

    scored_matches = []

    for m in materials:
        name = m.get("displayName", "")
        code = m.get("code", "")
        desc = m.get("description", "")
        fields = [name, code, desc]

        best_field_score = 0
        for field in fields:
            if not field:
                continue
            field_expanded = expand_synonyms(field)
            field_norm = normalize_material_text(field_expanded)
            field_numbers = extract_numbers_with_units(field_norm)
            field_tokens = set(field_norm.split())

            # 1️⃣ Fuzzy text similarity
            fuzzy_score = max(
                fuzz.partial_ratio(desc_norm, field_norm),
                fuzz.token_sort_ratio(desc_norm, field_norm)
            ) / 100.0

            # 2️⃣ Numeric + proximity scoring
            numeric_matches = sum(dn == fn for dn in desc_numbers for fn in field_numbers)
            numeric_partial = sum(dn in fn or fn in dn for dn in desc_numbers for fn in field_numbers)
            proximity_score = numeric_proximity_score(desc_numbers, field_numbers)
            numeric_score = 0.10 * numeric_matches + 0.05 * numeric_partial + proximity_score

            # 3️⃣ Semantic weighting
            semantic_score = 0.0
            for cat, keys in categories.items():
                if cat in desc_category and any(k in field_tokens for k in keys):
                    semantic_score += 0.6  # stronger boost
                elif cat in desc_category and any(bad in field_tokens for bad in ["wire", "breaker", "motor", "circuit"]):
                    semantic_score -= 0.3

            total_score = 0.50 * fuzzy_score + 0.25 * semantic_score + 0.20 * numeric_score
            total_score = min(total_score, 1.0)

            best_field_score = max(best_field_score, total_score)

        if best_field_score > 0:
            scored_matches.append({
                "id": m["id"],
                "name": name,
                "score": best_field_score
            })

    scored_matches.sort(key=lambda x: x["score"], reverse=True)
    top_matches = scored_matches[:3]

    print(f"\n🔎 Debug matches for '{description}':")
    for i, t in enumerate(top_matches, start=1):
        print(f"   {i}. {t['name']} (score {t['score']:.2f})")

    best = top_matches[0] if top_matches else {"id": None, "name": None, "score": 0}
    return (best["id"], best["name"], best["score"]) if best["score"] >= 0.55 else (None, None, 0)



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
    
# ==================================================
@app.route('/test-matching', methods=['POST'])
def test_matching():
    try:
        # Parse incoming JSON body
        data = request.get_json()
        test_inputs = data.get("inputs", [])
        limit = int(request.args.get("limit", 500))  # limit pricebook size for safety

        if not test_inputs:
            return jsonify({"error": "No inputs provided"}), 400

        print(f"🔍 Starting test-matching with {len(test_inputs)} test items...")

        # Fetch materials and trim for performance
        all_materials = fetch_materials_pricebook()
        materials_data = all_materials[:limit] if limit < len(all_materials) else all_materials
        print(f"✅ Loaded {len(materials_data)} materials (limit={limit})")

        # Process inputs in small chunks (avoids memory spikes)
        results = []
        for i, raw_input in enumerate(test_inputs, start=1):
            print(f"\n🔎 Testing input #{i}: {raw_input}")

            top_matches = match_material(raw_input, materials_data, debug=True)
            if top_matches:
                best = top_matches[0]
                result = {
                    "input": raw_input,
                    "matched": best["name"],
                    "score": round(best["score"], 2),
                    "debug_top_matches": [
                        {"name": m["name"], "score": round(m["score"], 2)}
                        for m in top_matches[:3]
                    ]
                }
                print(f"✅ Matched '{raw_input}' → {best['name']} (score {best['score']:.2f})")
            else:
                result = {"input": raw_input, "matched": None, "score": 0}
                print(f"⚠️ No matches found for '{raw_input}'")

            results.append(result)

            # Stream progress logs for large batches
            if i % 10 == 0:
                print(f"🟡 Processed {i}/{len(test_inputs)} items so far...")

        print("✅ Test-matching completed successfully.")
        return jsonify({
            "total_inputs": len(test_inputs),
            "materials_in_pricebook": len(materials_data),
            "results": results
        })

    except Exception as e:
        print(f"❌ Error in /test-matching: {e}")
        return jsonify({"error": str(e)}), 500


# =================== APP START ===================
if __name__ == "__main__":
    load_token_from_file()
    app.run(host="0.0.0.0", port=5000)
