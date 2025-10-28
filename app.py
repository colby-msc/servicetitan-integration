from flask import Flask, request, jsonify
import requests
import json
import time
import os
from difflib import SequenceMatcher
import threading

app = Flask(__name__)

# CONFIG
SERVICETITAN_TENANT_ID = os.getenv("SERVICETITAN_TENANT_ID")
SERVICETITAN_APP_KEY = os.getenv("SERVICETITAN_APP_KEY")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
TOKEN_FILE = "token_cache.json"
PROCESSED_FORMS_FILE = "processed_forms.json"
POLL_INTERVAL = 120  # Poll every 2 minutes

# GLOBAL STATE
token_data = {"access_token": None, "expires_at": 0}
materials_cache = {"data": [], "last_updated": 0, "cache_duration": 3600}
processed_forms = set()


# ========== TOKEN MANAGEMENT ==========
def save_token_to_file():
    try:
        with open(TOKEN_FILE, "w") as f:
            json.dump(token_data, f)
        print("💾 Token cached to file.")
    except Exception as e:
        print("⚠️ Could not save token:", e)

def load_token_from_file():
    if not os.path.exists(TOKEN_FILE):
        return
    try:
        with open(TOKEN_FILE, "r") as f:
            data = json.load(f)
        if time.time() < data.get("expires_at", 0):
            token_data.update(data)
            print("✅ Loaded valid token from cache.")
        else:
            print("⚠️ Cached token expired.")
    except Exception as e:
        print("⚠️ Could not read cached token:", e)

def fetch_new_token():
    url = "https://auth-integration.servicetitan.io/connect/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    print("🔐 Fetching new ServiceTitan token...")
    response = requests.post(url, data=payload, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        token_data["access_token"] = f"Bearer {data['access_token']}"
        token_data["expires_at"] = time.time() + data.get("expires_in", 900) - 30
        save_token_to_file()
        print("✅ New token acquired.")
    else:
        print(f"❌ Token fetch failed: {response.status_code}")
        raise Exception("Failed to get ServiceTitan token")

def get_token():
    if not token_data["access_token"] or time.time() > token_data["expires_at"]:
        fetch_new_token()
    return token_data["access_token"]


# ========== PROCESSED FORMS TRACKING ==========
def load_processed_forms():
    global processed_forms
    if os.path.exists(PROCESSED_FORMS_FILE):
        try:
            with open(PROCESSED_FORMS_FILE, "r") as f:
                processed_forms = set(json.load(f))
            print(f"📋 Loaded {len(processed_forms)} processed forms")
        except Exception as e:
            print("⚠️ Could not load processed forms:", e)

def save_processed_forms():
    try:
        with open(PROCESSED_FORMS_FILE, "w") as f:
            json.dump(list(processed_forms), f)
    except Exception as e:
        print("⚠️ Could not save processed forms:", e)


# ========== MATERIALS PRICEBOOK ==========
def fetch_materials_pricebook():
    """Fetch and cache all materials from pricebook."""
    if time.time() - materials_cache["last_updated"] < materials_cache["cache_duration"]:
        print("✅ Using cached materials pricebook")
        return materials_cache["data"]
    
    print("🔄 Fetching materials pricebook...")
    url = f"https://api-integration.servicetitan.io/pricebook/v2/tenant/{SERVICETITAN_TENANT_ID}/materials"
    headers = {
        "Authorization": get_token(),
        "ST-App-Key": SERVICETITAN_APP_KEY
    }
    
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
            print(f"❌ Failed to fetch materials: {response.status_code}")
            break
        
        data = response.json()
        materials = data.get("data", [])
        if not materials:
            break
        
        all_materials.extend(materials)
        print(f"📦 Page {page}: {len(materials)} materials")
        
        if not data.get("hasMore", False):
            break
        page += 1
    
    materials_cache["data"] = all_materials
    materials_cache["last_updated"] = time.time()
    print(f"✅ Cached {len(all_materials)} materials")
    return all_materials


# ========== FUZZY MATCHING ==========
def similarity_score(a, b):
    """Calculate similarity between two strings (0-1)."""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def match_material_to_sku(description, materials_list):
    """
    Match material description to SKU from pricebook.
    Returns (material_id, matched_name, confidence_score)
    """
    best_match = None
    best_score = 0
    best_name = None
    
    desc_clean = description.lower().strip()
    
    for material in materials_list:
        fields_to_check = [
            material.get("displayName", ""),
            material.get("description", ""),
            material.get("code", "")
        ]
        
        for field in fields_to_check:
            if not field:
                continue
            
            score = similarity_score(desc_clean, field)
            if score > best_score:
                best_score = score
                best_match = material["id"]
                best_name = material.get("displayName", "")
    
    # Only return matches with >60% confidence
    if best_score > 0.6:
        return best_match, best_name, best_score
    
    return None, None, 0


def parse_materials_from_text(materials_text):
    """
    Parse materials from free-text input.
    Format: "quantity description" per line
    Returns [{"quantity": int, "description": str}]
    """
    materials = []
    lines = materials_text.strip().split("\n")
    
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#") or line.lower().startswith("material"):
            continue
        
        parts = line.split(None, 1)  # Split on first whitespace
        
        if len(parts) == 2:
            qty_str, desc = parts
            try:
                qty_str = qty_str.rstrip("-")  # Handle "2-" format
                quantity = int(qty_str)
                materials.append({"quantity": quantity, "description": desc})
            except ValueError:
                # First part isn't a number, assume quantity 1
                materials.append({"quantity": 1, "description": line})
        else:
            # No quantity found, assume 1
            materials.append({"quantity": 1, "description": line})
    
    return materials


# ========== INVOICE OPERATIONS ==========
def add_materials_to_invoice(invoice_id, materials_to_add):
    """
    Add materials to invoice using PATCH endpoint.
    materials_to_add: [{"skuId": id, "quantity": qty, "description": desc}]
    """
    url = f"https://api-integration.servicetitan.io/sales/v2/tenant/{SERVICETITAN_TENANT_ID}/invoices/{invoice_id}"
    headers = {
        "Content-Type": "application/json",
        "Authorization": get_token(),
        "ST-App-Key": SERVICETITAN_APP_KEY
    }
    
    items = []
    for material in materials_to_add:
        items.append({
            "skuId": material["skuId"],
            "quantity": material["quantity"],
            "description": material["description"]
        })
    
    payload = {"items": items}
    
    print(f"📤 Adding {len(items)} materials to invoice {invoice_id}")
    response = requests.patch(url, headers=headers, json=payload)
    
    if response.status_code == 401:
        fetch_new_token()
        headers["Authorization"] = get_token()
        response = requests.patch(url, headers=headers, json=payload)
    
    if 200 <= response.status_code < 300:
        print(f"✅ Materials added to invoice {invoice_id}")
        return True
    else:
        print(f"❌ Failed to add materials: {response.status_code} - {response.text}")
        return False


# ========== JOB & INVOICE OPERATIONS ==========
def get_invoice_id_from_job(job_id):
    """Get the invoice ID associated with a job."""
    url = f"https://api-integration.servicetitan.io/jpm/v2/tenant/{SERVICETITAN_TENANT_ID}/jobs/{job_id}"
    headers = {
        "Authorization": get_token(),
        "ST-App-Key": SERVICETITAN_APP_KEY
    }
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 401:
        fetch_new_token()
        headers["Authorization"] = get_token()
        response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        job_data = response.json()
        invoice_id = job_data.get("invoice", {}).get("id")
        return invoice_id
    else:
        print(f"❌ Failed to get job {job_id}: {response.status_code}")
        return None


# ========== FORM POLLING ==========
def poll_forms():
    """
    Poll for new form submissions from ServiceTitan.
    
    Fetches recent form submissions and filters for those with materials.
    Returns list of forms to process.
    """
    print("🔍 Polling for form submissions...")
    
    url = f"https://api-integration.servicetitan.io/forms/v2/tenant/{SERVICETITAN_TENANT_ID}/submissions"
    headers = {
        "Authorization": get_token(),
        "ST-App-Key": SERVICETITAN_APP_KEY
    }
    
    # Get submissions from last 10 minutes to catch new ones
    from datetime import datetime, timedelta, timezone
    ten_min_ago = (datetime.now(timezone.utc) - timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%SZ")
    
    params = {
        "page": 1,
        "pageSize": 50,
        "modifiedOnOrAfter": ten_min_ago
    }
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 401:
        fetch_new_token()
        headers["Authorization"] = get_token()
        response = requests.get(url, headers=headers, params=params)
    
    if response.status_code != 200:
        print(f"❌ Failed to fetch submissions: {response.status_code}")
        return []
    
    data = response.json()
    submissions = data.get("data", [])
    print(f"📋 Found {len(submissions)} recent submissions")
    
    new_forms = []
    
    for submission in submissions:
        submission_id = submission.get("id")
        job_id = submission.get("jobId")
        
        # Skip if already processed
        if submission_id in processed_forms:
            continue
        
        # Look for materials field in the submission
        materials_text = None
        for field in submission.get("fields", []):
            field_name = field.get("name", "").lower()
            # Check if this is the materials field
            if "material" in field_name and "used" in field_name:
                materials_text = field.get("value", "")
                break
        
        # Only process if materials were provided
        if materials_text and materials_text.strip() and job_id:
            new_forms.append({
                "form_id": submission_id,
                "job_id": job_id,
                "materials_text": materials_text
            })
            print(f"✅ Found form {submission_id} with materials for job {job_id}")
    
    return new_forms


def process_form_submission(form_data):
    """Process a form submission and add materials to invoice."""
    form_id = form_data.get("form_id")
    invoice_id = form_data.get("invoice_id")
    materials_text = form_data.get("materials_text", "")
    
    if form_id in processed_forms:
        print(f"⏭️ Form {form_id} already processed")
        return
    
    print(f"\n📝 Processing form {form_id} for invoice {invoice_id}")
    print(f"📄 Materials text:\n{materials_text}\n")
    
    # Parse materials
    parsed_materials = parse_materials_from_text(materials_text)
    print(f"📋 Parsed {len(parsed_materials)} material entries")
    
    # Get pricebook
    pricebook = fetch_materials_pricebook()
    
    # Match materials to SKUs
    matched_materials = []
    unmatched_materials = []
    
    for material in parsed_materials:
        sku_id, matched_name, confidence = match_material_to_sku(
            material["description"], 
            pricebook
        )
        
        if sku_id:
            matched_materials.append({
                "skuId": sku_id,
                "quantity": material["quantity"],
                "description": material["description"]
            })
            print(f"✅ '{material['description']}' → '{matched_name}' ({confidence:.1%})")
        else:
            unmatched_materials.append(material)
            print(f"❌ No match for '{material['description']}'")
    
    # Add matched materials to invoice
    if matched_materials:
        success = add_materials_to_invoice(invoice_id, matched_materials)
        if success:
            processed_forms.add(form_id)
            save_processed_forms()
            print(f"✅ Form {form_id} processed successfully\n")
        else:
            print(f"⚠️ Failed to process form {form_id}\n")
    else:
        print(f"⚠️ No materials could be matched for form {form_id}\n")
    
    if unmatched_materials:
        print(f"⚠️ {len(unmatched_materials)} unmatched materials - review needed")


def polling_loop():
    """Background thread polling for forms every POLL_INTERVAL seconds."""
    print(f"🔄 Starting polling loop (every {POLL_INTERVAL}s)")
    
    while True:
        try:
            new_forms = poll_forms()
            for form in new_forms:
                process_form_submission(form)
        except Exception as e:
            print(f"🔥 Polling error: {e}")
        
        time.sleep(POLL_INTERVAL)


# ========== ROUTES ==========
@app.route("/", methods=["GET"])
def home():
    return jsonify({
        "status": "running",
        "service": "ServiceTitan Form → Invoice Bridge",
        "endpoints": {
            "/test-matching": "POST - Test material matching",
            "/manual-process": "POST - Manually process a form"
        }
    })


@app.route("/test-matching", methods=["POST"])
def test_material_matching():
    """Test endpoint to preview how materials would be matched."""
    try:
        data = request.get_json()
        materials_text = data.get("materials_text", "")
        
        if not materials_text:
            return jsonify({"error": "materials_text required"}), 400
        
        parsed = parse_materials_from_text(materials_text)
        pricebook = fetch_materials_pricebook()
        
        results = []
        for material in parsed:
            sku_id, matched_name, confidence = match_material_to_sku(
                material["description"], 
                pricebook
            )
            results.append({
                "input": material["description"],
                "quantity": material["quantity"],
                "matched_sku_id": sku_id,
                "matched_name": matched_name,
                "confidence": f"{confidence:.1%}" if confidence else "No match"
            })
        
        return jsonify({
            "total_materials": len(parsed),
            "results": results
        }), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/manual-process", methods=["POST"])
def manual_process():
    """Manually trigger processing of a form (for testing)."""
    try:
        data = request.get_json()
        required = ["form_id", "invoice_id", "materials_text"]
        
        if not all(k in data for k in required):
            return jsonify({"error": f"Required: {required}"}), 400
        
        process_form_submission(data)
        
        return jsonify({
            "success": True,
            "message": f"Processed form {data['form_id']}"
        }), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/stats", methods=["GET"])
def stats():
    """Get processing statistics."""
    return jsonify({
        "processed_forms_count": len(processed_forms),
        "cached_materials_count": len(materials_cache["data"]),
        "cache_age_seconds": int(time.time() - materials_cache["last_updated"]),
        "poll_interval_seconds": POLL_INTERVAL
    })


# ========== MAIN ==========
if __name__ == "__main__":
    print("🚀 Starting ServiceTitan Form → Invoice Bridge")
    
    # Load cached data
    load_token_from_file()
    load_processed_forms()
    
    # Get initial token
    if not token_data["access_token"]:
        fetch_new_token()
    
    # Start background polling
    polling_thread = threading.Thread(target=polling_loop, daemon=True)
    polling_thread.start()
    
    print("✅ Ready to process forms\n")
    app.run(host="0.0.0.0", port=5000)