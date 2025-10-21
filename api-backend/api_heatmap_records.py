"""
API service to fetch the latest heatmap-style quiz data for a specific user.
Supports multiple quizzes that use the 3Keys (heatmap) format.
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
from ultipa import Connection, UltipaConfig
import os
import json
import base64

# ================== CONFIGURATION ==================
HEATMAP_CATEGORIES = ["Origin", "Relevance", "Strategy", "Operations", "Function", "Feedback"]

def detect_environment():
    """Detect if running locally or in production"""
    if os.environ.get("KEN_ENV") == "production":
        return "production"
    return os.environ.get("FLASK_ENV", "development").lower()

# ================== FLASK APP ==================
app = Flask(__name__)
CORS(app)

# ================== ULTIPA CONNECTION ==================
def get_connection():
    """Establish connection to Ultipa"""
    ultipaConfig = UltipaConfig()
    ultipaConfig.hosts = [os.environ.get("ULTIPA_HOST", "114.242.60.100:10104")]
    ultipaConfig.username = os.environ.get("ULTIPA_USERNAME", "root")
    ultipaConfig.password = os.environ.get("ULTIPA_PASSWORD", "Zo1xy3SvSVFYKmapSpJ")
    ultipaConfig.defaultGraph = "test"
    ultipaConfig.heartBeat = 0
    return Connection.NewConnection(defaultConfig=ultipaConfig)

# ================== HELPER FUNCTION ==================
def decode_heatmap_data(encoded_str):
    """Decode base64 heatmap JSON and extract scores + quiz title"""
    try:
        decoded = base64.b64decode(encoded_str).decode("utf-8")
        data = json.loads(decoded)

        # Try to find quiz title from the structure
        quiz_title = None
        possible_title_keys = ["quizTitle", "quiz_title", "Quiz Title", "quiz_name", "title", "quiz"]

        for key in possible_title_keys:
            if key in data and aboutisinstance(data[key], str):
                quiz_title = data[key].strip()
                break

        # Find scores matching 6-category heatmap
        scores = None
        if all(k in data for k in HEATMAP_CATEGORIES):
            scores = {k: data[k] for k in HEATMAP_CATEGORIES}
        elif "scores" in data and isinstance(data["scores"], dict):
            if all(k in data["scores"] for k in HEATMAP_CATEGORIES):
                scores = data["scores"]

        if scores:
            return {"quiz_title": quiz_title, "scores": scores}
        return None
    except Exception:
        return None


# ================== FETCH LOGIC ==================
def fetch_heatmap_records_by_email(email):
    """Retrieve all heatmap-type quiz records for a user"""
    conn = get_connection()

    # Step 1: Get visitor_id
    find_visitor_uql = f"find().nodes({{@visitor.Email=='{email.lower()}'}}) as v return v{{visitor_id}} limit 1"
    visitor_res = conn.uql(find_visitor_uql).toDict()

    visitor_nodes = visitor_res.get('items', {}).get('v', {}).get('entities', [])
    if not visitor_nodes:
        raise ValueError(f"No visitor found for email: {email}")

    visitor_id = visitor_nodes[0]['values']['visitor_id']

    # Step 2: Get all records of that visitor
    uql_find_records = f"""
    find().nodes({{@record.visitor_id == '{visitor_id}'}}) as r
    return r{{record_id, quiz_id, server_time, parsed_data_encoded}}
    """
    record_res = conn.uql(uql_find_records).toDict()
    record_nodes = record_res.get('items', {}).get('r', {}).get('entities', [])
    if not record_nodes:
        raise ValueError("No records found for this visitor.")

    # Step 3: Filter only heatmap-style records
    heatmap_records = []
    for node in record_nodes:
        vals = node['values']
        encoded = vals.get('parsed_data_encoded', '')
        decoded_data = decode_heatmap_data(encoded)
        decoded = decode_heatmap_data(encoded)
        if decoded:
            heatmap_records.append({
                "record_id": vals.get("record_id"),
                "quiz_id": vals.get("quiz_id"),
                "quiz_title": decoded.get("quiz_title"),
                "server_time": vals.get("server_time"),
                "scores": decoded.get("scores")
            })


    if not heatmap_records:
        raise ValueError("No heatmap records found for this visitor.")

    return heatmap_records

# ================== API ENDPOINT ==================
@app.route("/api/heatmap-records", methods=["POST"])
def get_heatmap_records():
    """POST endpoint: expects { "email": "user@example.com" }"""
    if not request.is_json:
        return jsonify({"error": "Expected JSON body"}), 400
    data = request.get_json()
    email = data.get("email")

    if not email:
        return jsonify({"error": "Missing email"}), 400

    try:
        records = fetch_heatmap_records_by_email(email)
        return jsonify({
            "status": "success",
            "email": email,
            "total_records": len(records),
            "records": records
        })
    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 404
    except Exception as e:
        print("❌ Internal error:", e)
        return jsonify({"status": "error", "message": "Internal server error"}), 500

@app.route("/log", methods=["POST"])
def log_debug():
    try:
        data = request.get_json(force=True)
        print("🪵 [FRONTEND LOG]:", json.dumps(data, indent=2))
        return jsonify({"status": "logged"}), 200
    except Exception as e:
        print("⚠️ [LOG ERROR]:", e)
        return jsonify({"error": str(e)}), 500


# ================== SERVICE STARTUP ==================
if __name__ == "__main__":
    env = detect_environment()
    PORT = 5006
    print(f"🌍 Environment: {env.upper()}")
    print(f"🚀 Heatmap API running at http://localhost:{PORT}/api/heatmap-records")
    app.run(host="0.0.0.0", port=PORT, debug=True)
