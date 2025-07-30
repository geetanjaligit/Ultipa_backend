from flask import Flask, request, jsonify
import pandas as pd
import os
import json
import re
import base64
from datetime import datetime
from ultipa import Connection, UltipaConfig
from flask_cors import CORS  


# ================== Environment detection ==================
def detect_environment():
    """Environment detection logic"""
    if os.environ.get("KEN_ENV") == "production":
        return "production"
    return os.environ.get("FLASK_ENV", "development").lower()

# Create Flask app
app = Flask(__name__)
CORS(app)  # Allow cross-domain requests for all domains

def preprocess_parsed_data(parsed_data):
    """
    The ultimate JSON parser, handles the following situations:
    1. Multiple nested json fields
    2. Multi-layer escape characters
    3. Non-standard JSON format (such as missing quotes)
    """

    def deep_unpack(data, depth=0):
        # Prevent infinite recursion
        if depth > 8:
            return data

        # If it is a dictionary type, the json field will be unpacked first
        if isinstance(data, dict):
            if 'json' in data:
                unpacked = deep_unpack(data['json'], depth + 1)
                return deep_unpack(unpacked, depth + 1)
            return data

        # If it is a string type, try to continue parsing
        elif isinstance(data, str):
            # Cleanup steps (processing such multi-layer escapes)
            cleaned = data
            for _ in range(4):
                cleaned = (
                    cleaned.strip('"')
                    .replace(r'\\\"', '"')
                    .replace(r'\\"', '"')
                    .replace(r'\"', '"')
                    .replace(r"\\\'", "'")
                    .replace(r"\'", "'")
                    .replace(r'\\\\/', '/')
                    .replace(r'\\/', '/')
                    .replace('NaN', 'null')
                    .replace('\\n', '')
                )

            # Handle nested JSON objects in JSON strings
            try:
                # If you encounter nested json fields, you need to parse the json field first
                if '"json":' in cleaned:
                    match = re.search(r'"json":\s*"({.*?})"', cleaned)
                    if match:
                        # Extract and parse nested JSON strings
                        nested_json = match.group(1)
                        cleaned = cleaned.replace(match.group(0), f'"json": {nested_json}')
            except Exception as e:
                print(f"Nested json parsing failed: {e}")

            # First parsing attempt
            try:
                parsed = json.loads(cleaned)
                return deep_unpack(parsed, depth + 1)
            except json.JSONDecodeError:
                # Second attempt: Fix common format issues
                try:
                    # Fixed illegal formats like {key: value}
                    repaired = re.sub(
                        r'([{,])(\s*)([^":\s]+)(\s*):',
                        lambda m: f'{m.group(1)}{m.group(2)}"{m.group(3)}":',
                        cleaned
                    )
                    # Fix unquoted string values
                    repaired = re.sub(
                        r':\s*([^"\d{][^,}\n]*)',
                        lambda m: f': "{m.group(1).strip()}"' if not m.group(1).strip().startswith(
                            '"') else f': {m.group(1)}',
                        repaired
                    )
                    return deep_unpack(json.loads(repaired), depth + 1)
                except Exception as e:
                    # Last attempt: Violent analysis
                    try:
                        return json.loads(cleaned)
                    except:
                        return cleaned
        return data


    # Initial parsing attempt
    try:
        initial = json.loads(parsed_data)
    except:
        initial = parsed_data

    result = deep_unpack(initial)

    # Final type verification
    if isinstance(result, str):
        try:
            return json.loads(result)
        except:
            raise ValueError("Unable to parse JSON string:%s..." % result[:50])
    return result

def process_quiz_data(data_list):
    from collections import defaultdict
    result = defaultdict(lambda: defaultdict(list))  
    quiz_titles = {}

    for record in data_list:
        values = record.get('values', {})
        quiz_id = values.get('quiz_id', 'Unknown')
        timestamp = values.get('local_time', '')
        raw_data = values.get('parsed_data')
        if not raw_data:
            continue

        try:
            parsed = preprocess_parsed_data(raw_data)

            current = parsed
            depth = 0
            while 'quiz' not in current and 'json' in current and depth < 5:
                current = current['json'] if isinstance(current, dict) else current
                depth += 1

            if 'quiz' not in current:
                continue

            quiz_title = current['quiz'].get('title', '').lower()
            quiz_titles[quiz_id] = quiz_title 
            if "on boarding" in quiz_title:
                continue

            for response in current.get('responses', []):
                questions = response.get('questions', {})
                for qid, qdata in questions.items():
                    result[quiz_id][qid].append({
                        "x": timestamp,
                        "y": qdata.get('score', 0)
                    })

        except Exception as e:
            print(f"Error processing record: {e}")
            continue

    # Format as list of {quiz_id, question_id, data_points}
    formatted = []
    for quiz_id, questions in result.items():
        for qid, points in questions.items():
            formatted.append({
                "quiz_id": quiz_id,
                "quiz_title": quiz_titles.get(quiz_id, ""),
                "question_id": qid,
                "data": points
            })

    return formatted, None




def fetch_user_records(user_id):
    graph = "test"
    ultipaConfig = UltipaConfig()
    ultipaConfig.hosts = [os.environ.get("ULTIPA_HOST")]
    ultipaConfig.username = os.environ.get("ULTIPA_USERNAME")
    ultipaConfig.password = os.environ.get("ULTIPA_PASSWORD")
    ultipaConfig.defaultGraph = graph
    ultipaConfig.heartBeat = 0

    conn = Connection.NewConnection(defaultConfig=ultipaConfig)
    response = conn.uql("show().graph()")
    response.Print()
    # Find visitor_id via email
    response1 = conn.uql(
        f" find().nodes({{ @visitor.Email=='{user_id.lower()}' }}) as nodes return nodes{{*}} limit 1 ")
    # Check whether visitor_id is found
    nodes = response1.toDict().get('items', {}).get('nodes', {}).get('entities', [])
    if not nodes:
        raise ValueError(f"No data found for user_id: {user_id}")

    visitor_id = nodes[0]['values']['visitor_id']
    
    print(f"Querying visitor_id: {visitor_id} (type: {type(visitor_id)})")
    # Find all records through visitor_id and get parsed_data
    response2 = conn.uql(
        f" find().nodes({{@record.visitor_id =='{visitor_id}' }}) as nodes return nodes{{*}} limit 2000 ")
    records_raw = response2.toDict()['items']['nodes']['entities']
    print(f"Records found: {len(records_raw)}")
    # Decode base64 and prepare for process_quiz_data
    records = []
    for rec in records_raw:
        values = rec.get('values', {})
        quiz_id = values.get('quiz_id', 'Unknown')
        timestamp = values.get('local_time', '')
        raw_data_encoded = values.get('parsed_data_encoded', '')
        # Try base64 decode, if fails, use as plain JSON
        if raw_data_encoded:
            try:
                raw_data = base64.b64decode(raw_data_encoded).decode('utf-8')
            except Exception:
                raw_data = raw_data_encoded  # fallback: use as plain JSON
        else:
            raw_data = ''
        records.append({
            "values": {
                "quiz_id": quiz_id,
                "local_time": timestamp,
                "parsed_data": raw_data
            }
        })

    # Now process_quiz_data will work as expected
    processed, error = process_quiz_data(records)
    return processed, error

# API endpoints that handle POST requests
@app.route('/receive', methods=['POST'])
def api_handler():
    """
    Process the JSON request sent by the front-end and query the user's answer records based on user_id. 

    Request format:
    {
    "user_id": "user's email"
    }
    
    Return format:
    - When the query is successful:
    {
    "user_id": "user's email",
    "matched_records": [Matched record list],
    "total_records": Number of matched records
    }
    
    - On failure:
    {
    "error": "Error message"
    }
    """

    # Verify that the request format is JSON
    if not request.is_json:
        return jsonify({"error": "Only supports JSON format requests"}), 400

    data = request.get_json()
    user_id = data.get('user_id')

    # Check whether user_id is provided
    if not user_id:
        return jsonify({"error": "Missing required parameters: user_id"}), 400

    try:
        # Query user records
        records, error = fetch_user_records(user_id)

        # If the data cannot be found, a specific error will be thrown
        if not records:
            raise ValueError("No data found")

        # Return query results
        return jsonify({
            "user_id": user_id,
            "matched_records": records,
            "total_records": len(records)
        })

    except ValueError as e:
        # Processing errors that cannot query data
        print(f"ValueError: {e}") 
        return jsonify({"error": "No data found"}), 404  

    except Exception as e:
        # Handle other uncaught errors
        print(f"Exception: {e}")  # Print to the backend log to help troubleshoot
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500


# ================== Service startup logic ==================
if __name__ == '__main__':
    env = detect_environment()
    PORT = 5002  # Keep the original port

    print(f" Current running environment: {env.upper()}")

    if env == "production":
        print("\nThe use of Flask development server is prohibited in the production environment!")
        print(" Please start the production server with the following command:")
        print(f"    gunicorn -w 4 -b 0.0.0.0:{PORT} your_filename:app")  
        print("💡 Tip: Replace your_filename with the current Python file name (excluding extension)")
        exit(1)
    else:
        print(f"\n The development server has been started:http://localhost:{PORT}/receive")
        print(" Note: It is normal to see WARNING, which is the expected behavior of the development server.")
        app.run(host='0.0.0.0', port=PORT, debug=False)

"""
If it is a production environment, start in Terminal as follows:
# Step 1: Enter the project directory (key!)
cd /path/to/your/project

# Step 2: Set production environment identifier
export KEN_ENV=production

# Step 3: Add the current directory to the Python path (solve module import problems), and add the directory where the current terminal is located (the output of $(pwd)) to the Python module search path
export PYTHONPATH=$PYTHONPATH:$(pwd)

# Step 4: Start the production server
gunicorn -w 4 -b 0.0.0.0:5001 getMessage18-SaveLogByDay:app # Replace your_filename with the actual file name

# Press Ctrl+C when stopping service

#Development environment startup method (default)
python your_filename.py
"""
# illustrate:
# This code provides an API service that querys a specific user's questionnaire record through a POST request.
# The API endpoint is /receive, and the user needs to provide user_id (Email) to obtain its relevant records.
# The returned information includes:
# - user_id: The query user ID.
# - matched_records: All answer records of this user.
# - total_records: The number of records matched.
# If no data is found or an error occurs, an error message will be returned.

# {
# "user_id": "docwest007@yahoo.com"
# }
