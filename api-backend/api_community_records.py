# API for Requirement R004: Query questionnaire records of a visitor's community (by email or community_id)
from flask import Flask, request, jsonify
import pandas as pd
import os
import json
from datetime import datetime
from ultipa import Connection, UltipaConfig
from flask_cors import CORS

def detect_environment():
    if os.environ.get("KEN_ENV") == "production":
        return "production"
    return os.environ.get("FLASK_ENV", "development").lower()

app = Flask(__name__)
CORS(app)

def preprocess_parsed_data(parsed_data):
    def deep_unpack(data, depth=0):
        if depth > 8:
            return data
        if isinstance(data, dict):
            if 'json' in data:
                unpacked = deep_unpack(data['json'], depth + 1)
                return deep_unpack(unpacked, depth + 1)
            return data
        elif isinstance(data, str):
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
            try:
                if '"json":' in cleaned:
                    import re
                    match = re.search(r'"json":\s*"({.*?})"', cleaned)
                    if match:
                        nested_json = match.group(1)
                        cleaned = cleaned.replace(match.group(0), f'"json": {nested_json}')
            except Exception as e:
                print(f"Nested json parsing failed: {e}")
            try:
                parsed = json.loads(cleaned)
                return deep_unpack(parsed, depth + 1)
            except json.JSONDecodeError:
                try:
                    import re
                    repaired = re.sub(
                        r'([{,])(\s*)([^":\s]+)(\s*):',
                        lambda m: f'{m.group(1)}{m.group(2)}"{m.group(3)}":',
                        cleaned
                    )
                    repaired = re.sub(
                        r':\s*([^"\d{][^,}\n]*)',
                        lambda m: f': "{m.group(1).strip()}"' if not m.group(1).strip().startswith(
                            '"') else f': {m.group(1)}',
                        repaired
                    )
                    return deep_unpack(json.loads(repaired), depth + 1)
                except Exception as e:
                    try:
                        return json.loads(cleaned)
                    except:
                        return cleaned
        return data

    try:
        initial = json.loads(parsed_data)
    except:
        initial = parsed_data

    result = deep_unpack(initial)
    if isinstance(result, str):
        try:
            return json.loads(result)
        except:
            raise ValueError("Cannot parse JSON string:%s..." % result[:50])
    return result

def process_quiz_data(data_list):
    from collections import defaultdict
    import pandas as pd

    # Collect all attempts per quiz
    attempts = defaultdict(list)  # {quiz_id: [ {visitor_id, timestamp, question_scores} ] }

    for record in data_list:
        values = record.get('values', {})
        quiz_id = values.get('quiz_id', 'Unknown')
        timestamp = values.get('local_time', '')
        visitor_id = values.get('visitor_id', 'Unknown')
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

            quiz_title = current['quiz'].get('title','').lower()
            if "on boarding" in quiz_title:
                continue

            skip = True
            for response in current.get('responses', []):
                questions = response.get('questions', {})
                if any(qdata.get('score', 0) != 0 for qdata in questions.values()):
                    skip = False
            if skip:
                continue

            for response in current.get('responses', []):
                questions = response.get('questions', {})
                question_scores = {qid: qdata.get('score', 0) for qid, qdata in questions.items()}
                attempts[quiz_id].append({
                    "visitor_id": visitor_id,
                    "timestamp": timestamp,
                    "question_scores": question_scores
                })

        except Exception as e:
            print(f"Error processing record: {e}")
            continue

    # Group attempts by date (community round)
    formatted = []
    for quiz_id, records in attempts.items():
        df = pd.DataFrame(records)
        if df.empty:
            continue
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['date'] = df['timestamp'].dt.date  # Only date part

        grouped = df.groupby('date')
        for date, group in grouped:
            question_scores = defaultdict(list)
            for _, row in group.iterrows():
                for qid, score in row['question_scores'].items():
                    question_scores[qid].append(score)
            for qid, scores in question_scores.items():
                avg = sum(scores) / len(scores) if scores else 0
                formatted.append({
                    "quiz_id": quiz_id,
                    "question_id": qid,
                    "data": [{
                        "x": str(date),
                        "y": avg
                    }]
                })

    return formatted, None

def fetch_community_records(community_id):
    graph = "test"
    ultipaConfig = UltipaConfig()
    ultipaConfig.hosts = [os.environ.get("ULTIPA_HOST")]
    ultipaConfig.username = os.environ.get("ULTIPA_USERNAME")
    ultipaConfig.password = os.environ.get("ULTIPA_PASSWORD")
    ultipaConfig.defaultGraph = graph
    ultipaConfig.heartBeat = 0

    conn = Connection.NewConnection(defaultConfig=ultipaConfig)

    # 1. Find all visitors belonging to the community
    uql_visitors = f'''
    find().nodes({{@community.community_id == "{community_id}"}}) as c
    find().edges({{@belongs_to_community}}) as e1
    find().nodes({{@visitor}}) as v
    where e1._from == v._id && e1._to == c._id
    return v{{*}}
    '''
    response = conn.uql(uql_visitors)
    print("DEBUG: UQL for visitors:\n", uql_visitors)
    print("DEBUG: UQL response (raw):", response.toDict())
    visitors = response.toDict().get('items', {}).get('v', {}).get('entities', [])
    print("DEBUG: Visitors found:", visitors)
    if not visitors:
        raise ValueError(f"No visitors found for community_id: {community_id}")

    visitor_ids = [v['values']['visitor_id'] for v in visitors if 'visitor_id' in v['values']]

    # 2. For each visitor, fetch all their records
    all_records = []
    for visitor_id in visitor_ids:
        uql_records = f'''
        find().nodes({{@record.visitor_id == "{visitor_id}"}}) as rec
        return rec{{*}}
        '''
        resp = conn.uql(uql_records)
        records = resp.toDict().get('items', {}).get('rec', {}).get('entities', [])
        print(f"DEBUG: Records for visitor {visitor_id}:", records)
        # Decode parsed_data_encoded for each record
        for r in records:
            values = r.get('values', {})
            raw_data_encoded = values.get('parsed_data_encoded', '')
            if raw_data_encoded:
                try:
                    import base64
                    raw_data = base64.b64decode(raw_data_encoded).decode('utf-8')
                except Exception:
                    raw_data = ''
            else:
                raw_data = values.get('parsed_data', '')
            values['parsed_data'] = raw_data
            r['values'] = values
        all_records.extend(records)

    print("DEBUG: Total records fetched for all visitors:", len(all_records))

    # 3. Process all records robustly
    records, error = process_quiz_data(all_records)
    print("DEBUG: Processed records:", records)
    return records, error

@app.route('/community_records', methods=['POST'])
def api_community_handler():
    if not request.is_json:
        return jsonify({"error": "Only supports JSON format requests"}), 400
    data = request.get_json()
    community_id = data.get('community_id')
    email = data.get('email')
    if not community_id and not email:
        return jsonify({"error": "Missing required parameters: community_id or email"}), 400

    print("DEBUG: community_id received:", community_id)
    # If community_id is not provided, but email is, look up community_id by email
    if not community_id and email:
        graph = "test"
        ultipaConfig = UltipaConfig()
        ultipaConfig.hosts = [os.environ.get("ULTIPA_HOST")]
        ultipaConfig.username = os.environ.get("ULTIPA_USERNAME")
        ultipaConfig.password = os.environ.get("ULTIPA_PASSWORD")
        ultipaConfig.defaultGraph = graph
        ultipaConfig.heartBeat = 0
        conn = Connection.NewConnection(defaultConfig=ultipaConfig)
        uql = f'''
        find().nodes({{@visitor.Email == "{email.lower()}"}}) as v
        find().edges({{@belongs_to_community}}) as e
        find().nodes({{@community}}) as c
        where e._from == v._id && e._to == c._id
        return c{{*}}
        ''' 
        resp = conn.uql(uql)
        community = resp.toDict().get('items', {}).get('c', {}).get('entities', [])
        if not community:
            return jsonify({"error": "No community found"}), 404
        community_id = community[0]['values']['community_id']
    try:
        records, question_averages = fetch_community_records(community_id)
        if not records:
            raise ValueError("No data found")
        return jsonify({
            "community_id": community_id,
            "matched_records": records,
            "total_records": len(records),
            "question_averages": question_averages
        })
    except ValueError as e:
        return jsonify({"error": "No data found"}), 404
    except Exception as e:
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500

def get_community_id_by_email(email):
    graph = "test"
    ultipaConfig = UltipaConfig()
    ultipaConfig.hosts = [os.environ.get("ULTIPA_HOST")]
    ultipaConfig.username = os.environ.get("ULTIPA_USERNAME")
    ultipaConfig.password = os.environ.get("ULTIPA_PASSWORD")
    ultipaConfig.defaultGraph = graph
    ultipaConfig.heartBeat = 0
    conn = Connection.NewConnection(defaultConfig=ultipaConfig)
    uql = f'''
    find().nodes({{@visitor.Email == "{email.lower()}"}}) as v
    find().edges({{@belongs_to_community}}) as e
    find().nodes({{@community}}) as c
    where e._from == v._id && e._to == c._id
    return c{{*}}
    '''
    resp = conn.uql(uql)
    communities = resp.toDict().get('items', {}).get('c', {}).get('entities', [])
    if not communities:
        return None
    return communities[0]['values']['community_id']

if __name__ == '__main__':
    env = detect_environment()
    PORT = 5004
    print(f"\U0001F4E1 Current environment: {env.upper()}")
    if env == "production":
        print("\n Flask development server is not allowed in production!")
        print("Use the following command to start the production server:")
        print(f"gunicorn -w 4 -b 0.0.0.0:{PORT} api_community_records:app")
        exit(1)
    else:
        print(f"\n Development server started: http://localhost:{PORT}/community_records")
        app.run(host='0.0.0.0', port=PORT, debug=False)
