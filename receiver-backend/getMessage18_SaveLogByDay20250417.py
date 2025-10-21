"""
# This is the code that was successful on April 2 (production environment judgement has been added)
Integrated version of data processing service - simultaneously implement:
1. Receive POST request and record the original log
2. Extract structured data and save it to CSV

Request processing flow:
User POST request → Flask route matches to /receive → Trigger receive_data()
↓
Parsing request data (JSON/form/file, etc.)
↓
Verify data validity (including "json" fields is the key)
↓
Valid data → Call process_data() to structured processing → Write to CSV file
↓
Whether successful or not → Log the original request log to the JSON file
↓
Returns JSON response (successfully with CSV path, failure with error message)
R001
"""


from flask import Flask, request, jsonify, make_response
import json
import pandas as pd
import os
from datetime import datetime
import uuid  
from timezonefinder import TimezoneFinder
from ultipa import Connection, UltipaConfig
import pytz
import base64 
from urllib.parse import unquote_plus, parse_qs

# ================== Environment detection and startup logic ==================
def detect_environment():
    """Environment detection logic"""
    # Preferential reading of custom environment variables
    if os.environ.get("KEN_ENV") == "production": #First check whether the KEN_ENV environment variable exists and is equal to "production" 
        return "production"
    return os.environ.get("FLASK_ENV", "development").lower() # When FLASK_ENV is not set, the default return to development

# ==================Configuration section==================
# Read configuration from environment variables. When KEN_LOG_DIR does not exist, use the logs folder in the directory where the current script is located.
LOG_DIR = os.environ.get("KEN_LOG_DIR", os.path.join(os.path.dirname(__file__), "logs/logs"))
CSV_DIR = os.environ.get("KEN_DATA_DIR", os.path.join(os.path.dirname(__file__), "logs/data"))
PORT = 5001  #Service Port

HEATMAP_CATEGORIES = [
    "Origin", "Relevance", "Strategy", "Operations", "Function", "Feedback"
]

# ================== File Writer Base Class ==================
class DailyFileHandler:  
    """Split files by day processing base classes""" 

    def __init__(self, base_name, target_dir):
        self.current_day = self._get_current_day()  # Change variable name
        self.base_name = base_name
        self.target_dir = target_dir
        os.makedirs(self.target_dir, exist_ok=True)

    def _get_current_day(self):  # Method name modification
        """The unified time format is YYYYMMDD"""
        return datetime.now().strftime("%Y%m%d")  # Format string modification

    def _get_filename(self):
        """Generate full file path"""
        return os.path.join(self.target_dir, f"{self.base_name}_u{self.current_day}.csv")  # File name format modification

    def check_day(self):  # Method name modification
        """Check if you need to switch to a new file"""
        current = self._get_current_day()
        if current != self.current_day: # Each instance is maintained independently
            self.current_day = current
            return True
        return False

# ================== CSV Writer ==================
class DailyCsvWriter(DailyFileHandler):  
    """CSV writer divided by day"""  

    def __init__(self):
        super().__init__("survey_responses", CSV_DIR)
        self._init_file()

    def _init_file(self):
        """Initialize the CSV header"""
        if not os.path.exists(self._get_filename()):
            columns = [
                "request_id","timestamp", "timestamp_utc",  "Local Time", 
                "Quiz ID", "Community Name", "Team Name", "Email", "Visitor ID", "Device ID",  "Latitude", "Longitude",
                "Start Time", "End Time", "IP Address", "Browser",
                "Country", "City", "Region", "Total Score", "Percent",
                "Question ID", "Question Text", "Selected Answer",
                "Answer Option", "Option Score", "Question Score",
                "Page Time Taken (ms)", "Image URL", "Answer Options"
            ]
            pd.DataFrame(columns=columns).to_csv(self._get_filename(), index=False)

    def append_data(self, df):
        """Add data"""
        if self.check_day():
            self._init_file()
        df.to_csv(self._get_filename(), mode='a', header=False, index=False)

class DailyHeatmapCsvWriter(DailyFileHandler):
    def __init__(self):
        super().__init__("heatmap_responses", CSV_DIR)
        self._init_file()

    def _init_file(self):
        if not os.path.exists(self._get_filename()):
            columns = [
                "request_id", "timestamp_utc", "email", "visitor_id", "quiz_id", "quiz_title"
            ] + HEATMAP_CATEGORIES
            pd.DataFrame(columns=columns).to_csv(self._get_filename(), index=False)
    
    def append_data(self, data_dict):
        if self.check_day():
            self._init_file()
        df = pd.DataFrame([data_dict])
        df.to_csv(self._get_filename(), mode='a', header=False, index=False)

# ================== Log Writer ==================
class DailyLogWriter(DailyFileHandler):  
    """Log writer divided by day"""  

    def __init__(self):
        super().__init__("received_data", LOG_DIR)
        self.current_file = None

    def _get_filename(self):
        return os.path.join(self.target_dir, f"{self.base_name}_{self.current_day}.json")  # File name format modification

    def write_entry(self, entry):
        """Write to log entries"""
        if self.check_day() or not os.path.exists(self._get_filename()):  
            self._rotate_file()

        with open(self._get_filename(), "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    def _rotate_file(self):
        """Create a new log file (empty file)"""
        open(self._get_filename(), 'a').close()

# ================== Flask application initialization ==================
app = Flask(__name__)

# ================== Data processing functions (Wellbeing Quiz) ==================
def process_data(post_data, processing_time_server, processing_time_utc, request_id):
    """
    Extract structured information from raw POST data
    :param post_data: Dictionary that must contain the "json" key
    :param processing_time_server: server's local time zone timestamp (consistent with log)
    :param processing_time_utc: UTC+0 time zone time stamp
    :return: Structured DataFrame
    """
    try:
        # 1. Take out the URL encoded JSON string
        raw_json = post_data.get("json", "")
        # 2. Decoding: Restore %xx and +
        decoded_str = unquote_plus(raw_json)
        # 3. Parse JSON
        data = json.loads(decoded_str)

        quiz_info = data["quiz"]
        quiz_id = quiz_info.get("id", "unknown")  # Get the unique identification of the questionnaire
        questions = quiz_info["questions"]
        responses = data.get("responses", [])

        all_data = []
        tf = TimezoneFinder()  # Create a time zone finder instance

        for response in responses:
            # Extract basic information (using defensive programming)
            geo = response.get("geo", {})
            times = response.get("times", {})
            user = response.get("user", {})
            # Added local time calculation logic
            latitude = geo.get("lat", 0.0)
            longitude = geo.get("lng", 0.0)
            try:
                timezone_str = tf.timezone_at(lat=latitude, lng=longitude)
                tz = pytz.timezone(timezone_str) if timezone_str else pytz.utc
                local_time = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
            except Exception as e:
                local_time = "Time zone calculation error"
                app.logger.error(f"Time zone calculation failed:{str(e)}")

            # --- Extract Community Name from quiz title ---
            quiz_title = quiz_info.get("title", "")
            if "on boarding for" in quiz_title.lower():
                # e.g. "On Boarding for Resetme.goodbarber.app"
                after_for = quiz_title.split("for", 1)[-1].strip()
                # Take only the first word or the part before a dot/space if needed
                community_name = after_for.split(".")[0].split()[0]
            else:
                community_name = "unknown"
            
            team_qid = None
            for qid, qdata in questions.items():
                if qdata.get("question", "").lower().strip() in ["team name", "teamname", "team"]:
                    team_qid = qid
                    break
            questions_data = response.get("questions", {})
            team_name = questions_data.get(team_qid, {}).get("text", "") if team_qid else "unknown"

            email_list = response.get("contact", {}).get("emails", [])
            email = email_list[0] if email_list else "unknown"

            base_info = {
                "request_id": request_id,
                "timestamp": processing_time_server,  # Server local time stamp
                "timestamp_utc": processing_time_utc,  # UTC timestamp
                "Local Time": local_time,  
                "Quiz ID": quiz_id,
                "Community Name": community_name,
                "Team Name": team_name,
                "Email": email,  
                "Visitor ID": user.get("visitorID", "unknown"), 
                "Device ID": response.get("device_id", "unknown"),
                "Latitude": geo.get("lat", 0.0),
                "Longitude": geo.get("lng", 0.0),
                "Start Time": times.get("start", 0),
                "End Time": times.get("end", 0),
                "IP Address": response.get("ip", ["unknown"])[0],
                "Browser": response.get("browser", "unknown"),
                "Country": geo.get("country", ""),
                "City": geo.get("city", ""),
                "Region": geo.get("region", ""),
                "Total Score": response.get("score", 0),
                "Percent": response.get("percent", 0)
            }

            # Handle problem data
            # Preload the problem ID list (improve traversal performance)
            question_ids = list(questions.keys())

            # Iterate through the answers to each question
            for q_id in question_ids:
                q_data = questions[q_id]  # Problem metadata
                # Extract all options
                answer_options = json.dumps(
                    {ans["answer"]: ans["score"] for ans in q_data.get("answers", [])},
                    ensure_ascii=False
                )
                answer_data = response.get("questions", {}).get(q_id, {})
                question_index = question_ids.index(q_id)
                pages = times.get("pages", [])
                page_time_taken = pages[question_index] if question_index < len(pages) else 0

                # For multiple choice/checkbox questions
                if "answers" in answer_data and answer_data.get("answers") is not None:
                    if len(answer_data.get("answers", [])) > 0:
                        for ans_index in answer_data["answers"]:
                            ans_details = q_data["answers"][ans_index]
                            record = base_info.copy()
                            record.update({
                                "Question ID": q_id,
                                "Question Text": q_data["question"],
                                "Selected Answer": answer_data.get("text", ""),
                                "Answer Option": ans_details["answer"],
                                "Option Score": ans_details["score"],
                                "Question Score": answer_data.get("score", 0),
                                "Page Time Taken (ms)": page_time_taken,
                                "Image URL": ans_details.get("image", ""),
                                "Answer Options": answer_options
                            })
                            all_data.append(record)
                    else:
                        # No answers selected for this question
                        record = base_info.copy()
                        record.update({
                            "Question ID": q_id,
                            "Question Text": q_data["question"],
                            "Selected Answer": answer_data.get("text", "No answer"),
                            "Answer Option": "No answer",
                            "Option Score": 0,
                            "Question Score": answer_data.get("score", 0),
                            "Page Time Taken (ms)": 0,
                            "Image URL": "No answer",
                            "Answer Options": answer_options
                        })
                        all_data.append(record)
                else:
                    # For text questions (no 'answers' field)
                    record = base_info.copy()
                    record.update({
                        "Question ID": q_id,
                        "Question Text": q_data["question"],
                        "Selected Answer": answer_data.get("text", "No answer"),
                        "Answer Option": answer_data.get("text", "No answer"),
                        "Option Score": 0,
                        "Question Score": answer_data.get("score", 0),
                        "Page Time Taken (ms)": page_time_taken,
                        "Image URL": "",
                        "Answer Options": answer_options
                    })
                    all_data.append(record)

        return pd.DataFrame(all_data)

    except Exception as e:
        raise ValueError(f"Data processing failed: {str(e)}")


# ================== DATA PROCESSING LOGIC (Heatmap Quiz) ==================
def process_heatmap_data(post_data, processing_time_utc, request_id):
    # This function is correct and does not need to change
    try:
        # This function expects clean, parsed JSON data, which it will now receive
        data = post_data
        
        quiz_info = data.get("quiz", {})
        response = data.get("responses", [{}])[0]
        user_info = response.get("user", {})
        contact_info = response.get("contact", {})
        
        # Extract email from the correct place in the quiz-maker response
        email = contact_info.get("emails", ["unknown"])[0]

        base_record = {
            "request_id": request_id,
            "timestamp_utc": processing_time_utc,
            "email": email,
            "visitor_id": user_info.get("visitorID", "unknown"),
            "quiz_id": quiz_info.get("id", "unknown"),
            "quiz_title": quiz_info.get("title", "unknown")
        }

        category_scores = response.get("categories", {})
        json_category_keys = list(category_scores.keys())

        for i, client_category_name in enumerate(HEATMAP_CATEGORIES):
            if i < len(json_category_keys):
                json_key = json_category_keys[i]
                score = category_scores[json_key].get("score", 0)
                base_record[client_category_name] = score
            else:
                base_record[client_category_name] = 0
        
        return base_record
    except Exception as e:
        raise ValueError(f"Heatmap data processing failed: {str(e)}")


# ================== Flask routing processing (Wellbeing Quiz) ==================
@app.route('/receive5001', methods=['POST']) #When any HTTP POST request reaches http://server address:POST/receive, Flask will automatically parse the request data, call the receive_data() function to process the request and return the response.
def receive_data():
    """The main entrance to receive POST requests"""
    # Generate a unique request ID (UUID4 format)
    request_id = str(uuid.uuid4())
    # Initialize the writer (each requests independent instance to ensure thread safety)
    csv_writer = DailyCsvWriter()
    log_writer = DailyLogWriter()

    # Record the original data
    headers = dict(request.headers)
    raw_data = request.get_data(as_text=True)
    parsed_data = None
    error_msg = None
    # Generate double timestamps
    processing_time_server = datetime.now().isoformat()
    processing_time_utc = datetime.utcnow().isoformat()

    try:
        # Parsing data in different formats
        # First try to parse the URL-encoded format
        qs = parse_qs(raw_data, keep_blank_values=True)
        if 'json' in qs:
            parsed_data = {'json': qs['json'][0]}
        # Otherwise, parse according to Content-Type
        elif request.content_type == 'application/json':
            parsed_data = request.get_json() or {}
        elif request.content_type == 'application/x-www-form-urlencoded':
            parsed_data = request.form.to_dict()
        elif request.content_type.startswith('multipart/form-data'):
            parsed_data = {**request.form.to_dict(), 'files': {k: v.filename for k, v in request.files.items()}}
        else:
            parsed_data = {}

        # Structured data processing
        if parsed_data and "json" in parsed_data:  # Key data verification
            df = process_data(parsed_data, processing_time_server, processing_time_utc, request_id)  # Pass in time stamp
            csv_writer.append_data(df)
            print(f"{len(df)} data was successfully written to {csv_writer._get_filename()}")
            # --------- Start adding Ultipa database writing code ---------
            try:
                # ultipa configuration
                from ultipa import Connection, UltipaConfig  

                graph = "test"
                ultipaConfig = UltipaConfig()
                # Configure database server information

                ultipaConfig.hosts = [os.environ.get("ULTIPA_HOST")]
                ultipaConfig.username = os.environ.get("ULTIPA_USERNAME")
                ultipaConfig.password = os.environ.get("ULTIPA_PASSWORD")
                ultipaConfig.defaultGraph ="test"
                ultipaConfig.heartBeat = 0

                conn = Connection.NewConnection(defaultConfig=ultipaConfig)

                # Extract the required information from the structured data (assuming there is at least one record in the DataFrame)
                if not df.empty:
                    visitorID = str(df.iloc[0]["Visitor ID"])
                    Email = str(df.iloc[0]["Email"]).strip().lower()
                    quiz_id = str(df.iloc[0]["Quiz ID"])
                    local_time = str(df.iloc[0]["Local Time"])
                    team_name = str(df.iloc[0].get("Team Name", "unknown"))
                    community_name = str(df.iloc[0].get("Community Name", "unknown"))
                    team_id = team_name.replace(" ", "_").lower() if team_name != "unknown" else "unknown"
                    community_id = community_name.replace(" ", "_").lower() if community_name != "unknown" else "unknown"

                    # --- Fallback for missing team/community ---
                    if (team_id == "unknown" or community_id == "unknown" or Email== "unknown") and visitorID != "unknown":
                        result = conn.uql(f"""
                            fetch({{
                                v = node('{visitorID}');
                                c = v.out(@belongs_to_community);
                                t = v.out(@belongs_to_team);
                                return {{
                                    email: v.Email,
                                    community_id: c.community_id,
                                    community_name: c.community_name,
                                    team_id: t.team_id,
                                    team_name: t.team_name
                                }};
                            }})
                        """)
                        if result and isinstance(result, list) and result:
                            fallback = result[0]
                            if Email == "unknown":
                                Email = fallback.get("email", "unknown")
                            if community_id == "unknown":
                                community_id = fallback.get("community_id", "unknown")
                                community_name = fallback.get("community_name", "unknown")
                            if team_id == "unknown":
                                team_id = fallback.get("team_id", "unknown")
                                team_name = fallback.get("team_name", "unknown")
                else:
                    visitorID = "unknown"
                    Email = ""
                    quiz_id = "unknown"
                    local_time = "unknown"
                    team_name = "unknown"
                    community_name = "unknown"
                    team_id = "unknown"
                    community_id = "unknown"
                print("DEBUG visitorID:", visitorID)
                print("DEBUG team_id:", team_id)
                print("DEBUG community_id:", community_id)
                print("DEBUG request_id:", request_id)
                print("DEBUG Email:", Email)
                print("DEBUG quiz_id:", quiz_id)
                # ==================== ULTIPA UPSERT LOGIC ====================
                # 1. Upsert community
                if community_id and community_id.strip().lower() != "unknown":
                    conn.uql(f"""
                        upsert().into(@community).nodes({{
                            _id:'{community_id}', community_id:'{community_id}', community_name:'{community_name}'
                        }})
                    """)

                # 2. Upsert team
                if team_id and team_id.strip().lower() != "unknown":
                    conn.uql(f"""
                        upsert().into(@team).nodes({{
                            _id:'{team_id}', team_id:'{team_id}', team_name:'{team_name}'
                        }})
                    """)

                # 3. Upsert visitor node 
                # Only update email if it's not "unknown"
                if Email and Email != "unknown":
                    conn.uql(f"""
                        upsert().into(@visitor).nodes({{
                           _id:'{visitorID}', visitor_id:'{visitorID}', Email:'{Email}'
                        }})
                    """)
                else:
                    # Just upsert visitor without changing email
                    conn.uql(f"""
                        upsert().into(@visitor).nodes({{
                           _id:'{visitorID}', visitor_id:'{visitorID}'
                        }})
                    """)

                # 5. Upsert belongs_to_team edge
                if team_id and team_id.strip().lower() != "unknown" and visitorID and visitorID.strip().lower() != "unknown":
                    conn.uql(f"""
                        insert().into(@belongs_to_team).edges({{
                            _from:'{visitorID}', _to:'{team_id}', id:'{visitorID}_{team_id}'
                        }})
                    """)

                # 6. Upsert belongs_to_community edge
                if community_id and community_id.strip().lower() != "unknown" and visitorID and visitorID.strip().lower() != "unknown":
                    conn.uql(f"""
                        insert().into(@belongs_to_community).edges({{
                            _from:'{visitorID}', _to:'{community_id}', id:'{visitorID}_{community_id}'
                        }})
                    """)
                    

                # 7. Upsert quiz node
                conn.uql(f"""
                    upsert().into(@quiz).nodes({{
                        _id:'{quiz_id}', quiz_id:'{quiz_id}'
                    }})
                """)

                # 8. Insert record node (always)
                if visitorID and visitorID.strip().lower() != "unknown" and quiz_id and quiz_id.strip().lower() != "unknown":
                    parsed_data_json = json.dumps(parsed_data, ensure_ascii=False)
                    parsed_data_encoded = base64.b64encode(parsed_data_json.encode('utf-8')).decode('utf-8')
                    uql = f"""
                        insert().into(@record).nodes({{
                           _id:'{request_id}', record_id:'{request_id}', visitor_id:'{visitorID}', Email:'{Email}', quiz_id:'{quiz_id}', 
                           team_id:'{team_id}', community_id:'{community_id}', 
                           server_time:'{processing_time_server}', local_time:'{local_time}',
                           parsed_data_encoded:'{parsed_data_encoded}'
                        }})
                    """
                    print("UQL for record:", uql)
                    conn.uql(uql) 

                # 9. Insert has edges to link visitor and quiz to record
                conn.uql(f"insert().into(@has).edges({{ _from:'{visitorID}', _to:'{request_id}', id:'{request_id}_visitor' }})")
                conn.uql(f"insert().into(@has).edges({{ _from:'{quiz_id}', _to:'{request_id}', id:'{request_id}_quiz' }})")

            except Exception as e:
                app.logger.error(f"Ultipa DB insert error: {e}")
            # --------- Ultipa database writing code ends ---------
        else:
            error_msg = "Missing required data fields"

    except Exception as e:
        error_msg = f"Handling errors: {str(e)}"
        app.logger.error(error_msg)

    print("\n Received a POST request:")
    print(f" Headers: {json.dumps(headers, indent=4, ensure_ascii=False)}")
    print(f" Raw data length: {len(raw_data)} bytes")
    print(f" The parsed data (parsed_data): {json.dumps(parsed_data, indent=4, ensure_ascii=False) if parsed_data else 'None'}")
    print("parsed_data of type:",type(parsed_data))

    if error_msg:
        print(f" error message: {error_msg}")

    # Build log entries
    log_entry = {
        "request_id": request_id,
        "timestamp": processing_time_server,  # Use the same timestamp datetime.now().isoformat()
        "headers": headers,
        "data_summary": {
            "raw_data_length": len(raw_data),
            "raw_data": raw_data,
            "parsed_data": parsed_data,
            "parsed_fields": list(parsed_data.keys()) if parsed_data else None
        },
        "error": error_msg,
        "related_csv": csv_writer._get_filename() if not error_msg else None
    }

    # Logs are recorded only when valid data is received (can be adjusted according to requirements)
    if not error_msg or len(raw_data) > 0:
        log_writer.write_entry(log_entry)
        print(f" Logged to {log_writer._get_filename()}")

    # Return response
    if error_msg:
        if error_msg:
            return jsonify({
                "status": "error",
                "request_id": request_id,  
                "message": error_msg
            }), 400
    return jsonify({
        "status": "success",
        "request_id": request_id,  
        "csv_file": csv_writer._get_filename()
    })


# ================== API ENDPOINT (Heatmap Quiz) ==================
@app.route('/receive-heatmap', methods=['POST'])
def receive_heatmap_data():
    request_id = str(uuid.uuid4())
    heatmap_csv_writer = DailyHeatmapCsvWriter()
    log_writer = DailyLogWriter()
    processing_time_utc = datetime.utcnow().isoformat()
    raw_post_body = request.get_data(as_text=True) # Get the entire raw body as text
    error_msg = None
    parsed_data = None

    try:
        # Step 1: Handle the URL-encoded format, just like the old endpoint
        qs = parse_qs(raw_post_body, keep_blank_values=True)
        
        # Step 2: Check if the 'json' parameter exists
        if 'json' not in qs:
            raise ValueError("Required 'json' field missing in URL-encoded payload.")
        
        # Step 3: Decode and parse the JSON string from the 'json' parameter
        json_string = qs['json'][0]
        decoded_string = unquote_plus(json_string)
        parsed_data = json.loads(decoded_string) # This is the clean dictionary
        
        if not parsed_data:
            raise ValueError("Parsed JSON data is empty.")
        
        # Step 4: Call the processing function with the clean data
        heatmap_record = process_heatmap_data(parsed_data, processing_time_utc, request_id)
        
        # Step 5: Append the single row of data to the new CSV
        heatmap_csv_writer.append_data(heatmap_record)
        print(f"📊 Successfully wrote 1 heatmap row to {heatmap_csv_writer._get_filename()}")

        try:
            # Re-establish connection config
            from ultipa import Connection, UltipaConfig
            graph = "test"
            ultipaConfig = UltipaConfig()
            ultipaConfig.hosts = [os.environ.get("ULTIPA_HOST","114.242.60.100:10104")]
            ultipaConfig.username = os.environ.get("ULTIPA_USERNAME","root")
            ultipaConfig.password = os.environ.get("ULTIPA_PASSWORD","Zo1xy3SvSVFYKmapSpJ")
            ultipaConfig.defaultGraph = graph
            ultipaConfig.heartBeat = 0
            conn = Connection.NewConnection(defaultConfig=ultipaConfig)

            # 🧩 Debugging setup
            print("✅ Connected to Ultipa host:", ultipaConfig.hosts)
            print("🧭 Using graph:", ultipaConfig.defaultGraph)
            
            # Enable verbose SDK logs (this will print UQL requests/responses)
            try:
                conn.setLogLevel("DEBUG")
                print("🔍 SDK debug logging enabled.")
            except Exception as log_err:
                print("⚠️ Could not enable SDK debug logs:", log_err)
            
            # Test connection
            try:
                test_result = conn.uql("list().graphs()")
                print("🔗 Connection test successful. Available graphs:", test_result.toDict())
            except Exception as test_err:
                print("⚠️ Connection test failed:", test_err)


            # 1. Extract key info. We absolutely need a visitorID to proceed.
            visitorID = str(heatmap_record.get("visitor_id", "unknown"))
            if not visitorID or visitorID == "unknown":
                raise ValueError("Cannot process record without a valid visitorID from the payload.")
            
            Email = str(heatmap_record.get("email", "unknown")).strip().lower()
            quiz_id = str(heatmap_record.get("quiz_id", "unknown"))
            server_time_utc = heatmap_record.get("timestamp_utc")
            
            # 2. Check if the visitor already exists in the database.
            check_visitor_uql = f"find().nodes({{@visitor.visitor_id == '{visitorID}'}}) as v return v"
            visitor_exists_result = conn.uql(check_visitor_uql)
            
            # If the visitor does NOT exist, create them, using the visitorID as the primary _id.
            if not visitor_exists_result.toDict().get('items'):
                print(f"Visitor {visitorID} not found. Creating a new visitor node.")
                conn.uql(f"insert().into(@visitor).nodes({{ _id:'{visitorID}', visitor_id:'{visitorID}', Email:'{Email}' }})")
            
            # 3. Upsert the quiz node.
            conn.uql(f"upsert().into(@quiz).nodes({{ _id:'{quiz_id}', quiz_id:'{quiz_id}' }})")
            
            # 4. Prepare the category scores for storage.
            category_scores_data = {cat: heatmap_record.get(cat, 0) for cat in HEATMAP_CATEGORIES}
            scores_json_string = json.dumps(category_scores_data, ensure_ascii=False)
            scores_encoded = base64.b64encode(scores_json_string.encode('utf-8')).decode('utf-8')
            
            # 5. Insert the new @record node.
            if quiz_id and quiz_id != "unknown":
                # Fetch the real email from the database to ensure the record is accurate.
                get_email_uql = f"find().nodes({{@visitor.visitor_id == '{visitorID}'}}) as v return v.Email"
                email_result = conn.uql(get_email_uql)
                # Safely extract the email from the nested response
                db_email_list = email_result.toDict().get('items', {}).get('v', [])
                db_email = db_email_list[0][0] if db_email_list and db_email_list[0] else "unknown"
            
                uql_query=f"""insert().into(@record).nodes({{
                       _id:'{request_id}', record_id:'{request_id}', visitor_id:'{visitorID}', 
                       Email:'{db_email}', quiz_id:'{quiz_id}', 
                       server_time:'{server_time_utc}',
                       parsed_data_encoded:'{scores_encoded}'
                    }})"""
                result = conn.uql(uql_query)
                print("📥 UQL executed:", uql_query)
                print("📤 Ultipa response:", result.toDict())

                # 6. Link the new record to the visitor and quiz using direct edge insertion
                try:
                    # Visitor → Record edge
                    visitor_edge_uql = f"insert().into(@has).edges({{ _from:'{visitorID}', _to:'{request_id}', id:'{request_id}_visitor' }})"
                    visitor_edge_result = conn.uql(visitor_edge_uql)
                    print("🔗 Visitor→Record edge result:", visitor_edge_result.toDict())
                
                    # Quiz → Record edge
                    quiz_edge_uql = f"insert().into(@has).edges({{ _from:'{quiz_id}', _to:'{request_id}', id:'{request_id}_quiz' }})"
                    quiz_edge_result = conn.uql(quiz_edge_uql)
                    print("🔗 Quiz→Record edge result:", quiz_edge_result.toDict())
                
                except Exception as edge_err:
                    print("❌ Edge creation error:", edge_err)


                confirm_query = f"find().nodes({{@record.record_id == '{request_id}'}}) return @record"
                confirm_result = conn.uql(confirm_query)
                print("🔍 Confirm record inserted:", confirm_result.toDict())


                print(f"💾 Successfully wrote and linked heatmap record {request_id} to Ultipa DB.")
            else:
                raise ValueError("Cannot create record without a valid quiz_id.")
            
            
        except Exception as db_e:
            import traceback
            print("❌ DB insertion error:", db_e)
            traceback.print_exc()

    except Exception as e:
        error_msg = f"Processing error: {str(e)}"
        app.logger.error(str(e))

    log_entry = { "request_id": request_id, "timestamp": processing_time_utc, "endpoint": "/receive-heatmap", "raw_payload": raw_post_body, "parsed_json_from_payload": parsed_data, "error": error_msg }
    log_writer.write_entry(log_entry)

    if error_msg:
        print(f"❌ Error on /receive-heatmap: {error_msg}")
        return jsonify({"status": "error", "message": error_msg}), 400
        
    return jsonify({"status": "success", "request_id": request_id})



# ================== Start the service ==================
if __name__ == '__main__':
    env = detect_environment()

    print(f" Current running environment: {env.upper()}")
    print(f" Log storage：{os.path.abspath(LOG_DIR)}(Split by day)")
    print(f" CSV storage:{os.path.abspath(CSV_DIR)}(Split by day)")

    if env == "production":
        # Provide clear guidance on the production environment
        print("\n Flask development servers are prohibited in production environments!")
        print(" Please start the production server with the following command:")
        print(f"gunicorn -w 4 -b 0.0.0.0:{PORT} your_filename:app")
        print(" Tip: Replace your_filename with the current Python file name (excluding extension)")
        exit(1)
    else:
        # The development environment starts a server with warnings
        print(f"\n The development server has been started.")
        print(f"   -> Wellbeing Quiz Endpoint: http://localhost:{PORT}/receive5001")
        print(f"   -> Heatmap Quiz Endpoint:   http://localhost:{PORT}/receive-heatmap") 
        print(" Note: It is normal to see WARNING, which is the expected behavior of the development server.")
        app.run(host='0.0.0.0', port=PORT, debug=False)# The production environment should set debug=False. If debug=True, the debug mode will be enabled for easy debugging.


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