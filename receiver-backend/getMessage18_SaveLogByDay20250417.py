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
class DailyCsvWriter(DailyFileHandler):  # Class name modification
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


# ================== Log Writer ==================
class DailyLogWriter(DailyFileHandler):  # Class name modification
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

# ================== Data processing functions ==================
def process_data(post_data, processing_time_server, processing_time_utc, request_id):
    """
    Extract structured information from raw POST data
    :param post_data: Dictionary that must contain the "json" key
    :param processing_time_server: server's local time zone timestamp (consistent with log)
    :param processing_time_utc: UTC+0 time zone time stamp
    :return: Structured DataFrame
    """
    try:
        data = post_data

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


# ================== Flask routing processing ==================
# Add strict_slashes=False to handle URLs that may or may not have a trailing slash
@app.route('/receive5001', methods=['POST'], strict_slashes=False) #When any HTTP POST request reaches http://server address:POST/receive, Flask will automatically parse the request data, call the receive_data() function to process the request and return the response.
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
            # This is the industry-standard way. 
            # It requires the request to have a 'Content-Type: application/json' header.
            parsed_data = request.get_json()
            
            # If no JSON is sent, or it's empty, raise an error.
            if not parsed_data:
                raise ValueError("No JSON data received or body is empty.")

            # Now, call your processing function with the clean data
            df = process_data(parsed_data, processing_time_server, processing_time_utc, request_id)
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
        print(f"\n The development server has been started：http://localhost:{PORT}/receive5001")
        print(" Note: It is normal to see WARNING, which is the expected behavior of the development server.")
        app.run(host='0.0.0.0', port=PORT, debug=False)  # The production environment should set debug=False. If debug=True, the debug mode will be enabled for easy debugging.


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