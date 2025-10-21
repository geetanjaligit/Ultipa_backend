from flask import Flask, request, jsonify
from flask_cors import CORS

# Import the record-fetching functions from your other files
from api_team_records import fetch_team_records
from api_community_records import fetch_community_records
from analysis7_readDB_response import fetch_user_records
from api_heatmap_records import fetch_heatmap_records_by_email

app = Flask(__name__)
CORS(app)

@app.route('/records', methods=['POST'])
def records_handler():
    data = request.get_json()
    record_type = data.get('type', 'visitor')
    try:
        if record_type == 'visitor':
            email = data.get('email')
            if not email:
                return jsonify({'error': 'Missing email'}), 400
            records, error = fetch_user_records(email)
            if not records:
                return jsonify({'error': 'No data found'}), 404
            return jsonify({
                "matched_records": records,
                "total_records": len(records)
            })

        elif record_type == 'team':
            team_id = data.get('team_id')
            email = data.get('email')
            if team_id:
                records, error = fetch_team_records(team_id)
            elif email:
                from api_team_records import get_team_id_by_email
                team_id = get_team_id_by_email(email)
                if not team_id:
                    return jsonify({'error': 'No team found for this email'}), 404
                records, error = fetch_team_records(team_id)
            else:
                return jsonify({'error': 'Missing team_id or email'}), 400
            if not records:
                return jsonify({'error': 'No data found'}), 404
            return jsonify({
                "matched_records": records,
                "total_records": len(records)
            })

        elif record_type == 'community':
            community_id = data.get('community_id')
            email = data.get('email')
            if community_id:
                records, error = fetch_community_records(community_id)
            elif email:
                from api_community_records import get_community_id_by_email
                community_id = get_community_id_by_email(email)
                if not community_id:
                    return jsonify({'error': 'No community found for this email'}), 404
                records, error = fetch_community_records(community_id)
            else:
                return jsonify({'error': 'Missing community_id or email'}), 400
            if not records:
                return jsonify({'error': 'No data found'}), 404
            return jsonify({
                "matched_records": records,
                "total_records": len(records)
            })
        
        elif record_type == 'heatmap':
            email = data.get('email')
            if not email:
                return jsonify({'error': 'Missing email'}), 400
            
            records = fetch_heatmap_records_by_email(email)
            
            if not records:
                return jsonify({'error': 'No heatmap data found for this user'}), 404

            return jsonify({
                "email": email,
                "total_records": len(records),
                "records": records
            })

        else:
            return jsonify({'error': 'Invalid type'}), 400

    except Exception as e:
        return jsonify({'error': f'Internal server error: {str(e)}'}), 500

if __name__ == '__main__':
    app.run(port=5005)