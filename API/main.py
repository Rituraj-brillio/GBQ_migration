import os
import certifi
from google.cloud import storage
import json
from flask import Flask, request, jsonify
from flask_cors import CORS
from pydantic import BaseModel, ValidationError

app = Flask(__name__)
CORS(app, origins=["http://localhost:3000"])

# Set the certificate bundle environment variable
os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()

# Define bucket name and file name as constants
BUCKET_NAME = "snow_function"
FILE_NAME = "mapping.json"

class MappingData(BaseModel):
    gbq_output_table: str
    project_id: str
    region: str
    temp_location: str
    delimiter: str
    gbq_write_mode: str
    overwrite: bool = True

def upload_mapping_json(bucket_name, file_name, gbq_output_table, project_id, region,
                        temp_location, delimiter, gbq_write_mode, overwrite=True):
    # Create JSON content
    mapping = {
        "gbq_output_table": gbq_output_table,
        "project_id": project_id,
        "region": region,
        "temp_location": temp_location,
        "delimiter": delimiter,
        "gbq_write_mode": gbq_write_mode,
        "overwrite": overwrite
    }
    json_content = json.dumps(mapping, indent=4)

    # Upload JSON file to GCS
    storage_client = storage.Client(project='brlcto-gbq-migration')
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    if blob.exists() and overwrite:
        blob.delete()

    if not blob.exists() or overwrite:
        blob.upload_from_string(json_content)

    print(f"File {file_name} uploaded to bucket {bucket_name} with overwrite={overwrite}")

@app.route('/upload_mapping', methods=['POST', 'OPTIONS'])
def upload_mapping():
    try:
        if request.method == 'OPTIONS':
            cors_headers = {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                'Access-Control-Allow-Headers': 'Origin, Content-Type, Accept, Authorization, X-Requested-With',
                'Access-Control-Max-Age': '3600',
            }
            return ('', 204, cors_headers)
        
        cors_headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
            'Access-Control-Allow-Headers': 'Origin, Content-Type, Accept, Authorization, X-Requested-With',
        }

        data = request.get_json()
        print("Received data:", data)

        # Extract the gbqData dictionary from the incoming data

        
        try:
            mapping_data = MappingData(**data)
            print("Validated mapping data:", mapping_data)

            print(BUCKET_NAME, FILE_NAME, mapping_data.gbq_output_table, mapping_data.project_id, mapping_data.region, mapping_data.temp_location, mapping_data.delimiter, mapping_data.gbq_write_mode, mapping_data.overwrite)
    
            # Upload the JSON file
            upload_mapping_json(BUCKET_NAME, FILE_NAME, mapping_data.gbq_output_table, mapping_data.project_id,
                                mapping_data.region, mapping_data.temp_location, mapping_data.delimiter,
                                mapping_data.gbq_write_mode, mapping_data.overwrite)
            return jsonify({"message": f"File {FILE_NAME} uploaded to bucket {BUCKET_NAME} with overwrite={mapping_data.overwrite}"}), 200, cors_headers
        except ValidationError as e:
            print("Validation error:", e)
            return jsonify({"error": e.errors()}), 400, cors_headers
    except Exception as e:
        print("Exception:", e)
        return jsonify({"error": str(e)}), 500, cors_headers

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
