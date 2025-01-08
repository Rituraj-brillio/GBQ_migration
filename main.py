import os
import certifi
from google.cloud import storage
import json
import snowflake.connector as snowflake
from datetime import datetime
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

class TransferRequest(BaseModel):
    snowflake_user: str
    snowflake_password: str
    snowflake_account: str
    snowflake_database: str
    snowflake_schema: str
    snowflake_table: str
    snowflake_warehouse: str
    snowflake_role: str
    gcs_bucket_name: str
    gcs_project_id: str
    gcs_folder_name: str
    gcs_file_name: str

def get_formatted_datetime():
    now = datetime.now()
    return now.strftime("%Y%m%d_%H%M%S")

@app.route('/transfer', methods=['POST', 'OPTIONS'])
def transfer_data():
    cors_headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Origin, Content-Type, Accept, Authorization, X-Requested-With',
        'Access-Control-Max-Age': '3600',
    }

    if request.method == 'OPTIONS':
        return ('', 204, cors_headers)

    try:
        data = request.get_json()
        print("Received data:", data)
        
        source_data = data.get('sourceData')
        if not source_data:
            return jsonify({"error": "sourceData field is missing"}), 400, cors_headers
        
        transfer_request = TransferRequest(**source_data)
        print("Validated transfer_request:", transfer_request)

        # Create Snowflake connection
        conn = snowflake.connect(
            user=transfer_request.snowflake_user,
            password=transfer_request.snowflake_password,
            account=transfer_request.snowflake_account,
            warehouse=transfer_request.snowflake_warehouse,
            database=transfer_request.snowflake_database,
            schema=transfer_request.snowflake_schema,
            role=transfer_request.snowflake_role
        )

        cur = conn.cursor()
        print("Snowflake connection established successfully.")

        # Create Snowflake stage
        stage_name = 'CPULOADSTG'
        storage_integration = 'gcs_int'

        try:
            # Create Storage Integration (if not already created)
            create_integration_sql = f"""
            CREATE OR REPLACE STORAGE INTEGRATION {storage_integration}
            TYPE = EXTERNAL_STAGE
            STORAGE_PROVIDER = GCS
            ENABLED = TRUE
            STORAGE_ALLOWED_LOCATIONS = ('gcs://{transfer_request.gcs_bucket_name}/')
            """
            cur.execute(create_integration_sql)
            print(f"Storage integration '{storage_integration}' created successfully.")
        except Exception as e:
            print(f"Storage integration '{storage_integration}' already exists or an error occurred: {e}")

        try:
            # Create Stage
            create_stage_sql = f"""
            CREATE OR REPLACE STAGE {stage_name}
            URL='gcs://{transfer_request.gcs_bucket_name}/'
            STORAGE_INTEGRATION={storage_integration}
            """
            cur.execute(create_stage_sql)
            print(f"Stage '{stage_name}' created successfully.")
        except Exception as e:
            print(f"An error occurred while creating the stage: {e}")

        # Copy data from Snowflake to GCS
        formatted_datetime = get_formatted_datetime()

        try:
            copy_data_sql = f"""
            COPY INTO @{stage_name}/{transfer_request.gcs_folder_name}/{formatted_datetime}/{transfer_request.gcs_file_name}
            FROM "{transfer_request.snowflake_database}"."{transfer_request.snowflake_schema}"."{transfer_request.snowflake_table}"
            FILE_FORMAT = (TYPE = 'CSV' COMPRESSION = 'GZIP')
            OVERWRITE = TRUE
            """
            cur.execute(copy_data_sql)
            print("Data copied from Snowflake to GCS successfully.")
        except Exception as e:
            print(f"An error occurred while copying data: {e}")
            return jsonify({"error": f"An error occurred while copying data: {e}"}), 500, cors_headers

        return jsonify({"message": "Data copied from Snowflake to GCS successfully."}), 200, cors_headers

    except ValidationError as e:
        print(f"Validation error: {e}")
        return jsonify({"error": e.errors()}), 400, cors_headers
    except Exception as e:
        print(f"Exception: {e}")
        return jsonify({"error": str(e)}), 500, cors_headers

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=True)
