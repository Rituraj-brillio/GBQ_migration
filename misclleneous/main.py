from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
import snowflake.connector as snowflake

app = FastAPI()

class SnowflakeCredentials(BaseModel):
    user: str
    password: str
    account: str
    database: str
    Schema: str 
    warehouse: str
    role: str

class GCSCredentials(BaseModel):
    bucket_name: str
    project_id: str

class DataTransferRequest(BaseModel):
    snowflake: SnowflakeCredentials
    gcs: GCSCredentials

@app.post("/transfer-data")
async def transfer_data(request: DataTransferRequest):
    # Extract credentials
    snowflake_creds = request.snowflake
    gcs_creds = request.gcs

    # Stage and integration names
    stage_name = 'REPORTSTG'
    storage_integration = 'gcs_int'

    # Connect to Snowflake
    try:
        conn = snowflake.connect(
            user=snowflake_creds.user,
            password=snowflake_creds.password,
            account=snowflake_creds.account,
            warehouse=snowflake_creds.warehouse,
            database=snowflake_creds.database,
            Schema=snowflake_creds.Schema,
            role=snowflake_creds.role
        )
        cur = conn.cursor()
        print('snowflake connection successfull')
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error connecting to Snowflake: {e}")

    try:
        # Create Storage Integration
        create_integration_sql = f"""
        CREATE OR REPLACE STORAGE INTEGRATION {storage_integration}
        TYPE = EXTERNAL_STAGE
        STORAGE_PROVIDER = GCS
        ENABLED = TRUE
        STORAGE_ALLOWED_LOCATIONS = ('gcs://{gcs_creds.bucket_name}/')
        """
        cur.execute(create_integration_sql)
        print(f"Storage integration '{storage_integration}' created successfully.")
    except Exception as e:
        # Log a warning but don't stop the process
        print(f"Storage integration '{storage_integration}' already exists or an error occurred: {e}")

    try:
        # Create Stage
        create_stage_sql = f"""
        CREATE OR REPLACE STAGE {stage_name}
        URL='gcs://{gcs_creds.bucket_name}/'
        STORAGE_INTEGRATION={storage_integration}
        """
        cur.execute(create_stage_sql)
        print('stage creation successfull')
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An error occurred while creating the stage: {e}")

    try:
        # Copy data from Snowflake to GCS
        copy_data_sql = f"""
        COPY INTO @{stage_name}/report/Report_Device_Inventory
        FROM "{snowflake_creds.database}"."{snowflake_creds.Schema}"."Report_Device_Inventory"
        FILE_FORMAT = (TYPE = 'CSV' COMPRESSION = 'GZIP')
        OVERWRITE = TRUE
        """
        cur.execute(copy_data_sql)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An error occurred while copying data: {e}")

    # Close Snowflake connection
    cur.close()
    conn.close()

    return {"message": "Data copied from Snowflake to GCS successfully."}
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
