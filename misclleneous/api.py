from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import snowflake.connector as snowflake
import google.cloud.storage as storage

app = FastAPI()

class SnowflakeGCSConfig(BaseModel):
    snowflake_user: str
    snowflake_password: str
    snowflake_account: str
    snowflake_database: str
    snowflake_schema: str
    snowflake_warehouse: str
    snowflake_role: str
    gcs_bucket_name: str
    gcs_project_id: str

@app.post("/start-transfer")
async def start_transfer(config: SnowflakeGCSConfig):
    stage_name = 'REPORTSTG'
    storage_integration = 'gcs_int'

    # Create Snowflake connection
    try:
        conn = snowflake.connect(
            user=config.snowflake_user,
            password=config.snowflake_password,
            account=config.snowflake_account,
            warehouse=config.snowflake_warehouse,
            database=config.snowflake_database,
            schema=config.snowflake_schema,
            role=config.snowflake_role
        )
        cur = conn.cursor()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error connecting to Snowflake: {e}")

    try:
        # Create Storage Integration
        create_integration_sql = f"""
        CREATE OR REPLACE STORAGE INTEGRATION {storage_integration}
        TYPE = EXTERNAL_STAGE
        STORAGE_PROVIDER = GCS
        ENABLED = TRUE
        STORAGE_ALLOWED_LOCATIONS = ('gcs://{config.gcs_bucket_name}/')
        """
        cur.execute(create_integration_sql)
    except Exception as e:
        cur.close()
        conn.close()
        raise HTTPException(status_code=400, detail=f"Storage integration error: {e}")

    try:
        # Create Stage
        create_stage_sql = f"""
        CREATE OR REPLACE STAGE {stage_name}
        URL='gcs://{config.gcs_bucket_name}/'
        STORAGE_INTEGRATION={storage_integration}
        """
        cur.execute(create_stage_sql)
    except Exception as e:
        cur.close()
        conn.close()
        raise HTTPException(status_code=400, detail=f"Stage creation error: {e}")

    try:
        # Copy data from Snowflake to GCS
        copy_data_sql = f"""
        COPY INTO @{stage_name}/report/Report_Device_Inventory
        FROM "{config.snowflake_database}"."{config.snowflake_schema}"."Report_Device_Inventory"
        FILE_FORMAT = (TYPE = 'CSV' COMPRESSION = 'GZIP')
        OVERWRITE = TRUE
        """
        cur.execute(copy_data_sql)
    except Exception as e:
        cur.close()
        conn.close()
        raise HTTPException(status_code=500, detail=f"Data copy error: {e}")

    cur.close()
    conn.close()
    return {"message": "Data copied from Snowflake to GCS successfully."}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, port=8000)
