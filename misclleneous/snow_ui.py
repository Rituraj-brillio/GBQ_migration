import streamlit as st
import snowflake.connector as snowflake
import google.cloud.storage as storage

# Streamlit UI
st.title('Snowflake to GCS Data Transfer')

# Snowflake credentials input
st.subheader('Snowflake Credentials')
snowflake_user = st.text_input('Snowflake Username')
snowflake_password = st.text_input('Snowflake Password', type='password')
snowflake_account = st.text_input('Snowflake Account')
snowflake_database = st.text_input('Snowflake Database')
snowflake_schema = st.text_input('Snowflake Schema')
snowflake_warehouse = st.text_input('Snowflake Warehouse')
snowflake_role = st.text_input('Snowflake Role')

# GCS credentials input
st.subheader('GCS Credentials')
gcs_bucket_name = st.text_input('GCS Bucket Name')
gcs_project_id = st.text_input('GCS Project ID')


# Stage and integration names
stage_name = 'REPORTSTG'
storage_integration = 'gcs_int'

# Button to trigger the process
if st.button('Start Data Transfer'):
    # Create Snowflake connection
    try:
        conn = snowflake.connect(
            user=snowflake_user,
            password=snowflake_password,
            account=snowflake_account,
            warehouse=snowflake_warehouse,
            database=snowflake_database,
            schema=snowflake_schema,
            role=snowflake_role
        )
        cur = conn.cursor()
        st.success('Snowflake connection established successfully.')
    except Exception as e:
        st.error(f'Error connecting to Snowflake: {e}')
        st.stop()

    try:
        # Create Storage Integration
        create_integration_sql = f"""
        CREATE OR REPLACE STORAGE INTEGRATION {storage_integration}
        TYPE = EXTERNAL_STAGE
        STORAGE_PROVIDER = GCS
        ENABLED = TRUE
        STORAGE_ALLOWED_LOCATIONS = ('gcs://{gcs_bucket_name}/')
        """
        cur.execute(create_integration_sql)
        st.success(f"Storage integration '{storage_integration}' created successfully.")
    except Exception as e:
        st.warning(f"Storage integration '{storage_integration}' already exists or an error occurred: {e}")

    try:
        # Create Stage
        create_stage_sql = f"""
        CREATE OR REPLACE STAGE {stage_name}
        URL='gcs://{gcs_bucket_name}/'
        STORAGE_INTEGRATION={storage_integration}
        """
        cur.execute(create_stage_sql)
        st.success(f"Stage '{stage_name}' created successfully.")
    except Exception as e:
        st.error(f"An error occurred while creating the stage: {e}")
        st.stop()

    try:
        # Copy data from Snowflake to GCS
        copy_data_sql = f"""
        COPY INTO @{stage_name}/NODE/Node_with_IP
        FROM "{snowflake_database}"."{snowflake_schema}"."Node_with_IP"
        FILE_FORMAT = (TYPE = 'CSV' COMPRESSION = 'GZIP')
        OVERWRITE = TRUE
        """
        cur.execute(copy_data_sql)
        st.success("Data copied from Snowflake to GCS successfully.")
    except Exception as e:
        st.error(f"An error occurred while copying data: {e}")

    # Close Snowflake connection
    cur.close()
    conn.close()