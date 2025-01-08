import streamlit as st
import requests

st.title('Snowflake to GCS Data Transfer')

st.subheader('Snowflake Credentials')
snowflake_user = st.text_input('Snowflake Username')
snowflake_password = st.text_input('Snowflake Password', type='password')
snowflake_account = st.text_input('Snowflake Account')
snowflake_database = st.text_input('Snowflake Database')
snowflake_schema = st.text_input('Snowflake Schema')
snowflake_warehouse = st.text_input('Snowflake Warehouse')
snowflake_role = st.text_input('Snowflake Role')

st.subheader('GCS Credentials')
gcs_bucket_name = st.text_input('GCS Bucket Name')
gcs_project_id = st.text_input('GCS Project ID')

if st.button('Start Data Transfer'):
    url = 'http://localhost:8000/start-transfer'
    payload = {
        "snowflake_user": snowflake_user,
        "snowflake_password": snowflake_password,
        "snowflake_account": snowflake_account,
        "snowflake_database": snowflake_database,
        "snowflake_schema": snowflake_schema,
        "snowflake_warehouse": snowflake_warehouse,
        "snowflake_role": snowflake_role,
        "gcs_bucket_name": gcs_bucket_name,
        "gcs_project_id": gcs_project_id
    }
    response = requests.post(url, json=payload)
    if response.status_code == 200:
        st.success(response.json()['message'])
    else:
        st.error(f"Error: {response.json()['detail']}")
