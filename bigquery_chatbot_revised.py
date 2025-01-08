# import vertexai
# from vertexai.preview.generative_models import GenerativeModel, Image
# import json
# import os
# import certifi

# os.environ["REQUESTS_CA_BUNDLE"] = certifi.where() 
# PROJECT_ID = "brlcto-gbq-migration"
# REGION = "us-central1"
# vertexai.init(project=PROJECT_ID, location=REGION)

# generative_multimodal_model = GenerativeModel("gemini-1.5-flash")
# prompt = "Explain what AI is in simple terms."
# response = generative_multimodal_model.generate_content(prompt)
# if response and response.candidates:
#     print(response.candidates[0].content)  # Assuming `content` holds the response
# else:
#     print("No candidates found in the response.")





import streamlit as st
import pandas as pd
from google.cloud import bigquery
from langchain_google_genai import ChatGoogleGenerativeAI
import ssl
import warnings
import re
import os
import time
import certifi

# Set Streamlit page configuration
st.set_page_config(page_title="BigQuery NL to SQL Converter", layout="wide")

# Set the API key and environment variables
os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()
os.environ["GOOGLE_API_KEY"] = "AIzaSyAl1zyKrzdn9ymlnFJ_f7DT_8FlOkZOidE"  # Replace with your actual API key

# BigQuery credentials and configurations
project_id = 'brlcto-gbq-migration'  # Replace with your Google Cloud Project ID

# Initialize the LLM with the model name
llm = ChatGoogleGenerativeAI(model="gemini-1.5-pro")

# Initialize BigQuery client using Application Default Credentials
try:
    bq_client = bigquery.Client(project=project_id)
    st.write(f"BigQuery Client Initialized for project: {project_id}")
except Exception as e:
    st.error(f"Failed to initialize BigQuery client: {e}")

# Configuration for Gemini API
generation_config = {"temperature": 0.0, "top_p": 0.9, "top_k": 40}

# Helper function to get datasets from BigQuery
def get_datasets():
    try:
        datasets = bq_client.list_datasets()
        return [dataset.dataset_id for dataset in datasets]
    except Exception as e:
        st.error(f"Failed to retrieve datasets: {e}")
        return []

# Helper function to get tables from a dataset
def get_tables(dataset):
    try:
        tables = bq_client.list_tables(dataset)
        return [table.table_id for table in tables]
    except Exception as e:
        st.error(f"Failed to retrieve tables for dataset {dataset}: {e}")
        return []

# Helper function to get the schema of a table
def get_table_schema(dataset, table):
    try:
        table_ref = f"{project_id}.{dataset}.{table}"
        table = bq_client.get_table(table_ref)
        return table.schema
    except Exception as e:
        st.error(f"Failed to retrieve schema for table {table} in dataset {dataset}: {e}")
        return None

# Function to fetch schemas of all tables
def fetch_schemas():
    schemas = {}
    datasets = get_datasets()
    for dataset in datasets:
        tables = get_tables(dataset)
        for table in tables:
            schema = get_table_schema(dataset, table)
            if schema:
                schemas[f"{dataset}.{table}"] = schema
    return schemas

# Function to correct table names in user prompts
def correct_table_name(user_prompt, schemas):
    for full_table_name in schemas.keys():
        dataset, table = full_table_name.split('.')
        if table.lower() in user_prompt.lower():
            return full_table_name
    return None

# Function to create a prompt for Gemini API
def create_prompt():
    prompt = f"""
    You are an expert in converting natural language questions into BigQuery SQL queries. 
    Your goal is to understand the user's intent and generate a valid BigQuery SQL query.
    dont include \n characeters in the response.below are examples you can refer to while converting 
    
    Example 1:
    <natural language question>
    "give me all datasets"
    <BigQuery SQL query>
    SELECT * FROM `{project_id}.INFORMATION_SCHEMA.SCHEMATA`;

    """
    datasets = get_datasets()
    for dataset in datasets:
        prompt += f"""
        <natural language question>
        "show me all tables in {dataset} dataset"
        <BigQuery SQL query>
        SELECT * FROM `{project_id}.{dataset}.INFORMATION_SCHEMA.TABLES`;

        """
        tables = get_tables(dataset)
        for table in tables:
            schema = get_table_schema(dataset, table)
            if schema:
                columns = ", ".join([field.name for field in schema])
                prompt += f"""
                <natural language question>
                "Show me total row count from {table} table in {dataset} dataset."
                <BigQuery SQL query>
                SELECT COUNT(*) AS val FROM `{project_id}.{dataset}.{table}`;

                <natural language question>
                "Show me columns from {table} table in {dataset} dataset."
                <BigQuery SQL query>
                SELECT {columns} FROM `{project_id}.{dataset}.{table}`;
                """
    prompt += "\nNow, generate the SQL query for the following question:\n"
    return prompt

# Function to get response from Gemini API with retry logic
def get_gemini_response(question, prompt, generation_config, max_retries=3, backoff_factor=2):
    retries = 0
    while retries < max_retries:
        try:
            response = llm.invoke([prompt + "\nUser: " + question])
            st.write('Data', response)
            return response.content
        except Exception as e:
            error_message = str(e)
            if "500" in error_message:
                retries += 1
                wait_time = backoff_factor ** retries
                st.warning(f"Retrying in {wait_time} seconds due to server error: {error_message}")
                time.sleep(wait_time)
            else:
                st.error(f"Failed to get response from Gemini: {e}")
                break
    st.error("Exceeded maximum retries for Gemini API request.")
    return None

# Function to execute SQL query on BigQuery
def execute_bigquery_query(query):
    try:
        query_job = bq_client.query(query)
        query_result = query_job.result().to_dataframe()
        return query_result
    except Exception as e:
        st.error(f"Failed to execute query: {e}\nQuery: {query}\nError Details: {str(e)}")
        return None

# Function to handle natural language to SQL conversion
def handle_nl_to_sql(user_prompt):
    schemas = fetch_schemas()
    correct_table = correct_table_name(user_prompt, schemas)
    if correct_table:
        user_prompt = re.sub(r'\b{}\b'.format(correct_table.split('.')[1]), correct_table, user_prompt, flags=re.IGNORECASE)
    
    gemini_prompt = create_prompt() + "\nUser: " + user_prompt
    gemini_response = get_gemini_response(user_prompt, gemini_prompt, generation_config)
    if not gemini_response:
        return "Failed to get response from Gemini."

    st.write("Gemini Response: ", gemini_response)
    
    # Check if the response contains a valid SQL query
    sql_query = None
    # Updated logic to detect SQL content
    if "SELECT" in gemini_response.upper() and ";" in gemini_response:
        sql_query = gemini_response.strip()
    
    if sql_query:
        st.write("Generated SQL Query: ", sql_query)
        
        query_result = execute_bigquery_query(sql_query)
        if query_result is not None:
            st.write("Query Result: ", query_result)
            return query_result
        else:
            return "Failed to execute the generated SQL query."
    else:
        fallback_message = "I am sorry, I am unable to provide you with the response. This is because your question needs to be improved or the data you are requesting is beyond the scope of what is available to me."
        st.write(fallback_message)
        return fallback_message

# Streamlit UI components
st.title("Natural Language to BigQuery SQL Converter")
user_prompt = st.text_input("Enter your natural language query:")

if st.button("Convert and Query"):
    if user_prompt:
        result = handle_nl_to_sql(user_prompt)
        if isinstance(result, pd.DataFrame):
            st.dataframe(result)
        else:
            st.write(result)
    else:
        st.warning("Please enter a query before clicking the button.")
