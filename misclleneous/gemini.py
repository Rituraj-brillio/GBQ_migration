import os
import snowflake.connector as snowflake
import google.generativeai as genai
import re
import configparser
import pandas as pd
import streamlit as st   


# Snowflake credentials
snowflake_user = 'SwapnilK'
snowflake_password = 'Todayis31st'
snowflake_account = 'tr62363.us-east-2.aws'
snowflake_database = 'SNOWFLAKE_TO_GBQ'
snowflake_schema = 'ANNUAL_ENTERPRISE_SURVEY_2021'
snowflake_warehouse = 'ATF_TESTING'
snowflake_role = 'SYSADMIN'

# Gemini API key
apikey = 'AIzaSyAl1zyKrzdn9ymlnFJ_f7DT_8FlOkZOidE'
genai.configure(api_key=apikey)

generation_config = {"temperature": 0.2, "top_p": 0.9, "top_k": 40}
prompt = """
You are an AI that converts natural language questions into snowflake-SQL queries. Your goal is to understand the user's intent and generate a valid snowflake-SQL query.

Here are some examples of natural language questions and their corresponding snowflake-SQL queries:
<natural language question>
"Show me total row count."

<snowflake SQL query>
SELECT COUNT(*) AS val
FROM SNOWFLAKE_TO_GBQ.ANNUAL_ENTERPRISE_SURVEY_2021.ANNUAL_ENTERPRISE_SURVEY_2021;

<natural language question>
"Can you provide the total income for each industry based on the 'H01' variable code."

<snowflake SQL query>
SELECT 
Industry_name_NZSIOC,
SUM(CAST(REPLACE(value, ',', '') AS NUMBER)) AS total_income
FROM SNOWFLAKE_TO_GBQ.ANNUAL_ENTERPRISE_SURVEY_2021.ANNUAL_ENTERPRISE_SURVEY_2021
WHERE variable_code = 'H01'
GROUP BY Industry_name_NZSIOC;


Now, generate the SQL query for the following question:
"""

 # Function to load Google Gemini Model and provide queries as response
def get_gemini_response(question, prompt, generation_config):
    model = genai.GenerativeModel('gemini-1.5-pro-latest', generation_config=generation_config)
    response = model.generate_content([prompt[0], question])
    return response.text

# Function to connect to Snowflake and execute a query
def execute_snowflake_query(query):
    conn = snowflake.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account,
        warehouse=snowflake_warehouse,
        schema=snowflake_schema,
        role=snowflake_role
    )
    query_result = pd.read_sql_query(query, conn)
    conn.close()
    return query_result

#Function to handle natural language input and generate SQL query
def handle_nl_to_sql(user_prompt):
    # Prepare the prompt for Gemini-Pro model
    gemini_prompt = prompt + "\nUser: " + user_prompt

    # Get the SQL query from the Gemini-Pro model
    gemini_response = get_gemini_response(gemini_prompt, prompt, generation_config)
    print("Gemini Response: ", gemini_response)
    
    # Extract SQL query from Gemini-Pro's response
    sql_match = re.search(r"```sql\n(.*)\n```", gemini_response, re.DOTALL)
    if sql_match:
        sql_query = sql_match.group(1).strip("```")
        print("Generated SQL Query: ", sql_query)
        
        # Execute the SQL query on Snowflake
        query_result = execute_snowflake_query(sql_query)
        print("Query Result: ", query_result)
        return query_result
    else:
        fallback_message = "I am sorry, I am unable to provide you with the response. This is because your question needs to be improved or the data you are requesting is beyond the scope of what is available to me."
        print(fallback_message)
        return fallback_message
# Streamlit UI
st.title("Natural Language to SQL queries")
user_prompt = st.text_input("Enter your query in natural language:")
if st.button("Submit"):
    if user_prompt:
        result = handle_nl_to_sql(user_prompt)
        st.write("Result:", result)
    else:
        st.write("Please enter a query.")

