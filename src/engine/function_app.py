# src/engine/function_app.py
import logging
import pandas as pd
import io
import os
import json
import azure.functions as func
from sqlalchemy import create_engine,text
import urllib
import numpy as np
from azure.storage.blob import BlobServiceClient
from azure.storage.queue import QueueServiceClient
import azure.durable_functions as df
from durable_blueprints import bp

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)
app.register_functions(bp)

def get_user_id(req: func.HttpRequest) -> str:
    """Helper function to safely extract the user's unique ID from request headers."""
    # The 'x-ms-client-principal-id' header is automatically injected by
    # the Azure App Service Authentication feature we just enabled.
    return req.headers.get("x-ms-client-principal-id")


@app.function_name(name="HttpIngestPolicyCsv")
@app.route(route="ingest/policies/csv", auth_level=func.AuthLevel.ANONYMOUS, methods=["post"])
def http_ingest_policy_csv(req: func.HttpRequest) -> func.HttpResponse:
    """
    This v2 function ingests policy data from a CSV POST request
    and loads it into the Azure SQL database.
    """
    logging.info('Python HTTP Ingestion function (v2 model) triggered.')

    sql_connection_string = os.environ.get("SqlConnectionString")
    if not sql_connection_string:
        return func.HttpResponse("FATAL: SqlConnectionString setting is missing.", status_code=500)

    try:
        # Get the raw CSV data from the body of the POST request
        csv_data = req.get_body()
        
        # Use Pandas to read the CSV data from memory
        # We assume the CSV headers perfectly match our SQL table columns for simplicity
        df = pd.read_csv(io.BytesIO(csv_data))
        
        # --- Database Connection Logic ---
        params = urllib.parse.quote_plus(sql_connection_string)
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

        # Use Pandas' to_sql function to efficiently insert the data
        # 'if_exists="replace"' will clear the table and insert the new data.
        # This is better for our simple MVP workflow.
        df.to_sql('Policies', con=engine, if_exists='replace', index=False, chunksize=1000)

        return func.HttpResponse(
            body=f"Successfully ingested and replaced {len(df)} policy records.",
            status_code=200
        )

    except Exception as e:
        logging.error(f"An error occurred during ingestion: {e}")
        return func.HttpResponse(f"Error during ingestion: {e}", status_code=500)


@app.function_name(name="HttpIngestPolicyJson")
@app.route(route="ingest/policies/json", auth_level=func.AuthLevel.ANONYMOUS, methods=["post"])
def http_ingest_policy_json(req: func.HttpRequest) -> func.HttpResponse:
    """Ingests policy data from a JSON POST request into the SQL database."""
    logging.info('Python HTTP JSON Policy Ingestion function triggered.')
    
    sql_connection_string = os.environ.get("SqlConnectionString")
    if not sql_connection_string:
        return func.HttpResponse("FATAL: SqlConnectionString setting is missing.", status_code=500)

    try:
        # Get the JSON data from the body of the POST request
        json_data = req.get_json()
        
        # Use Pandas to convert the list of JSON objects into a DataFrame
        # We assume the JSON is a list of records, e.g., [{"Policy_ID": "A", ...}, {"Policy_ID": "B", ...}]
        df = pd.DataFrame(json_data)
        
        # The rest of the logic is identical to the CSV ingest function
        params = urllib.parse.quote_plus(sql_connection_string)
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

        df.to_sql('Policies', con=engine, if_exists='replace', index=False, chunksize=1000)

        return func.HttpResponse(
            body=f"Successfully ingested and replaced {len(df)} policy records from JSON.",
            status_code=200
        )
    except Exception as e:
        return func.HttpResponse(f"Error during JSON ingestion: {e}", status_code=500)



@app.function_name(name="HttpIngestScenarios")
@app.route(route="ingest/scenarios/csv", auth_level=func.AuthLevel.ANONYMOUS, methods=["post"])
def http_ingest_scenarios(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP Scenario Ingestion function triggered.')

    sql_connection_string = os.environ.get("SqlConnectionString")
    if not sql_connection_string:
        return func.HttpResponse("FATAL: SqlConnectionString setting is missing.", status_code=500)

    try:
        csv_data = req.get_body()

        # --- FIX 1: Define clean column names ---
        # This list must be in the exact same order as the columns in your CSV file.
        clean_column_names = [
            'ScenarioID', 'Month', 'Rate_0_25_yr', 'Rate_0_5_yr', 'Rate_1_yr', 
            'Rate_2_yr', 'Rate_3_yr', 'Rate_5_yr', 'Rate_7_yr', 'Rate_10_yr', 
            'Rate_20_yr', 'Rate_30_yr'
        ]

        # Read the CSV. 
        # `header=0` tells pandas to read the first row as the header.
        # `encoding='utf-8-sig'` correctly handles the invisible BOM character (﻿) at the start of your file.
        df = pd.read_csv(io.BytesIO(csv_data), encoding='utf-8-sig', header=0)
        
        # --- FIX 2: Replace the entire rename block with a direct assignment ---
        # This is more robust than trying to rename individual columns.
        df.columns = clean_column_names

        # Remove rows where Month is 0, as they are starting values, not projections
        df = df[df['Month'] > 0].copy()

        # --- Database Connection and Load ---
        params = urllib.parse.quote_plus(sql_connection_string)
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

        with engine.connect() as connection:
            with connection.begin() as transaction:
                try:
                    # --- FIX 3: Truncate the CORRECT table ---
                    logging.info("Truncating existing data from EconomicScenarios table.")
                    connection.execute(text("TRUNCATE TABLE EconomicScenarios"))
                    # The transaction will be automatically committed here if no errors occurred
                except Exception as e:
                    logging.error(f"Failed to truncate table: {e}", exc_info=True)
                    transaction.rollback()
                    raise # Re-raise the exception to stop the function

        # Load the cleaned data.
        df.to_sql('EconomicScenarios', con=engine, if_exists='append', index=False, chunksize=5000)

        return func.HttpResponse(
            body=f"Successfully ingested {len(df)} monthly scenario records.",
            status_code=200
        )

    except Exception as e:
        logging.error(f"An error occurred during scenario ingestion: {e}", exc_info=True)
        return func.HttpResponse(f"Error during scenario ingestion: {e}", status_code=500)

# In function_app.py, replace the old HttpGetJobs
@app.function_name(name="HttpGetJobs")
@app.route(route="jobs", auth_level=func.AuthLevel.FUNCTION) # All protected routes are now 'function' or 'anonymous' if handled by API Gateway
def http_get_jobs(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Request for job history received.')
    
    user_id = get_user_id(req)
    if not user_id:
        return func.HttpResponse("Authentication token is missing or invalid.", status_code=401)

    sql_connection_string = os.environ.get("SqlConnectionString")
    # ... (the rest of the function remains similar, but now includes the user check)
    try:
        engine = create_engine(urllib.parse.quote_plus(sql_connection_string))
        with engine.connect() as con:
            # --- JIT User Provisioning Logic ---
            user_check_query = text("SELECT COUNT(1) FROM Users WHERE UserID = :uid")
            user_exists = con.execute(user_check_query, {"uid": user_id}).scalar()
            
            if not user_exists:
                logging.info(f"New user detected. Provisioning user ID: {user_id}")
                # Get user details from headers injected by the auth platform
                user_email = req.headers.get("x-ms-client-principal-name")
                # For B2C, you might get a different claim for name. This is a simple start.
                user_display_name = req.headers.get("x-ms-client-principal-name")
                identity_provider = req.headers.get("x-ms-client-principal-provider")
                
                provision_query = text("""
                    INSERT INTO Users (UserID, IdentityProvider, Email, DisplayName) 
                    VALUES (:uid, :idp, :email, :name)
                """)
                con.execute(provision_query, {"uid": user_id, "idp": identity_provider, "email": user_email, "name": user_display_name})
                con.commit()
            
            # --- Fetch Jobs for the Logged-in User ONLY ---
            query = text("SELECT TOP 50 JobID, Product_Code, Job_Status, Requested_Timestamp FROM CalculationJobs WHERE UserID = :uid ORDER BY JobID DESC")
            jobs_df = pd.read_sql(query, con, params={"uid": user_id})
        
        jobs_json = jobs_df.to_json(orient='records', date_format='iso')
        return func.HttpResponse(body=jobs_json, mimetype="application/json", status_code=200)
    except Exception as e:
        return func.HttpResponse(f"Error fetching job history: {e}", status_code=500)
    

# Add this new function to function_app.py
@app.function_name(name="HttpUpdatePolicy")
@app.route(route="policies/update", auth_level=func.AuthLevel.FUNCTION, methods=["post"])
def http_update_policy(req: func.HttpRequest) -> func.HttpResponse:
    user_id = get_user_id(req)
    if not user_id: return func.HttpResponse("Unauthorized", status_code=401)
    
    try:
        policy_data = req.get_json()
        policy_id = policy_data.get("Policy_ID")
        account_value = policy_data.get("Account_Value") # Example field to update
        
        # In a real app, you would validate all incoming fields
        if not all([policy_id, account_value]):
            return func.HttpResponse("Missing required fields.", status_code=400)
            
        sql_connection_string = os.environ.get("SqlConnectionString")
        engine = create_engine(urllib.parse.quote_plus(sql_connection_string))
        with engine.connect() as con:
            query = text("UPDATE Policies SET Account_Value = :av WHERE Policy_ID = :pid AND UserID = :uid")
            result = con.execute(query, {"av": account_value, "pid": policy_id, "uid": user_id})
            con.commit()
            if result.rowcount == 0:
                return func.HttpResponse("Policy not found or you do not have permission.", status_code=404)

        return func.HttpResponse(f"Policy {policy_id} updated successfully.", status_code=200)
    except Exception as e:
        return func.HttpResponse(f"Error updating policy: {e}", status_code=500)

# Add this new function to function_app.py
@app.function_name(name="HttpDeletePolicy")
@app.route(route="policies/{policyId}", auth_level=func.AuthLevel.FUNCTION, methods=["delete"])
def http_delete_policy(req: func.HttpRequest) -> func.HttpResponse:
    user_id = get_user_id(req)
    if not user_id: return func.HttpResponse("Unauthorized", status_code=401)
    
    policy_id = req.route_params.get('policyId')
    
    try:
        sql_connection_string = os.environ.get("SqlConnectionString")
        engine = create_engine(urllib.parse.quote_plus(sql_connection_string))
        with engine.connect() as con:
            query = text("DELETE FROM Policies WHERE Policy_ID = :pid AND UserID = :uid")
            result = con.execute(query, {"pid": policy_id, "uid": user_id})
            con.commit()
            if result.rowcount == 0:
                return func.HttpResponse("Policy not found or you do not have permission.", status_code=404)
                
        return func.HttpResponse(f"Policy {policy_id} deleted successfully.", status_code=200)
    except Exception as e:
        return func.HttpResponse(f"Error deleting policy: {e}", status_code=500)
    
    # Add this new function to function_app.py
@app.function_name(name="HttpGetPolicySets")
@app.route(route="my-policy-sets", auth_level=func.AuthLevel.FUNCTION, methods=["get"])
def http_get_policy_sets(req: func.HttpRequest) -> func.HttpResponse:
    user_id = get_user_id(req)
    if not user_id: return func.HttpResponse("Unauthorized", status_code=401)

    # Logic to query the PolicySets table for the given UserID
    # ... return a JSON list of sets
    # Placeholder for now:
    return func.HttpResponse(json.dumps([{"PolicySetID": 1, "SetName": "Q1 2024 Policies"}]))


# Add this new function to function_app.py
@app.function_name(name="HttpGetProductCodes")
@app.route(route="product-codes", auth_level=func.AuthLevel.FUNCTION, methods=["get"])
def http_get_product_codes(req: func.HttpRequest) -> func.HttpResponse:
    user_id = get_user_id(req)
    if not user_id: return func.HttpResponse("Unauthorized", status_code=401)
    
    policy_set_ids_str = req.params.get('setIds') # e.g., "1,2,3"
    
    # Logic to run SELECT DISTINCT Product_Code FROM Policies WHERE UserID = ... AND PolicySetID IN (...)
    # ... return a JSON list of product codes
    # Placeholder for now:
    return func.HttpResponse(json.dumps(["SPDA_G3", "VA_GLWB5"]))