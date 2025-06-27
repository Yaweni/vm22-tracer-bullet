import logging
import os
import json
import urllib
import io
from datetime import datetime, timedelta

import azure.functions as func
import azure.durable_functions as df
import pandas as pd
from sqlalchemy import create_engine, text
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions

# --- 1. Main App and Blueprint Registration ---
# Create the main app object
app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

# Import the durable blueprint and register it with the main app
from durable_blueprints import bp
app.register_functions(bp)


# --- 2. Helper Functions ---
def get_user_id(req: func.HttpRequest) -> str:
    """Safely extracts the user's unique ID from validated request headers."""
    return req.headers.get("x-ms-client-principal-id")

def get_sql_engine():
    """Creates a reusable, secure SQL engine connection."""
    sql_conn_str = os.environ.get("SqlConnectionString")
    params = urllib.parse.quote_plus(sql_conn_str)
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

def provision_user_if_not_exists(user_id: str, req: func.HttpRequest, con):
    """Checks for a user and creates them on their first API call (JIT Provisioning)."""
    user_check_query = text("SELECT COUNT(1) FROM Users WHERE UserID = :uid")
    user_exists = con.execute(user_check_query, {"uid": user_id}).scalar()
    
    if not user_exists:
        logging.info(f"New user detected. Provisioning user ID: {user_id}")
        user_email = req.headers.get("x-ms-client-principal-name")
        display_name = req.headers.get("x-ms-client-principal-name") # Simple default
        idp = req.headers.get("x-ms-client-principal-provider")
        
        provision_query = text("INSERT INTO Users (UserID, IdentityProvider, Email, DisplayName) VALUES (:uid, :idp, :email, :name)")
        con.execute(provision_query, {"uid": user_id, "idp": idp, "email": user_email, "name": display_name})
        con.commit()

# =================================================================
#  SECTION 1: DATA MANAGEMENT API
# =================================================================

@app.function_name(name="HttpListPolicySets")
@app.route(route="policy-sets", methods=["GET"])
def http_list_policy_sets(req: func.HttpRequest) -> func.HttpResponse:
    user_id = get_user_id(req)
    if not user_id: return func.HttpResponse("Unauthorized.", status_code=401)
    
    try:
        engine = get_sql_engine()
        with engine.connect() as con:
            query = text("SELECT PolicySetID as id, SetName as name, RecordCount, UploadTimestamp as createdAt FROM PolicySets WHERE UserID = :uid ORDER BY UploadTimestamp DESC")
            df = pd.read_sql(query, con, params={"uid": user_id})
        return func.HttpResponse(df.to_json(orient='records', date_format='iso'), mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(f"Error fetching policy sets: {e}", status_code=500)


@app.function_name(name="HttpGetUploadUrlForPolicies")
@app.route(route="policy-sets/get-upload-url", methods=["GET"])
def http_get_upload_url_for_policies(req: func.HttpRequest) -> func.HttpResponse:
    user_id = get_user_id(req)
    if not user_id: return func.HttpResponse("Unauthorized.", status_code=401)
    
    file_name = req.params.get('fileName')
    if not file_name: return func.HttpResponse("fileName query parameter is required.", status_code=400)

    try:
        storage_conn_str = os.environ["AzureWebJobsStorage"]
        container_name = "raw-uploads"
        blob_name = f"{user_id}/policies/{datetime.now().strftime('%Y%m%d%H%M%S')}_{file_name}"

        sas_token = generate_blob_sas(
            account_name=BlobServiceClient.from_connection_string(storage_conn_str).account_name,
            container_name=container_name,
            blob_name=blob_name,
            account_key=BlobServiceClient.from_connection_string(storage_conn_str).credential.account_key,
            permission=BlobSasPermissions(create=True, write=True, read=True),
            expiry=datetime.now() + timedelta(hours=1)
        )
        upload_url = f"https://{BlobServiceClient.from_connection_string(storage_conn_str).account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"
        
        return func.HttpResponse(json.dumps({"uploadUrl": upload_url}), mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(f"Error generating upload URL: {e}", status_code=500)


@app.function_name(name="HttpGetPolicies")
@app.route(route="policies", methods=["GET"])
def http_get_policies(req: func.HttpRequest) -> func.HttpResponse:
    user_id = get_user_id(req); set_id = req.params.get('setId')
    if not user_id: return func.HttpResponse("Unauthorized.", status_code=401)
    if not set_id: return func.HttpResponse("setId query parameter is required.", status_code=400)
    
    try:
        engine = get_sql_engine()
        with engine.connect() as con:
            query = text("SELECT Policy_ID as id, * FROM Policies WHERE UserID = :uid AND PolicySetID = :sid")
            df = pd.read_sql(query, con, params={"uid": user_id, "sid": set_id})
        return func.HttpResponse(df.to_json(orient='records'), mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(f"Error fetching policies: {e}", status_code=500)



@app.function_name(name="HttpListScenarioSets")
@app.route(route="scenario-sets", methods=["GET"])
def http_list_scenario_sets(req: func.HttpRequest) -> func.HttpResponse:
    """Fetches a list of all economic scenario sets the logged-in user has uploaded."""
    user_id = get_user_id(req)
    if not user_id: return func.HttpResponse("Unauthorized.", status_code=401)
    
    try:
        engine = get_sql_engine()
        with engine.connect() as con:
            query = text("SELECT ScenarioSetID as id, SetName as name, Granularity, CreatedTimestamp as createdAt FROM ScenarioSets WHERE UserID = :uid ORDER BY CreatedTimestamp DESC")
            sets_df = pd.read_sql(query, con, params={"uid": user_id})
        
        return func.HttpResponse(sets_df.to_json(orient='records', date_format='iso'), mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(f"Error fetching scenario sets: {e}", status_code=500)

# --- Endpoint 2: Get a secure upload URL for a new Scenario file ---

@app.function_name(name="HttpGetUploadUrlForScenarios")
@app.route(route="scenario-sets/get-upload-url", methods=["GET"])
def http_get_upload_url_for_scenarios(req: func.HttpRequest) -> func.HttpResponse:
    """Provides a secure, temporary URL for the frontend to upload a scenario file directly to Blob Storage."""
    user_id = get_user_id(req)
    if not user_id: return func.HttpResponse("Unauthorized.", status_code=401)
    
    file_name = req.params.get('fileName')
    if not file_name: return func.HttpResponse("fileName query parameter is required.", status_code=400)

    try:
        storage_conn_str = os.environ["AzureWebJobsStorage"]
        container_name = "raw-uploads"
        # We use a different subfolder for organization
        blob_name = blob_name = f"{user_id}/scenarios/{datetime.now().strftime('%Y%m%d%H%M%S')}_{file_name}"

        sas_token = generate_blob_sas(
            account_name=BlobServiceClient.from_connection_string(storage_conn_str).account_name,
            container_name=container_name,
            blob_name=blob_name,
            account_key=BlobServiceClient.from_connection_string(storage_conn_str).credential.account_key,
            permission=BlobSasPermissions(create=True, write=True, read=True),
            expiry=datetime.now() + timedelta(hours=1)
        )
        upload_url = f"https://{BlobServiceClient.from_connection_string(storage_conn_str).account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"
        
        return func.HttpResponse(json.dumps({"uploadUrl": upload_url}), mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(f"Error generating upload URL: {e}", status_code=500)

# --- Endpoint 3: Update a single Policy row ---
# This is a more robust version of the placeholder from before.

@app.function_name(name="HttpUpdatePolicy")
@app.route(route="policies/update", auth_level=func.AuthLevel.FUNCTION, methods=["POST"])
def http_update_policy(req: func.HttpRequest) -> func.HttpResponse:
    """Updates one or more fields for a single policy record."""
    user_id = get_user_id(req)
    if not user_id: return func.HttpResponse("Unauthorized", status_code=401)
    
    try:
        policy_data = req.get_json()
        policy_id = policy_data.get("id") # The frontend sends 'id', which is our Policy_ID
        
        if not policy_id:
            return func.HttpResponse("Missing required field: 'id'", status_code=400)
        
        # Build the dynamic part of the UPDATE statement
        # This makes the function flexible to update any column.
        update_fields = []
        params = {"pid": policy_id, "uid": user_id}
        
        # Iterate over allowed columns to prevent SQL injection
        allowed_columns = ["Account_Value", "Issue_Age", "Gender"] # Add more editable columns here
        for col in allowed_columns:
            if col in policy_data:
                update_fields.append(f"{col} = :{col.lower()}")
                params[col.lower()] = policy_data[col]

        if not update_fields:
            return func.HttpResponse("No valid fields to update were provided.", status_code=400)
            
        set_clause = ", ".join(update_fields)
        
        engine = get_sql_engine()
        with engine.connect() as con:
            # Note the WHERE clause includes UserID for security
            query = text(f"UPDATE Policies SET {set_clause} WHERE Policy_ID = :pid AND UserID = :uid")
            result = con.execute(query, params)
            con.commit()
            
            if result.rowcount == 0:
                return func.HttpResponse("Policy not found or you do not have permission.", status_code=404)

        return func.HttpResponse(f"Policy {policy_id} updated successfully.", status_code=200)
    except Exception as e:
        return func.HttpResponse(f"Error updating policy: {e}", status_code=500)


@app.function_name(name="HttpDeletePolicy")
@app.route(route="policies/{policyId}", auth_level=func.AuthLevel.FUNCTION, methods=["DELETE"])
def http_delete_policy(req: func.HttpRequest) -> func.HttpResponse:
    """Deletes a single policy record."""
    user_id = get_user_id(req)
    if not user_id: return func.HttpResponse("Unauthorized", status_code=401)
    
    policy_id = req.route_params.get('policyId')
    
    try:
        engine = get_sql_engine()
        with engine.connect() as con:
            # The WHERE clause ensures a user can only delete their own policies
            query = text("DELETE FROM Policies WHERE Policy_ID = :pid AND UserID = :uid")
            result = con.execute(query, {"pid": policy_id, "uid": user_id})
            con.commit()

            if result.rowcount == 0:
                return func.HttpResponse("Policy not found or you do not have permission.", status_code=404)
                
        return func.HttpResponse(f"Policy {policy_id} deleted successfully.", status_code=204) # 204 No Content is best for DELETE
    except Exception as e:
        return func.HttpResponse(f"Error deleting policy: {e}", status_code=500)

# =================================================================
#  SECTION 2: CALCULATION LAB & JOB HISTORY API
# =================================================================

@app.function_name(name="HttpGetProductCodesForSets")
@app.route(route="product-codes", methods=["GET"])
def http_get_product_codes_for_sets(req: func.HttpRequest) -> func.HttpResponse:
    user_id = get_user_id(req)
    if not user_id: return func.HttpResponse("Unauthorized.", status_code=401)
    
    set_ids_str = req.params.get('setIds')
    if not set_ids_str: return func.HttpResponse(json.dumps([]), mimetype="application/json") # Return empty list if no sets selected
    
    try:
        set_ids = [int(s_id) for s_id in set_ids_str.split(',')]
        engine = get_sql_engine()
        with engine.connect() as con:
            placeholders = ','.join([f':id{i}' for i in range(len(set_ids))])
            params = {'uid': user_id, **{f'id{i}': s_id for i, s_id in enumerate(set_ids)}}
            query = text(f"SELECT DISTINCT Product_Code FROM Policies WHERE UserID = :uid AND PolicySetID IN ({placeholders})")
            df = pd.read_sql(query, con, params=params)
        return func.HttpResponse(json.dumps(df['Product_Code'].tolist()), mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(f"Error fetching product codes: {e}", status_code=500)

@app.function_name(name="HttpGetJobs")
@app.route(route="jobs", methods=["GET"])
def http_get_jobs(req: func.HttpRequest) -> func.HttpResponse:
    user_id = get_user_id(req)
    if not user_id: return func.HttpResponse("Unauthorized.", status_code=401)
    
    try:
        engine = get_sql_engine()
        with engine.connect() as con:
            provision_user_if_not_exists(user_id, req, con)
            query = text("SELECT JobID as jobId, Job_Status as status, Requested_Timestamp as requestedTimestamp FROM CalculationJobs WHERE UserID = :uid ORDER BY JobID DESC")
            df = pd.read_sql(query, con, params={"uid": user_id})
        return func.HttpResponse(df.to_json(orient='records', date_format='iso'), mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(f"Error fetching job history: {e}", status_code=500)


@app.function_name(name="HttpGetJobResults")
@app.route(route="jobs/{jobId}/results", methods=["GET"])
def http_get_job_results(req: func.HttpRequest) -> func.HttpResponse:
    """
    This function retrieves the final calculated results and generates
    an AI narrative for a specific, completed job.
    """
    user_id = get_user_id(req)
    if not user_id: return func.HttpResponse("Unauthorized.", status_code=401)

    job_id = req.route_params.get('jobId')
    if not job_id: return func.HttpResponse("jobId is required.", status_code=400)

    try:
        engine = get_sql_engine()
        with engine.connect() as con:
            # --- Security Check: Verify this job belongs to the logged-in user ---
            job_check_query = text("SELECT Product_Code FROM CalculationJobs WHERE JobID = :jid AND UserID = :uid")
            job_info = con.execute(job_check_query, {"jid": job_id, "uid": user_id}).fetchone()
            
            if not job_info:
                return func.HttpResponse("Job not found or you do not have permission to view it.", status_code=404)
            
            # --- Fetch the numerical results from the Results table ---
            results_query = text("SELECT Result_Type, Result_Value FROM Results WHERE JobID = :jid")
            results_df = pd.read_sql(results_query, con, params={"jid": job_id})
            
            # Convert the results dataframe into a simple dictionary
            numerical_results = dict(zip(results_df.Result_Type, results_df.Result_Value))

        # --- AI Narrative Generation (Placeholder for the real call) ---
        # In a real implementation, you would send the 'numerical_results' object
        # to Azure OpenAI with a detailed prompt.
        ai_narrative = (
            f"Analysis for Job ID {job_id} on product(s) {job_info[0]}:\n"
            f"The primary calculation, Deterministic_Reserve_Monthly, resulted in a value of ${numerical_results.get('Aggregated_Reserve', 0):,.2f}. "
            "Further analysis of stochastic scenarios and assumption attribution is recommended."
        )

        # --- Assemble the final report payload ---
        final_report = {
            "aiNarrative": ai_narrative,
            "numericalResults": numerical_results
        }
        
        return func.HttpResponse(json.dumps(final_report), mimetype="application/json")

    except Exception as e:
        logging.error(f"Error fetching results for job {job_id}: {e}", exc_info=True)
        return func.HttpResponse(f"Error fetching results: {e}", status_code=500)


@app.function_name(name="HttpGetJobEmbedToken")
@app.route(route="jobs/{jobId}/embed-token", methods=["GET"])
def http_get_job_embed_token(req: func.HttpRequest) -> func.HttpResponse:
    user_id = get_user_id(req)
    if not user_id: return func.HttpResponse("Unauthorized.", status_code=401)
    job_id = req.route_params.get('jobId')
    
    # Placeholder logic
    return func.HttpResponse(json.dumps({
        "accessToken": f"FAKE_TOKEN_FOR_JOB_{job_id}",
        "embedUrl": "https://app.powerbi.com/reportEmbed?reportId=FAKE_REPORT_ID",
        "id": "FAKE_REPORT_ID"
    }), mimetype="application/json")



@app.function_name(name="BlobIngestPolicies")
@app.blob_trigger(
    arg_name="blob",
    path="raw-uploads/{user_id}/policies/{name}",
    connection="AzureWebJobsStorage"
)
def blob_ingest_policies(blob: func.InputStream):
    """
    This function is triggered by a new blob in the uploads container, for uploading policies.
    It validates the data and loads it into the SQL Policies table.
    """
    logging.info(f"Processing scenario file: {blob.name}")
    logging.info(f"Blob size: {blob.length} Bytes")
    
    # Extract metadata from the blob path, e.g., "userid-123/policies/q1_data.csv"
    parts = blob.name.split('/')
    user_id = parts[1]
    original_file_name = parts[3]

    sql_connection_string = os.environ.get("SqlConnectionString")
    if not sql_connection_string:
        logging.error("FATAL: SqlConnectionString setting is missing.")
        return # Cannot proceed

    try:
        # --- Data Validation & Processing ---
        csv_data = blob.read()
        df = pd.read_csv(io.BytesIO(csv_data))

        # TODO: Implement your data compliance/validation logic here.
        # For now, we assume the file is good.
        # In a real app, you would split good/bad rows and save bad rows to a 'quarantine' container.

        # --- Database Operations ---
        engine = get_sql_engine()
        with engine.connect() as con:
            # 1. Create a new PolicySet record for this upload
            set_insert_query = text("INSERT INTO PolicySets (UserID, SetName, OriginalFileName, RecordCount) OUTPUT INSERTED.PolicySetID VALUES (:uid, :sname, :fname, :rcount)")
            policy_set_id = con.execute(set_insert_query, {
                "uid": user_id,
                "sname": f"Set from {original_file_name}",
                "fname": original_file_name,
                "rcount": len(df)
            }).scalar()
            con.commit()

            
            df['UserID'] = user_id
            df['PolicySetID'] = policy_set_id
            
            # 3. Load the validated data into the master Policies table
            # We use 'append' because we are adding a new set, not replacing all policies.
            df.to_sql('Policies', con=engine, if_exists='append', index=False, chunksize=1000)
        
        logging.info(f"Successfully ingested PolicySetID {policy_set_id} for user {user_id}.")

    except Exception as e:
        logging.error(f"Error processing blob {blob.name}: {e}", exc_info=True)
        


@app.function_name(name="BlobIngestScenarios")
@app.blob_trigger(arg_name="blob", path="raw-uploads/{user_id}/scenarios/{name}", connection="AzureWebJobsStorage")
def blob_ingest_scenarios(blob: func.InputStream):

    """Triggered by a new blob in the 'scenarios' subfolder. Processes scenario files."""

    logging.info(f"Processing scenario file: {blob.name}")
    parts = blob.name.split('/')
    user_id = parts[1]
    original_file_name = parts[3]

    sql_connection_string = os.environ.get("SqlConnectionString")
    if not sql_connection_string:
        logging.error("FATAL: SqlConnectionString setting is missing.")
        return # Cannot proceed
    # --- Data Validation & Processing ---
    csv_data = blob.read()
    df = pd.read_csv(io.BytesIO(csv_data))

    # TODO: Implement your data compliance/validation logic here.
    # For now, we assume the file is good.
    # In a real app, you would split good/bad rows and save bad rows to a 'quarantine' container.

    # --- Database Operations ---
    engine = get_sql_engine()
    with engine.connect() as con:
        # 1. Create a new ScenarioSet record for this upload
        set_insert_query = text("INSERT INTO ScenarioSets (UserID, SetName, OriginalFileName, Granularity) OUTPUT INSERTED.ScenarioSetID VALUES (:uid, :sname, :fname, :gran)")
        scenario_set_id = con.execute(set_insert_query, {
            "uid": user_id,
            "sname": f"Set from {original_file_name}",
            "fname": original_file_name,
            "gran": "Monthly"  # Assuming monthly granularity for now
        }).scalar()
        con.commit()

        df['UserID'] = user_id
        df['ScenarioSetID'] = scenario_set_id
        
        # 3. Load the validated data into the master EconomicScenarios table
        df.to_sql('EconomicScenarios', con=engine, if_exists='append', index=False, chunksize=1000) 