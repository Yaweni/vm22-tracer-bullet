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

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)


@app.function_name(name="HttpIngest")
@app.route(route="ingest/policies", auth_level=func.AuthLevel.ANONYMOUS, methods=["post"])
def http_ingest(req: func.HttpRequest) -> func.HttpResponse:
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


@app.function_name(name="HttpIngestScenarios")
@app.route(route="ingest/scenarios", auth_level=func.AuthLevel.ANONYMOUS, methods=["post"])
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




@app.function_name("http_process_policies_orchestrator")
@app.route(route="process_policies",auth_level=func.AuthLevel.ANONYMOUS, methods=["POST"])
@app.durable_client_input(client_name="client")
def http_process_policies_orchestrator(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    """
    The user's entry point. Reads input from the request body,
    starts the orchestration, and returns immediately.
    """
    logging.info("HTTP orchestrator starter trigger processed a request.")

    # --- Get product_code from the request body ---
    try:
        # req.get_json() is a convenient way to parse the body as JSON.
        # It raises a ValueError if the body is not valid JSON.
        req_body = req.get_json()
        product_code = req_body.get('product_code')
    except ValueError:
        logging.error("Invalid JSON received in request body.")
        return func.HttpResponse(
             "Request body must be valid JSON.",
             status_code=400
        )

    # Check if 'product_code' key was present in the JSON
    if not product_code:
        return func.HttpResponse(
             "Please provide 'product_code' in the JSON request body.",
             status_code=400
        )

    # --- Start the orchestration ---
    orchestrator_input = {"product_code": product_code}

    instance_id = client.start_new("CalculationOrchestrator", client_input=orchestrator_input)
    logging.info(f"Started orchestration with ID = '{instance_id}'.")
    
    # This creates the standard HTTP 202 response with location headers
    # for checking the status of the orchestration.
    return client.create_check_status_response(req, instance_id)

# --- 2b. The "ORCHESTRATOR" Function (The Project Manager) ---
@app.orchestration_trigger(context_name="context")
def CalculationOrchestrator(context: df.DurableOrchestrationContext):
    """Manages the workflow steps."""
    input_data = context.get_input()
    product_code = input_data.get("product_code")
    
    try:
        job_id = yield context.call_activity("CreateCalculationJob", product_code)
        final_reserve = yield context.call_activity("RunCalculationEngine", {"job_id": job_id, "product_code": product_code})
        yield context.call_activity("SaveResults", {"job_id": job_id, "reserve": final_reserve})
        return {"status": "Success", "reserve": final_reserve}
    except Exception as e:
        if 'job_id' in locals():
            yield context.call_activity("UpdateJobStatusToFailed", job_id)
        return {"status": "Failed", "error": str(e)}

# --- 2c. The "ACTIVITY" Functions (The Workers) ---

@app.activity_trigger(input_name="product_code")
def CreateCalculationJob(product_code: str) -> int:
    """Activity: Creates the initial job log in SQL."""
    sql_engine = create_engine(urllib.parse.quote_plus(os.environ["SqlConnectionString"]))
    with sql_engine.connect() as con:
        job_id = con.execute(text("INSERT INTO CalculationJobs (Product_Code, Job_Status) OUTPUT INSERTED.JobID VALUES (:pcode, 'Pending')"), {"pcode": product_code}).scalar()
        con.commit()
    return job_id

@app.activity_trigger(input_name="job_data")
def RunCalculationEngine(job_data: dict) -> float:
    """Activity: The main, high-performance calculation logic."""
    job_id = job_data['job_id']
    product_code = job_data['product_code']
    sql_engine = create_engine(urllib.parse.quote_plus(os.environ["SqlConnectionString"]))
    
    with sql_engine.connect() as con:
        con.execute(text("UPDATE CalculationJobs SET Job_Status = 'Running' WHERE JobID = :jobid"), {"jobid": job_id})
        con.commit()
        # (The full vectorized calculation logic goes here as before)
        policies_df = pd.read_sql(text("SELECT * FROM Policies WHERE Product_Code = :pcode"), con, params={"pcode": product_code})
        scenarios_df = pd.read_sql(text("SELECT * FROM EconomicScenarios"), con)
        dr_scenario = scenarios_df[scenarios_df['ScenarioID'] == 1].copy()
        dr_scenario.set_index('Month', inplace=True)
        PROJECTION_MONTHS, MONTHLY_LAPSE_RATE, MONTHLY_ACCOUNT_GROWTH = 360, 0.05 / 12, 0.03 / 12
        num_policies = len(policies_df)
        account_values = policies_df['Account_Value'].to_numpy(dtype=np.float64)
        in_force_mask = np.ones(num_policies, dtype=np.float64)
        total_pv_of_claims = np.zeros(num_policies, dtype=np.float64)
        discount_rates = (1 + dr_scenario['Rate_0_25_yr'].to_numpy())**(1/12) - 1
        cumulative_discount_factors = np.cumprod(1 / (1 + discount_rates))

        for month_idx in range(PROJECTION_MONTHS):
            pv_of_payouts = (account_values * MONTHLY_LAPSE_RATE) * cumulative_discount_factors[month_idx]
            total_pv_of_claims += pv_of_payouts * in_force_mask
            account_values += account_values * MONTHLY_ACCOUNT_GROWTH
            in_force_mask *= (1 - MONTHLY_LAPSE_RATE)
        
        final_reserve = np.sum(total_pv_of_claims)
        return final_reserve

@app.activity_trigger(input_name="result_data")
def SaveResults(result_data: dict):
    """Activity: Saves the final result and updates the job status to Complete."""
    job_id = result_data['job_id']; final_reserve = result_data['reserve']
    sql_engine = create_engine(urllib.parse.quote_plus(os.environ["SqlConnectionString"]))
    with sql_engine.connect() as con:
        con.execute(text("INSERT INTO Results (JobID, Result_Type, Result_Value) VALUES (:jobid, 'Deterministic_Reserve_Monthly', :resval)"), {"jobid": job_id, "resval": final_reserve})
        con.execute(text("UPDATE CalculationJobs SET Job_Status = 'Complete', Completed_Timestamp = GETDATE() WHERE JobID = :jobid"), {"jobid": job_id})
        con.commit()
    logging.info(f"Successfully saved results for job ID: {job_id}")

@app.activity_trigger(input_name="job_id")
def UpdateJobStatusToFailed(job_id: int):
    """Activity: Marks a job as Failed in the database."""
    sql_engine = create_engine(urllib.parse.quote_plus(os.environ["SqlConnectionString"]))
    with sql_engine.connect() as con:
        con.execute(text("UPDATE CalculationJobs SET Job_Status = 'Failed' WHERE JobID = :jobid"), {"jobid": job_id})
        con.commit()
    logging.error(f"Marked job ID: {job_id} as Failed.")