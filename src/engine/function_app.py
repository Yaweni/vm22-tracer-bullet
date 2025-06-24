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

app = func.FunctionApp()





@app.function_name(name="HttpIngest")
@app.route(route="ingest", auth_level=func.AuthLevel.ANONYMOUS, methods=["post"])
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
@app.route(route="ingest-scenarios", auth_level=func.AuthLevel.ANONYMOUS, methods=["post"])
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




@app.function_name(name="HttpStartCalculation")
@app.route(route="calculate/{product_code}", auth_level=func.AuthLevel.ANONYMOUS, methods=["post"])
def http_start_calculation(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP StartCalculation trigger processed a request.')
    
    # In a real app with login, we would get the user ID from the request token.
    # For the MVP, we will hardcode a test user ID.
    user_id = "solo_actuary_01" 
    
    product_code = req.route_params.get('product_code')
    sql_connection_string = os.environ.get("SqlConnectionString")
    # We now use our new, explicit setting name
    queue_connection_string = os.environ.get("QueueConnectionString") 
    queue_name = "calculation-requests"

    if not all([product_code, sql_connection_string, queue_connection_string]):
        return func.HttpResponse("FATAL: Missing required application settings.", status_code=500)

    try:
        params = urllib.parse.quote_plus(sql_connection_string)
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

        with engine.connect() as con:
            # We now log the user ID with the job
            job_id = con.execute(
                text("INSERT INTO CalculationJobs (Product_Code, Job_Status, UserID) OUTPUT INSERTED.JobID VALUES (:pcode, 'Pending', :uid)"),
                {"pcode": product_code, "uid": user_id}
            ).scalar()
            con.commit()

        # Add the userId to the message payload
        message_body = json.dumps({
            "job_id": job_id,
            "product_code": product_code,
            "user_id": user_id 
        })
        
        queue_service_client = QueueServiceClient.from_connection_string(queue_connection_string)
        queue_client = queue_service_client.get_queue_client(queue_name)
        queue_client.send_message(message_body)
        
        logging.info(f"Successfully sent message for Job ID: {job_id} to queue '{queue_name}'.")

        return func.HttpResponse(
            body=json.dumps({"job_id": job_id, "status": "Job successfully queued."}),
            mimetype="application/json",
            status_code=202
        )

    except Exception as e:
        logging.error(f"Error starting calculation for {product_code}: {e}")
        return func.HttpResponse(f"Error queuing job: {e}", status_code=500)





@app.function_name(name="ProcessCalculationJob")
@app.queue_trigger(
    arg_name="job_message",
    connection="QueueConnectionString",
    queue_name="calculation-requests"
)
def process_calculation_job(job_message: func.QueueMessage):
    """
    This function is a background worker. It activates automatically
    when a new message appears in the 'calculation-requests' queue.
    """
    message_body = job_message.get_body().decode('utf-8')
    job_data = json.loads(message_body)
    job_id = job_data['job_id']
    product_code = job_data['product_code']
    user_id = job_data.get('user_id') # We get the user_id but don't use it yet
    
    logging.info(f"Python Queue trigger processing job ID: {job_id} for product: {product_code}")
    

    sql_connection_string = os.environ.get("SqlConnectionString")
    params = urllib.parse.quote_plus(sql_connection_string)
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    try:
        with engine.connect() as con:
            con.execute(text("UPDATE CalculationJobs SET Job_Status = 'Running' WHERE JobID = :jobid"), {"jobid": job_id})
            con.commit()
            policies_df = pd.read_sql(text("SELECT * FROM Policies WHERE Product_Code = :pcode"), con, params={"pcode": product_code})
            scenarios_df = pd.read_sql(text("SELECT * FROM EconomicScenarios"), con)
            dr_scenario = scenarios_df[scenarios_df['ScenarioID'] == 1].copy()
            dr_scenario.set_index('Month', inplace=True)
            PROJECTION_MONTHS, MONTHLY_LAPSE_RATE, MONTHLY_ACCOUNT_GROWTH = 360, 0.05 / 12, (1+0.03)**(1/12) -1
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
            con.execute(text("INSERT INTO Results (JobID, Result_Type, Result_Value) VALUES (:jobid, 'Deterministic_Reserve_Monthly', :resval)"), {"jobid": job_id, "resval": final_reserve})
            con.execute(text("UPDATE CalculationJobs SET Job_Status = 'Complete', Completed_Timestamp = GETDATE() WHERE JobID = :jobid"), {"jobid": job_id})
            con.commit()
        logging.info(f"Successfully completed job ID: {job_id}")
    except Exception as e:
        logging.error(f"Job ID {job_id} failed with error: {e}")
        with engine.connect() as con:
            con.execute(text("UPDATE CalculationJobs SET Job_Status = 'Failed' WHERE JobID = :jobid"), {"jobid": job_id})
            con.commit()

