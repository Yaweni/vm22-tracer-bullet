# src/engine/function_app.py
import logging
import pandas as pd
import io
import os
import json
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine,text
import urllib


# --- Hardcoded values for the Tracer Bullet ---
CONNECTION_STRING = os.environ.get("AzureWebJobsStorage")
CONTAINER_NAME = "uploads"
BLOB_NAME = "tracer_policies.csv"

app = func.FunctionApp()

@app.function_name(name="RunCalculation")
@app.route(route="calculate/{product_code}", auth_level=func.AuthLevel.ANONYMOUS, methods=["post"])
def run_calculation(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python Monthly Calculation Engine triggered.')

    product_code = req.route_params.get('product_code')
    sql_connection_string = os.environ.get("SqlConnectionString")
    
    if not product_code or not sql_connection_string:
        return func.HttpResponse("Missing product_code or SQL connection string.", status_code=400)

    try:
        params = urllib.parse.quote_plus(sql_connection_string)
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

        with engine.connect() as con:
            # --- Setup and Data Loading ---
            job_id = con.execute(text("INSERT INTO CalculationJobs (Product_Code, Job_Status) OUTPUT INSERTED.JobID VALUES (:pcode, 'Running')"), {"pcode": product_code}).scalar()
            con.commit()

            policies_df = pd.read_sql(text("SELECT * FROM Policies WHERE Product_Code = :pcode"), con, params={"pcode": product_code})
            scenarios_df = pd.read_sql(text("SELECT * FROM EconomicScenarios"), con)
            
            if policies_df.empty:
                return func.HttpResponse(f"No policies found for Product_Code: {product_code}", status_code=404)

            # --- Monthly Deterministic Reserve (DR) Calculation ---
            dr_scenario = scenarios_df[scenarios_df['ScenarioID'] == 1].copy()

            PROJECTION_MONTHS = 360
            MONTHLY_LAPSE_RATE = 0.05 / 12
            MONTHLY_ACCOUNT_GROWTH = 0.03 / 12
            
            total_present_value_of_claims = 0

            # Loop through each policy in the block
            for index, policy in policies_df.iterrows():
                current_av = policy['Account_Value']
                
                # =================================================================
                #  CORRECTED DISCOUNTING LOGIC STARTS HERE
                # =================================================================
                
                # This variable will accumulate the discount factor over time
                cumulative_discount_factor = 1.0 
                
                # Project cash flows for this single policy, month by month
                for month in range(1, PROJECTION_MONTHS + 1):
                    # 1. Get the monthly discount rate for THIS specific month
                    # We'll use the 10-year rate, converted to a monthly effective rate
                    annual_rate = dr_scenario.loc[dr_scenario['Month'] == month, 'Rate_10_yr'].iloc[0]
                    monthly_discount_rate = (1 + annual_rate)**(1/12) - 1
                    
                    # 2. Update the cumulative discount factor for this month
                    # This is the key: we compound it period by period
                    cumulative_discount_factor = cumulative_discount_factor / (1 + monthly_discount_rate)
                    
                    # 3. Calculate cash flows for this month
                    account_growth = current_av * MONTHLY_ACCOUNT_GROWTH
                    surrender_payout = current_av * MONTHLY_LAPSE_RATE
                    
                    # 4. Discount this month's payout back to today using the CUMULATIVE factor
                    pv_of_payout = surrender_payout * cumulative_discount_factor
                    total_present_value_of_claims += pv_of_payout
                    
                    # 5. Roll forward the account value for the next month
                    current_av = (current_av + account_growth) * (1 - MONTHLY_LAPSE_RATE)

                # =================================================================
                #  CORRECTED DISCOUNTING LOGIC ENDS HERE
                # =================================================================

            final_reserve = total_present_value_of_claims

            # --- Save Results and Finalize Job ---
            # (The rest of the function remains the same)
            con.execute(text("INSERT INTO Results (JobID, Result_Type, Result_Value) VALUES (:jobid, 'Deterministic_Reserve_Monthly', :resval)"), {"jobid": job_id, "resval": final_reserve})
            con.execute(text("UPDATE CalculationJobs SET Job_Status = 'Complete', Completed_Timestamp = GETDATE() WHERE JobID = :jobid"), {"jobid": job_id})
            con.commit()

        # --- Prepare final response ---
        response_data = {
            "job_id": job_id, "product_code": product_code,
            "policies_processed": len(policies_df),
            "calculated_reserve_dr_monthly": round(final_reserve, 2)
        }
        
        return func.HttpResponse(body=json.dumps(response_data), mimetype="application/json", status_code=200)

    except Exception as e:
        logging.error(f"Error during calculation for {product_code}: {e}")
        return func.HttpResponse(f"Error during calculation: {e}", status_code=500)

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