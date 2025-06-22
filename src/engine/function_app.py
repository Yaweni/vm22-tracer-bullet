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
@app.route(route="func-vm22-tracer-engine", auth_level=func.AuthLevel.ANONYMOUS)
@app.route(route="calculate/{product_code}", auth_level=func.AuthLevel.ANONYMOUS, methods=["post"])
def run_calculation(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python Calculation Engine triggered.')

    product_code = req.route_params.get('product_code')
    if not product_code:
        return func.HttpResponse("Provide a product_code in the URL, like /api/calculate/SPDA_G3", status_code=400)

    sql_connection_string = os.environ.get("SqlConnectionString")
    if not sql_connection_string:
        return func.HttpResponse("FATAL: SqlConnectionString setting is missing.", status_code=500)

    try:
        params = urllib.parse.quote_plus(sql_connection_string)
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

        with engine.connect() as con:
            # Create a new job log
            job_insert_query = text("INSERT INTO CalculationJobs (Product_Code, Job_Status) OUTPUT INSERTED.JobID VALUES (:pcode, 'Running')")
            result = con.execute(job_insert_query, {"pcode": product_code})
            job_id = result.scalar()
            con.commit() # Using SQLAlchemy 2.0 style commit

            # Read policy data for the specified block
            policy_query = text("SELECT * FROM Policies WHERE Product_Code = :pcode")
            policies_df = pd.read_sql(policy_query, con, params={"pcode": product_code})

            if policies_df.empty:
                return func.HttpResponse(f"No policies found for Product_Code: {product_code}", status_code=404)

            # --- Simplified Deterministic Reserve (DR) Calculation ---
            PROJECTION_YEARS, DISCOUNT_RATE, LAPSE_RATE = 30, 0.04, 0.05
            total_present_value_of_claims = 0

            for index, policy in policies_df.iterrows():
                current_av = policy['Account_Value']
                for year in range(1, PROJECTION_YEARS + 1):
                    growth = current_av * 0.03
                    payout = current_av * LAPSE_RATE
                    pv_payout = payout / ((1 + DISCOUNT_RATE) ** year)
                    total_present_value_of_claims += pv_payout
                    current_av = (current_av + growth) * (1 - LAPSE_RATE)
            
            final_reserve = total_present_value_of_claims

            # Insert the result and update the job status
            result_insert_query = text("INSERT INTO Results (JobID, Result_Type, Result_Value) VALUES (:jobid, 'Deterministic_Reserve', :resval)")
            con.execute(result_insert_query, {"jobid": job_id, "resval": final_reserve})
            job_update_query = text("UPDATE CalculationJobs SET Job_Status = 'Complete', Completed_Timestamp = GETDATE() WHERE JobID = :jobid")
            con.execute(job_update_query, {"jobid": job_id})
            con.commit()

        response_data = {
            "job_id": job_id,
            "product_code": product_code,
            "policies_processed": len(policies_df),
            "calculated_reserve_dr": round(final_reserve, 2)
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

        with engine.connect() as con:
            con.execute('TRUNCATE TABLE Policies')

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