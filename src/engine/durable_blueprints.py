import logging
import urllib.parse
import azure.functions as func
import azure.durable_functions as df
from sqlalchemy import create_engine,text
import urllib
import os
import pandas as pd
import numpy as np
# --- 1. Import the Durable Functions Blueprint ---


bp = df.Blueprint(http_auth_level=func.AuthLevel.ANONYMOUS)


@bp.function_name("HttpStartOrchestrator")
@bp.route(route="calculate", methods=["POST"])
@bp.durable_client_input(client_name="client",connection_name="AzureWebJobsStorage")
def http_start_orchestrator(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    """
    The user's entry point. It now receives a complex JSON body describing the job.
    """
    logging.info('Complex calculation request received.')
    
    try:
        # The UI will now send a JSON body with all the options
        job_request = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON request body.", status_code=400)

    # --- Extract all the new options from the request ---
    product_codes = job_request.get("product_codes") # This is now a list
    calculate_stochastic = job_request.get("calculate_stochastic", False) # Default to False
    perform_attribution = job_request.get("perform_attribution", False) # Default to False
    assumptions_text = job_request.get("assumptions_text") # The text from the user
    # assumption_file would be handled here too if a file is uploaded

    if not product_codes or not isinstance(product_codes, list):
        return func.HttpResponse("Please provide a list of 'product_codes'.", status_code=400)

    # The Orchestrator input is now much richer
    orchestrator_input = {
        "product_codes": product_codes,
        "calculate_stochastic": calculate_stochastic,
        "perform_attribution": perform_attribution,
        "assumptions_text": assumptions_text
    }
    
    # Start the orchestration
    instance_id = client.start_new("CalculationOrchestrator", client_input=orchestrator_input)
    logging.info(f"Started multi-product orchestration with ID = '{instance_id}'.")
    
    # Return the status check response to the UI
    return client.create_check_status_response(req, instance_id)

# --- 2b. The "ORCHESTRATOR" Function (The Project Manager) ---
@bp.orchestration_trigger(context_name="context")
def CalculationOrchestrator(context):
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

@bp.activity_trigger(input_name="product_code")
def CreateCalculationJob(product_code: str) -> int:
    """Activity: Creates the initial job log in SQL."""
    sql_connection_string=os.environ.get("SqlConnectionString")
    params = urllib.parse.quote_plus(sql_connection_string)
    sql_engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    with sql_engine.connect() as con:
        job_id = con.execute(text("INSERT INTO CalculationJobs (Product_Code, Job_Status) OUTPUT INSERTED.JobID VALUES (:pcode, 'Pending')"), {"pcode": product_code}).scalar()
        con.commit()
    return job_id

@bp.activity_trigger(input_name="job_data")
def RunCalculationEngine(job_data: dict) -> float:
    """Activity: The main, high-performance calculation logic."""
    job_id = job_data['job_id']
    product_code = job_data['product_code']
    sql_connection_string=os.environ.get("SqlConnectionString")
    params = urllib.parse.quote_plus(sql_connection_string)
    sql_engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    
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

@bp.activity_trigger(input_name="result_data")
def SaveResults(result_data: dict):
    """Activity: Saves the final result and updates the job status to Complete."""
    job_id = result_data['job_id']
    final_reserve = result_data['reserve']
    sql_connection_string=os.environ.get("SqlConnectionString")
    params = urllib.parse.quote_plus(sql_connection_string)
    sql_engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    with sql_engine.connect() as con:
        con.execute(text("INSERT INTO Results (JobID, Result_Type, Result_Value) VALUES (:jobid, 'Deterministic_Reserve_Monthly', :resval)"), {"jobid": job_id, "resval": final_reserve})
        con.execute(text("UPDATE CalculationJobs SET Job_Status = 'Complete', Completed_Timestamp = GETDATE() WHERE JobID = :jobid"), {"jobid": job_id})
        con.commit()
    logging.info(f"Successfully saved results for job ID: {job_id}")

@bp.activity_trigger(input_name="job_id")
def UpdateJobStatusToFailed(job_id: int):
    """Activity: Marks a job as Failed in the database."""
    sql_connection_string=os.environ.get("SqlConnectionString")
    params = urllib.parse.quote_plus(sql_connection_string)
    sql_engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    with sql_engine.connect() as con:
        con.execute(text("UPDATE CalculationJobs SET Job_Status = 'Failed' WHERE JobID = :jobid"), {"jobid": job_id})
        con.commit()
    logging.error(f"Marked job ID: {job_id} as Failed.")