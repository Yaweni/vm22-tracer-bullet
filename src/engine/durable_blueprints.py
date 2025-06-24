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


@bp.route(route="process_policies", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def start_orchestrator(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
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

    instance_id = await client.start_new("CalculationOrchestrator", client_input=orchestrator_input)
    logging.info(f"Started orchestration with ID = '{instance_id}'.")
    
    # This creates the standard HTTP 202 response with location headers
    # for checking the status of the orchestration.
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
    sql_engine = create_engine(urllib.parse.quote_plus(os.environ["SqlConnectionString"]))
    with sql_engine.connect() as con:
        job_id = con.execute(text("INSERT INTO CalculationJobs (Product_Code, Job_Status) OUTPUT INSERTED.JobID VALUES (:pcode, 'Pending')"), {"pcode": product_code}).scalar()
        con.commit()
    return job_id

@bp.activity_trigger(input_name="job_data")
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

@bp.activity_trigger(input_name="result_data")
def SaveResults(result_data: dict):
    """Activity: Saves the final result and updates the job status to Complete."""
    job_id = result_data['job_id']; final_reserve = result_data['reserve']
    sql_engine = create_engine(urllib.parse.quote_plus(os.environ["SqlConnectionString"]))
    with sql_engine.connect() as con:
        con.execute(text("INSERT INTO Results (JobID, Result_Type, Result_Value) VALUES (:jobid, 'Deterministic_Reserve_Monthly', :resval)"), {"jobid": job_id, "resval": final_reserve})
        con.execute(text("UPDATE CalculationJobs SET Job_Status = 'Complete', Completed_Timestamp = GETDATE() WHERE JobID = :jobid"), {"jobid": job_id})
        con.commit()
    logging.info(f"Successfully saved results for job ID: {job_id}")

@bp.activity_trigger(input_name="job_id")
def UpdateJobStatusToFailed(job_id: int):
    """Activity: Marks a job as Failed in the database."""
    sql_engine = create_engine(urllib.parse.quote_plus(os.environ["SqlConnectionString"]))
    with sql_engine.connect() as con:
        con.execute(text("UPDATE CalculationJobs SET Job_Status = 'Failed' WHERE JobID = :jobid"), {"jobid": job_id})
        con.commit()
    logging.error(f"Marked job ID: {job_id} as Failed.")