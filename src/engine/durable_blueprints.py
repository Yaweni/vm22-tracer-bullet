import logging
import os
import json
import urllib

import azure.durable_functions as df
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

# --- 1. Create the Durable Functions Blueprint ---
bp = df.Blueprint()

# Helper function specific to this blueprint
def get_sql_engine():
    sql_conn_str = os.environ.get("SqlConnectionString")
    params = urllib.parse.quote_plus(sql_conn_str)
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

# =================================================================
#  DURABLE ORCHESTRATION WORKFLOW
# =================================================================

# --- The STARTER function (now lives in the main app, but we need to register the Orchestrator/Activities here) ---
# This is the "Project Manager" that coordinates the work.
@bp.orchestration_trigger(context_name="context")
def CalculationOrchestrator(context: df.DurableOrchestrationContext):
    job_request = context.get_input()
    user_id = job_request.get("user_id")
    
    try:
        # Step 1: Create the main Job log entry
        job_details = {
            "product_codes": job_request.get("productCodes"),
            "user_id": user_id
        }
        job_id = yield context.call_activity("CreateCalculationJob", job_details)

        # Step 2: Fan-Out/Fan-In pattern to run calculations for multiple products in parallel
        calculation_tasks = []
        for product_code in job_request.get("productCodes", []):
            engine_input = {
                "job_id": job_id,
                "product_code": product_code,
                "user_id": user_id,
                # Pass through other options from the UI
                "scenarioId": job_request.get("scenarioId"),
                "runStochastic": job_request.get("runStochastic")
            }
            calculation_tasks.append(context.call_activity("RunCalculationEngine", engine_input))
        
        # Wait for all parallel calculations to complete
        results = yield context.task_all(calculation_tasks)
        
        # Step 3: Aggregate results and finalize the job
        total_reserve = sum(results)
        yield context.call_activity("SaveFinalResults", {"job_id": job_id, "total_reserve": total_reserve})

        return {"status": "Success", "total_reserve": total_reserve}
        
    except Exception as e:
        # If anything fails, mark the main job as failed
        if 'job_id' in locals():
            yield context.call_activity("UpdateJobStatusToFailed", job_id)
        return {"status": "Failed", "error": str(e)}

# --- The ACTIVITY Functions (The Workers) ---

@bp.activity_trigger(input_name="job_details")
def CreateCalculationJob(job_details: dict) -> int:
    """Activity: Creates the initial job log in SQL, now linked to a user."""
    engine = get_sql_engine()
    product_codes_str = json.dumps(job_details.get("product_codes"))
    user_id = job_details.get("user_id")

    with engine.connect() as con:
        query = text("INSERT INTO CalculationJobs (Product_Code, Job_Status, UserID) OUTPUT INSERTED.JobID VALUES (:pcode, 'Pending', :uid)")
        job_id = con.execute(query, {"pcode": product_codes_str, "uid": user_id}).scalar()
        con.commit()
    return job_id

@bp.activity_trigger(input_name="engine_input")
def RunCalculationEngine(engine_input: dict) -> float:
    """Activity: The main calculation logic for a SINGLE product code."""
    job_id = engine_input['job_id']; 
    product_code = engine_input['product_code']; 
    user_id = engine_input['user_id']
    engine = get_sql_engine()
    
    with engine.connect() as con:
        # This update could be part of a more detailed job tracking table
        con.execute(text("UPDATE CalculationJobs SET Job_Status = 'Running' WHERE JobID = :jobid"), {"jobid": job_id})
        
        policy_query = text("SELECT * FROM Policies WHERE Product_Code = :pcode AND UserID = :uid")
        policies_df = pd.read_sql(policy_query, con, params={"pcode": product_code, "uid": user_id})
        
        scenario_query = text("SELECT * FROM EconomicScenarios WHERE ScenarioSetID = :sid")
        scenarios_df = pd.read_sql(scenario_query, con, params={"sid": engine_input.get("scenarioId")})

        # ... The full, high-performance vectorized calculation logic goes here ...
        # ... This would calculate the reserve just for this product_code ...
        final_reserve_for_product = 123.45 # Placeholder for the complex calculation
        
    return final_reserve_for_product

@bp.activity_trigger(input_name="result_data")
def SaveFinalResults(result_data: dict):
    """Activity: Saves the final aggregated result and marks the job as Complete."""
    job_id = result_data['job_id']; total_reserve = result_data['total_reserve']
    engine = get_sql_engine()
    with engine.connect() as con:
        con.execute(text("INSERT INTO Results (JobID, Result_Type, Result_Value) VALUES (:jobid, 'Aggregated_Reserve', :resval)"), {"jobid": job_id, "resval": total_reserve})
        con.execute(text("UPDATE CalculationJobs SET Job_Status = 'Complete', Completed_Timestamp = GETDATE() WHERE JobID = :jobid"), {"jobid": job_id})
        con.commit()
    logging.info(f"Successfully saved final results for job ID: {job_id}")

@bp.activity_trigger(input_name="job_id")
def UpdateJobStatusToFailed(job_id: int):
    """Activity: Marks a job as Failed in the database."""
    engine = get_sql_engine()
    with engine.connect() as con:
        con.execute(text("UPDATE CalculationJobs SET Job_Status = 'Failed' WHERE JobID = :jobid"), {"jobid": job_id})
        con.commit()
    logging.error(f"Marked job ID: {job_id} as Failed.")