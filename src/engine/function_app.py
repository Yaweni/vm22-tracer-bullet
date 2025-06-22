# src/engine/function_app.py
import logging
import pandas as pd
import io
import os
import json
import azure.functions as func
from azure.storage.blob import BlobServiceClient


# --- Hardcoded values for the Tracer Bullet ---
CONNECTION_STRING = os.environ.get("AzureWebJobsStorage")
CONTAINER_NAME = "uploads"
BLOB_NAME = "tracer_policies.csv"

app = func.FunctionApp()

@app.function_name(name="HttpTrigger1")
@app.route(route="func-vm22-tracer-engine", auth_level=func.AuthLevel.ANONYMOUS)
def run_calculation(req: func.HttpRequest) -> func.HttpResponse:
    """
    This is our main calculation function. The decorators above tell Azure
    how to trigger it.
    """
    logging.info('Python HTTP trigger function (v2 model) processed a request.')

    if not CONNECTION_STRING:
        return func.HttpResponse("FATAL: AzureWebJobsStorage setting is missing.", status_code=500)

    try:
        # 1. Connect to Azure Blob Storage and get the data
        blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=BLOB_NAME)
        
        df = pd.read_csv(io.BytesIO(blob_client.download_blob().readall()))
        
        # 2. Apply the simple, hardcoded calculation
        df['reserve'] = df['Account_Value'] * 1.03
        
        # 3. Sum up the results
        total_reserve = df['reserve'].sum()

        # 4. Prepare the JSON response
        response_data = {
            "total_reserve": total_reserve,
            "policies_processed": len(df),
            "source_file": BLOB_NAME,
            "model_version": "v2"
        }
        
        return func.HttpResponse(
            body=json.dumps(response_data),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        logging.error(f"An error occurred during execution: {e}")
        return func.HttpResponse("Error processing the request. Check the logs.", status_code=500)