import logging, pandas as pd, io, os, json
import azure.functions as func
from azure.storage.blob import BlobServiceClient

# For the tracer, we hardcode these. For a real app, they'd be dynamic.
STORAGE_CONNECTION_STRING = os.environ.get("UPLOADS_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "uploads"
BLOB_NAME = "tracer_policies.csv"

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    try:
        blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=BLOB_NAME)
        
        df = pd.read_csv(io.BytesIO(blob_client.download_blob().readall()))
        
        df['reserve'] = df['Account_Value'] * 1.03
        total_reserve = df['reserve'].sum()

        response_data = { "total_reserve": total_reserve }
        
        return func.HttpResponse(
            body=json.dumps(response_data),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        logging.error(f"Error: {e}")
        return func.HttpResponse("Error processing request.", status_code=500)