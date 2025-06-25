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
from durable_blueprints import bp

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)
app.register_functions(bp)

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
