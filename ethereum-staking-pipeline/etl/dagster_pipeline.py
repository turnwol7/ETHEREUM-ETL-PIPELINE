from dagster import asset, AssetExecutionContext, Definitions, ScheduleDefinition, define_asset_job
import subprocess
import os
import json
from datetime import datetime

@asset
def extract_staking_data(context: AssetExecutionContext):
    """Extract staking data from Etherscan API"""
    context.log.info("Extracting staking data from Etherscan")
    result = subprocess.run(["python", "fetch_data.py"], capture_output=True, text=True)
    context.log.info(result.stdout)
    
    # Save metadata about this run
    metadata = {
        "timestamp": datetime.now().isoformat(),
        "status": "success" if result.returncode == 0 else "failed"
    }
    with open("extract_metadata.json", "w") as f:
        json.dump(metadata, f)
    
    return "staking_transactions.csv"

@asset(deps=[extract_staking_data])
def transform_staking_data(context: AssetExecutionContext):
    """Transform staking data"""
    context.log.info("Transforming staking data")
    result = subprocess.run(["python", "transform_data.py"], capture_output=True, text=True)
    context.log.info(result.stdout)
    return ["transformed_staking_data.csv", "daily_staking_stats.csv"]

@asset(deps=[transform_staking_data])
def load_to_snowflake(context: AssetExecutionContext):
    """Load data to Snowflake"""
    context.log.info("Loading data to Snowflake")
    result = subprocess.run(["python", "load_snowflake.py"], capture_output=True, text=True)
    context.log.info(result.stdout)
    return "Data loaded to Snowflake"

# Define a job that will execute all assets
staking_etl_job = define_asset_job(name="staking_etl_job", selection="*")

# Define a schedule that will run the job every hour
hourly_schedule = ScheduleDefinition(
    job=staking_etl_job,
    cron_schedule="0 * * * *",  # Run every hour
)

# Define the Dagster definitions
defs = Definitions(
    assets=[extract_staking_data, transform_staking_data, load_to_snowflake],
    jobs=[staking_etl_job],
    schedules=[hourly_schedule],
)
