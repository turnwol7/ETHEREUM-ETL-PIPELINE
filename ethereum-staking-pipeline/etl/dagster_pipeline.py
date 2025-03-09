from dagster import asset, AssetExecutionContext, Definitions, ScheduleDefinition
import os
import subprocess
from dotenv import load_dotenv
import sys
import snowflake.connector

# Import your existing ETL scripts
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from fetch_data import fetch_staking_data
from transform_data import transform_data
from load_snowflake import upload_transactions_to_snowflake, connect_to_snowflake

load_dotenv()

@asset
def extract_ethereum_data(context: AssetExecutionContext):
    """Extract staking transaction data from Etherscan API"""
    context.log.info("Fetching data from Etherscan API")
    transactions = fetch_staking_data()
    context.log.info(f"Fetched {len(transactions)} transactions")
    return transactions

@asset(deps=[extract_ethereum_data])
def transform_ethereum_data(context: AssetExecutionContext, extract_ethereum_data):
    """Transform the raw transaction data"""
    context.log.info("Transforming transaction data")
    transformed_data = transform_data(extract_ethereum_data)
    context.log.info("Transformation complete")
    return transformed_data

@asset(deps=[transform_ethereum_data])
def load_to_snowflake_db(context: AssetExecutionContext, transform_ethereum_data):
    """Load transformed data to Snowflake"""
    context.log.info("Loading data to Snowflake")
    conn = connect_to_snowflake()
    upload_transactions_to_snowflake(conn, transform_ethereum_data)
    context.log.info("Data loaded to Snowflake")
    
    # Update pipeline run timestamp
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS pipeline_runs (run_id INT AUTOINCREMENT, run_time TIMESTAMP_NTZ)")
    cursor.execute("INSERT INTO pipeline_runs (run_time) VALUES (CURRENT_TIMESTAMP())")
    conn.close()
    
    return "Data loaded successfully"

@asset(deps=[load_to_snowflake_db])
def run_dbt_models(context: AssetExecutionContext):
    """Run dbt models to transform data in Snowflake"""
    context.log.info("Running dbt models")
    dbt_project_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dbt")
    result = subprocess.run(
        ["dbt", "run"], 
        cwd=dbt_project_dir,
        capture_output=True,
        text=True
    )
    context.log.info(f"dbt output: {result.stdout}")
    if result.returncode != 0:
        context.log.error(f"dbt error: {result.stderr}")
        raise Exception("dbt run failed")
    return "dbt models run successfully"

# Define a schedule to run the pipeline hourly
hourly_schedule = ScheduleDefinition(
    job_name="ethereum_staking_etl",
    cron_schedule="0 * * * *",  # Run at the start of every hour
    execution_timezone="UTC",
)

# Define the Dagster job
defs = Definitions(
    assets=[extract_ethereum_data, transform_ethereum_data, load_to_snowflake_db, run_dbt_models],
    schedules=[hourly_schedule]
)
