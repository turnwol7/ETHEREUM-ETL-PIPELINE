from dagster import asset, AssetExecutionContext, Definitions, ScheduleDefinition, define_asset_job
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
    
    # Convert the list to a DataFrame first
    import pandas as pd
    df = pd.DataFrame(extract_ethereum_data)
    
    # Then pass the DataFrame to transform_data
    transformed_data = transform_data(df)
    
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
    cursor.execute("CREATE TABLE IF NOT EXISTS pipeline_runs (run_id INT AUTOINCREMENT, run_time TIMESTAMP_NTZ, records_processed INT)")
    cursor.execute("INSERT INTO pipeline_runs (run_time, records_processed) VALUES (CURRENT_TIMESTAMP(), %s)", (len(transform_ethereum_data),))
    conn.close()
    
    return "Data loaded successfully"

@asset(deps=[load_to_snowflake_db])
def run_dbt_models(context: AssetExecutionContext):
    """Run dbt models to transform data in Snowflake"""
    context.log.info("Running dbt models")
    
    # Update path to point to the eth_staking subdirectory
    dbt_project_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dbt", "eth_staking")
    
    # Debug the path
    context.log.info(f"dbt project directory: {dbt_project_dir}")
    
    # Make sure to set the DBT_PROFILES_DIR environment variable
    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dbt")
    
    result = subprocess.run(
        ["dbt", "run"], 
        cwd=dbt_project_dir,
        capture_output=True,
        text=True,
        env=env  # Use the modified environment
    )
    
    context.log.info(f"dbt output: {result.stdout}")
    if result.returncode != 0:
        context.log.error(f"dbt error: {result.stderr}")
        raise Exception("dbt run failed")
    
    return "dbt models run successfully"

# Create a job from your assets
ethereum_staking_etl = define_asset_job(
    name="ethereum_staking_etl",
    selection="*",  # This selects all assets
    description="Job to run the entire Ethereum staking ETL pipeline"
)

# Define a schedule to run the pipeline every minute for testing
test_schedule = ScheduleDefinition(
    name="ethereum_staking_etl_schedule",
    job=ethereum_staking_etl,
    cron_schedule="*/5 * * * *",  # Run every 5 minutes
    execution_timezone="UTC",
)

# Define the Dagster job
defs = Definitions(
    assets=[extract_ethereum_data, transform_ethereum_data, load_to_snowflake_db, run_dbt_models],
    jobs=[ethereum_staking_etl],  # Include the job in your definitions
    schedules=[test_schedule]
)
