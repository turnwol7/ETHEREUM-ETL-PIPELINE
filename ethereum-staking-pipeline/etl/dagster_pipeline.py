from dagster import op, job, schedule, Definitions
import subprocess
import os

@op
def run_upload_script():
    """Run the upload script that handles the entire ETL pipeline"""
    # Get the path to the upload script (assuming it's in the root directory)
    script_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "..", "upload_script.sh")
    
    # Make sure the script is executable
    subprocess.run(["chmod", "+x", script_path], check=True)
    
    # Run the upload script
    result = subprocess.run(
        ["/bin/bash", script_path],
        capture_output=True,
        text=True
    )
    
    # Print the output
    print(f"Script output: {result.stdout}")
    
    if result.returncode != 0:
        print(f"Script error: {result.stderr}")
        raise Exception("ETL pipeline failed")
    
    return "ETL pipeline completed successfully"

@job
def eth_staking_pipeline():
    """Job to run the Ethereum staking ETL pipeline"""
    run_upload_script()

@schedule(
    job=eth_staking_pipeline,
    cron_schedule="*/3 * * * *",  
    execution_timezone="UTC",
)
def eth_staking_schedule():
    """Schedule for the Ethereum staking ETL pipeline"""
    return {}

# Define the Dagster job
defs = Definitions(
    jobs=[eth_staking_pipeline],
    schedules=[eth_staking_schedule]
)

# When running this file directly, start Dagster on port 4000
if __name__ == "__main__":
    from dagster import cli
    cli.main(["ui", "--port", "4000"])
