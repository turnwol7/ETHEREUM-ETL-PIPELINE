from dagster import op, job, schedule, Definitions
import subprocess
import os
import sys
import psutil
import logging

def log_memory_usage(context):
    """Log current memory usage."""
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()
    memory_mb = memory_info.rss / 1024 / 1024
    
    context.log.info(f"Current memory usage: {memory_mb:.2f} MB")
    
    # Alert if approaching limit (e.g., 80% of 512MB)
    if memory_mb > 400:
        context.log.warning(f"WARNING: High memory usage: {memory_mb:.2f} MB")

@op
def run_upload_script():
    """Run the upload script that handles the entire ETL pipeline"""
    # Get absolute path to project root
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    script_path = os.path.join(project_root, "upload_script.sh")
    
    print(f"Project root: {project_root}")
    print(f"Script path: {script_path}")
    
    # Make sure the script is executable
    subprocess.run(["chmod", "+x", script_path], check=True)
    
    # Copy current environment and ensure we're using the same Python
    env = os.environ.copy()
    env["PYTHONPATH"] = project_root + ":" + env.get("PYTHONPATH", "")
    
    # Run the upload script from the project root directory
    result = subprocess.run(
        ["/bin/zsh", script_path],  # Use zsh explicitly
        cwd=project_root,  # Set working directory to project root
        capture_output=True,
        text=True,
        env=env  # Pass the modified environment
    )
    
    # Print the output
    print(f"Script output: {result.stdout}")
    
    if result.returncode != 0:
        print(f"Script error: {result.stderr}")
        raise Exception("ETL pipeline failed")
    
    return "ETL pipeline completed successfully"

@op
def memory_intensive_operation(context):
    # Log before operation
    log_memory_usage(context)
    
    # Your operation code here
    result = perform_operation()
    
    # Log after operation
    log_memory_usage(context)
    
    return result

@job
def eth_staking_pipeline():
    """Job to run the Ethereum staking ETL pipeline"""
    run_upload_script()

@schedule(
    job=eth_staking_pipeline,
    cron_schedule="*/30 * * * *",  
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
