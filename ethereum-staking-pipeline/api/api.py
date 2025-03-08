from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import snowflake.connector
import os
from dotenv import load_dotenv
import json
from datetime import datetime, timedelta

# Load environment variables
load_dotenv()

app = FastAPI()

# Add CORS middleware to allow requests from your frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For production, specify your Vercel domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_snowflake_connection():
    """Get a connection to Snowflake"""
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE")
    )

@app.get("/")
def read_root():
    return {"message": "Ethereum Staking API"}

@app.get("/recent-transactions")
def get_recent_transactions():
    """Get the 10 most recent transactions"""
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        # Query the dbt model for recent transactions
        cursor.execute("SELECT * FROM RECENT_TRANSACTIONS")
        
        # Convert to list of dictionaries
        columns = [col[0] for col in cursor.description]
        transactions = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return {"transactions": transactions}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/hourly-stats")
def get_hourly_stats():
    """Get hourly statistics"""
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        # Query the dbt model for hourly stats
        cursor.execute("SELECT * FROM HOURLY_STATS LIMIT 24")  # Last 24 hours
        
        # Convert to list of dictionaries
        columns = [col[0] for col in cursor.description]
        stats = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return {"hourly_stats": stats}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/pipeline-status")
def get_pipeline_status():
    """Get the status of the data pipeline"""
    try:
        # Read the metadata file created by Dagster
        metadata_path = "../etl/extract_metadata.json"
        if os.path.exists(metadata_path):
            with open(metadata_path, "r") as f:
                metadata = json.load(f)
                
            # Calculate how long ago the pipeline ran
            last_run = datetime.fromisoformat(metadata["timestamp"])
            time_since_run = datetime.now() - last_run
            
            return {
                "last_run": metadata["timestamp"],
                "status": metadata["status"],
                "minutes_since_last_run": time_since_run.total_seconds() / 60
            }
        else:
            return {"status": "unknown", "message": "No pipeline metadata found"}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
