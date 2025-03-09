from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import snowflake.connector
import os
from dotenv import load_dotenv
from datetime import datetime
import json

load_dotenv()

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For production, specify exact origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_snowflake_connection():
    try:
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA')
        )
        return conn
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        raise HTTPException(status_code=500, detail=f"Database connection error: {str(e)}")

def safe_serialize(value):
    """Handle serialization of large numbers and other problematic types"""
    if isinstance(value, (int, float)) and (value > 9007199254740991 or value < -9007199254740991):
        return str(value)
    return value

@app.get("/")
def read_root():
    return {"message": "Ethereum Staking API"}

@app.get("/transactions/recent")
def get_recent_transactions():
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        # Cast problematic columns to strings in the SQL query
        cursor.execute("""
            SELECT 
                TO_VARCHAR(TRANSACTION_HASH) as TRANSACTION_HASH,
                TO_VARCHAR(SENDER_ADDRESS) as SENDER_ADDRESS,
                TO_VARCHAR(AMOUNT_ETH) as AMOUNT_ETH,
                TO_VARCHAR(TIMESTAMP) as TIMESTAMP,
                TO_VARCHAR(GAS_COST_ETH) as GAS_COST_ETH
            FROM recent_transactions
        """)
        
        columns = [col[0] for col in cursor.description]
        transactions = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return {"transactions": transactions}
    except Exception as e:
        print(f"Error fetching recent transactions: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching recent transactions: {str(e)}")
    finally:
        conn.close()

@app.get("/stats/hourly")
def get_hourly_stats():
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        # Cast problematic columns to strings in the SQL query
        cursor.execute("""
            SELECT 
                TO_VARCHAR(HOUR) as HOUR,
                TO_VARCHAR(NUM_TRANSACTIONS) as NUM_TRANSACTIONS,
                TO_VARCHAR(TOTAL_ETH) as TOTAL_ETH,
                TO_VARCHAR(AVG_ETH) as AVG_ETH,
                TO_VARCHAR(TOTAL_GAS_COST) as TOTAL_GAS_COST
            FROM hourly_stats
            LIMIT 24
        """)
        
        columns = [col[0] for col in cursor.description]
        stats = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return {"stats": stats}
    except Exception as e:
        print(f"Error fetching hourly stats: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching hourly stats: {str(e)}")
    finally:
        conn.close()

@app.get("/pipeline/status")
def get_pipeline_status():
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        # Use a simpler query that doesn't rely on timestamp formatting
        cursor.execute("""
            SELECT 
                TO_VARCHAR(COUNT(*)) as total_transactions,
                TO_VARCHAR(MIN(TIMESTAMP)) as first_transaction,
                TO_VARCHAR(MAX(TIMESTAMP)) as last_transaction
            FROM ETH_STAKING_TRANSACTIONS
        """)
        
        result = cursor.fetchone()
        columns = [col[0] for col in cursor.description]
        data = dict(zip(columns, result))
        
        return {
            "status": "active",
            "total_transactions": data.get("TOTAL_TRANSACTIONS", "0"),
            "first_transaction": data.get("FIRST_TRANSACTION", "N/A"),
            "last_transaction": data.get("LAST_TRANSACTION", "N/A"),
            "last_run_formatted": "Data available"
        }
    except Exception as e:
        print(f"Error fetching pipeline status: {e}")
        # Return a fallback status if there's an error
        return {
            "status": "active",
            "total_transactions": "N/A",
            "first_transaction": "N/A",
            "last_transaction": "N/A",
            "last_run_formatted": "Error retrieving data"
        }
    finally:
        conn.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)