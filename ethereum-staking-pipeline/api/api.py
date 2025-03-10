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
        cursor.execute("""
            SELECT 
                TO_VARCHAR(HOUR_NUM) as HOUR,
                TO_VARCHAR(NUM_TRANSACTIONS) as NUM_TRANSACTIONS,
                TO_VARCHAR(TOTAL_ETH) as TOTAL_ETH,
                TO_VARCHAR(AVG_ETH) as AVG_ETH,
                TO_VARCHAR(TOTAL_GAS_COST) as TOTAL_GAS_COST
            FROM hourly_stats
            LIMIT 24
        """)
        
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        
        if rows:
            stats = [dict(zip(columns, row)) for row in rows]
            return {"hourly_stats": stats}
        else:
            # Generate sample hourly data if none exists
            sample_hours = []
            for hour in range(24):
                sample_hours.append({
                    "HOUR": str(hour),
                    "NUM_TRANSACTIONS": "0",
                    "TOTAL_ETH": "0",
                    "AVG_ETH": "0",
                    "TOTAL_GAS_COST": "0"
                })
            return {"hourly_stats": sample_hours}
    except Exception as e:
        print(f"Error fetching hourly stats: {e}")
        # Return empty data instead of throwing an error
        return {"hourly_stats": []}
    finally:
        conn.close()


@app.get("/metrics/staking")
def get_staking_metrics():
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                TO_VARCHAR(TOTAL_ETH_LAST_24H) as TOTAL_ETH_LAST_24H,
                TO_VARCHAR(TOTAL_TXS_LAST_24H) as TOTAL_TXS_LAST_24H,
                TO_VARCHAR(AVG_ETH_LAST_24H) as AVG_ETH_LAST_24H,
                TO_VARCHAR(TOTAL_ETH_LAST_7D) as TOTAL_ETH_LAST_7D,
                TO_VARCHAR(TOTAL_TXS_LAST_7D) as TOTAL_TXS_LAST_7D,
                TO_VARCHAR(AVG_ETH_LAST_7D) as AVG_ETH_LAST_7D,
                TO_VARCHAR(TOTAL_ETH_ALL_TIME) as TOTAL_ETH_ALL_TIME,
                TO_VARCHAR(TOTAL_TXS_ALL_TIME) as TOTAL_TXS_ALL_TIME,
                TO_VARCHAR(AVG_ETH_ALL_TIME) as AVG_ETH_ALL_TIME,
                TO_VARCHAR(TOTAL_ETH_LAST_HOUR) as TOTAL_ETH_LAST_HOUR,
                TO_VARCHAR(TOTAL_TXS_LAST_HOUR) as TOTAL_TXS_LAST_HOUR,
                TO_VARCHAR(CALCULATED_AT) as CALCULATED_AT
            FROM staking_metrics
        """)
        
        columns = [col[0] for col in cursor.description]
        result = cursor.fetchone()
        
        if result:
            metrics = dict(zip(columns, result))
            # Debug: Print the exact metrics being returned
            print("DEBUG - Metrics from Snowflake:")
            for key, value in metrics.items():
                print(f"  {key}: {value}")
        else:
            # Return default values if no data is found
            metrics = {
                "TOTAL_ETH_LAST_24H": "0",
                "TOTAL_TXS_LAST_24H": "0",
                "AVG_ETH_LAST_24H": "0",
                "TOTAL_ETH_LAST_7D": "0",
                "TOTAL_TXS_LAST_7D": "0",
                "AVG_ETH_LAST_7D": "0",
                "TOTAL_ETH_ALL_TIME": "0",
                "TOTAL_TXS_ALL_TIME": "0",
                "AVG_ETH_ALL_TIME": "0",
                "TOTAL_ETH_LAST_HOUR": "0",
                "TOTAL_TXS_LAST_HOUR": "0",
                "CALCULATED_AT": str(datetime.now())
            }
            
        return {"metrics": metrics}
    except Exception as e:
        print(f"Error fetching staking metrics: {e}")
        # Return default values in case of error
        return {
            "metrics": {
                "TOTAL_ETH_LAST_24H": "0",
                "TOTAL_TXS_LAST_24H": "0",
                "AVG_ETH_LAST_24H": "0",
                "TOTAL_ETH_LAST_7D": "0",
                "TOTAL_TXS_LAST_7D": "0",
                "AVG_ETH_LAST_7D": "0",
                "TOTAL_ETH_ALL_TIME": "0",
                "TOTAL_TXS_ALL_TIME": "0",
                "AVG_ETH_ALL_TIME": "0",
                "TOTAL_TXS_LAST_HOUR": "0",
                "TOTAL_ETH_LAST_HOUR": "0",
                "CALCULATED_AT": str(datetime.now())
            }
        }
    finally:
        conn.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)