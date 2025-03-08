import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def connect_to_snowflake():
    """Establish connection to Snowflake"""
    try:
        # Get connection parameters from environment variables
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            role=os.getenv("SNOWFLAKE_ROLE")
        )
        print("Connected to Snowflake successfully!")
        return conn
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        return None

def create_tables(conn):
    """Create necessary tables in Snowflake if they don't exist"""
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # Create table for transformed staking data
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS ETH_STAKING_TRANSACTIONS (
            TRANSACTION_HASH VARCHAR(66) PRIMARY KEY,
            SENDER_ADDRESS VARCHAR(42),
            RECEIVER_ADDRESS VARCHAR(42),
            AMOUNT_ETH FLOAT,
            TIMESTAMP TIMESTAMP_NTZ,
            GAS_USED NUMBER,
            GAS_PRICE NUMBER,
            BLOCK_NUMBER NUMBER,
            GAS_COST_ETH FLOAT,
            DATE VARCHAR(10),
            HOUR NUMBER,
            DAY_OF_WEEK VARCHAR(10),
            MONTH VARCHAR(10),
            IS_STANDARD_STAKE BOOLEAN
        )
        """)
        
        # Create table for daily statistics
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS ETH_STAKING_DAILY_STATS (
            DATE VARCHAR(10) PRIMARY KEY,
            NUM_TRANSACTIONS NUMBER,
            TOTAL_ETH_STAKED FLOAT,
            AVG_STAKE_SIZE FLOAT,
            MIN_STAKE_SIZE FLOAT,
            MAX_STAKE_SIZE FLOAT,
            TOTAL_GAS_COST FLOAT,
            AVG_GAS_COST FLOAT,
            NUM_STANDARD_STAKES NUMBER,
            PCT_STANDARD_STAKES FLOAT
        )
        """)
        
        cursor.close()
        print("Tables created successfully!")
        return True
    except Exception as e:
        print(f"Error creating tables: {e}")
        return False

def load_data_from_csv(filename):
    """Load data from CSV file"""
    if not os.path.exists(filename):
        print(f"Error: File {filename} not found")
        return pd.DataFrame()
    
    return pd.read_csv(filename)

def upload_transactions_to_snowflake(conn, df):
    """Upload transformed transaction data to Snowflake"""
    if df.empty or not conn:
        return False
    
    try:
        # Convert date columns to proper format
        if "timestamp" in df.columns and not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
            df["timestamp"] = pd.to_datetime(df["timestamp"])
        
        if "date" in df.columns and not pd.api.types.is_datetime64_any_dtype(df["date"]):
            # Convert to datetime and then to string in YYYY-MM-DD format
            df["date"] = pd.to_datetime(df["date"]).dt.strftime('%Y-%m-%d')
        
        # Convert column names to uppercase for Snowflake
        df.columns = [col.upper() for col in df.columns]
        
        # Print data types for debugging
        print("Column data types:")
        for col in df.columns:
            print(f"  {col}: {df[col].dtype}")
        
        # Write to Snowflake using the more efficient write_pandas method
        success, num_chunks, num_rows, output = write_pandas(
            conn=conn,
            df=df,
            table_name="ETH_STAKING_TRANSACTIONS",
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA")
        )
        
        if success:
            print(f"Successfully uploaded {num_rows} transaction records to Snowflake")
            return True
        else:
            print("Failed to upload transaction data to Snowflake")
            return False
    
    except Exception as e:
        print(f"Error uploading transaction data: {e}")
        return False

def upload_daily_stats_to_snowflake(conn, df):
    """Upload daily statistics to Snowflake"""
    if df.empty or not conn:
        return False
    
    try:
        # Convert date column to proper format
        if "date" in df.columns and not pd.api.types.is_datetime64_any_dtype(df["date"]):
            # Convert to datetime and then to string in YYYY-MM-DD format
            df["date"] = pd.to_datetime(df["date"]).dt.strftime('%Y-%m-%d')
        
        # Convert column names to uppercase for Snowflake
        df.columns = [col.upper() for col in df.columns]
        
        # Print data types for debugging
        print("Column data types:")
        for col in df.columns:
            print(f"  {col}: {df[col].dtype}")
        
        # Write to Snowflake
        success, num_chunks, num_rows, output = write_pandas(
            conn=conn,
            df=df,
            table_name="ETH_STAKING_DAILY_STATS",
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA")
        )
        
        if success:
            print(f"Successfully uploaded {num_rows} daily statistic records to Snowflake")
            return True
        else:
            print("Failed to upload daily statistics to Snowflake")
            return False
    
    except Exception as e:
        print(f"Error uploading daily statistics: {e}")
        return False

def main():
    """Main function to load data into Snowflake"""
    # Check for required environment variables
    required_vars = ["SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_ACCOUNT", 
                     "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA"]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"Error: Missing required environment variables: {', '.join(missing_vars)}")
        print("Please set these variables in your .env file")
        return
    
    # Connect to Snowflake
    conn = connect_to_snowflake()
    if not conn:
        return
    
    # Create tables if they don't exist
    if not create_tables(conn):
        conn.close()
        return
    
    # Load transformed transaction data
    print("Loading transformed transaction data...")
    transactions_df = load_data_from_csv("transformed_staking_data.csv")
    
    if transactions_df.empty:
        print("No transaction data to upload. Run transform_data.py first.")
    else:
        # Upload transaction data to Snowflake
        print("Uploading transaction data to Snowflake...")
        upload_transactions_to_snowflake(conn, transactions_df)
    
    # Load daily statistics data
    print("Loading daily statistics data...")
    stats_df = load_data_from_csv("daily_staking_stats.csv")
    
    if stats_df.empty:
        print("No daily statistics to upload. Run transform_data.py first.")
    else:
        # Upload daily statistics to Snowflake
        print("Uploading daily statistics to Snowflake...")
        upload_daily_stats_to_snowflake(conn, stats_df)
    
    # Close connection
    conn.close()
    print("Data loading to Snowflake complete!")

if __name__ == "__main__":
    main()
