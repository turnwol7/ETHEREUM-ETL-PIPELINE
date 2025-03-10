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
        
        # Drop and recreate tables with correct schema
        cursor.execute("DROP TABLE IF EXISTS ETH_STAKING_TRANSACTIONS;")
        cursor.execute("DROP VIEW IF EXISTS RECENT_TRANSACTIONS;")
        cursor.execute("DROP VIEW IF EXISTS HOURLY_STATS;")
        cursor.execute("DROP VIEW IF EXISTS STAKING_METRICS;")
        
        # Create the main transactions table with proper timestamp format
        cursor.execute("""
        CREATE TABLE ETH_STAKING_TRANSACTIONS (
            TRANSACTION_HASH VARCHAR(66) PRIMARY KEY,
            SENDER_ADDRESS VARCHAR(42),
            RECEIVER_ADDRESS VARCHAR(42),
            AMOUNT_ETH FLOAT,
            TIMESTAMP_STR VARCHAR(30),
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
        
        # Create table for pipeline runs
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS PIPELINE_RUNS (
            RUN_ID NUMBER IDENTITY(1,1) PRIMARY KEY,
            RUN_TIMESTAMP TIMESTAMP_NTZ,
            STATUS VARCHAR(50),
            DETAILS VARCHAR(1000)
        )
        """)
        
        cursor.close()
        print("Tables created successfully!")
        return True
    except Exception as e:
        print(f"Error creating tables: {e}")
        return False

def truncate_tables(conn):
    """Truncate existing tables to ensure clean data load"""
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # Truncate transaction table
        cursor.execute("TRUNCATE TABLE IF EXISTS ETH_STAKING_TRANSACTIONS")
        
        # Truncate daily stats table
        cursor.execute("TRUNCATE TABLE IF EXISTS ETH_STAKING_DAILY_STATS")
        
        # Truncate pipeline runs table if it exists
        cursor.execute("TRUNCATE TABLE IF EXISTS PIPELINE_RUNS")
        
        print("Successfully truncated existing tables")
        return True
    except Exception as e:
        print(f"Error truncating tables: {e}")
        return False

def load_data_from_csv(filename):
    """Load data from CSV file"""
    # First check if file exists in the current directory
    if os.path.exists(filename):
        print(f"Loading {filename} from current directory")
        return pd.read_csv(filename)
    
    # Then check if file exists in the root directory (two levels up)
    root_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), filename)
    if os.path.exists(root_path):
        print(f"Loading {filename} from root directory: {root_path}")
        return pd.read_csv(root_path)
    
    print(f"Error: File {filename} not found in either current or root directory")
    return pd.DataFrame()

def upload_transactions_to_snowflake(conn, df):
    """Upload transformed transaction data to Snowflake"""
    if df.empty or not conn:
        return False
    
    try:
        # Prepare data for upload
        # Convert timestamp strings to datetime objects
        if 'timestamp' in df.columns:
            # Store timestamp as a string to avoid conversion issues
            if pd.api.types.is_datetime64_any_dtype(df['timestamp']):
                # Convert datetime to string
                df['timestamp_str'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
                print(f"Converted datetime to string: {df['timestamp_str'].iloc[0] if len(df) > 0 else 'No data'}")
            else:
                # If it's already a string, use it directly
                df['timestamp_str'] = df['timestamp']
                print(f"Using existing timestamp string: {df['timestamp_str'].iloc[0] if len(df) > 0 else 'No data'}")
            
            # Drop the original timestamp column
            df = df.drop(columns=['timestamp'])
        
        # Convert date strings to proper format if needed
        if 'date' in df.columns:
            # Convert to datetime and then to string in YYYY-MM-DD format
            df["date"] = pd.to_datetime(df["date"]).dt.strftime('%Y-%m-%d')
        
        # Convert column names to uppercase for Snowflake
        df.columns = [col.upper() for col in df.columns]
        
        # Print data types for debugging
        print("Column data types:")
        for col in df.columns:
            print(f"  {col}: {df[col].dtype}")
        
        # Create a temporary table for the new data
        cursor = conn.cursor()
        cursor.execute("CREATE OR REPLACE TEMPORARY TABLE TEMP_TRANSACTIONS LIKE ETH_STAKING_TRANSACTIONS")
        
        # Write new data to temp table
        success, num_chunks, num_rows, output = write_pandas(conn, df, "TEMP_TRANSACTIONS")
        
        if not success:
            print("Failed to upload transaction data to temporary table")
            return False
            
        # Count records before merge
        cursor.execute("SELECT COUNT(*) FROM ETH_STAKING_TRANSACTIONS")
        count_before = cursor.fetchone()[0]
        
        # Merge temp data into main table (insert new, update existing)
        cursor.execute("""
        MERGE INTO ETH_STAKING_TRANSACTIONS t
        USING TEMP_TRANSACTIONS s
        ON t.TRANSACTION_HASH = s.TRANSACTION_HASH
        WHEN NOT MATCHED THEN
            INSERT (
                TRANSACTION_HASH, SENDER_ADDRESS, RECEIVER_ADDRESS, AMOUNT_ETH, 
                TIMESTAMP_STR, GAS_USED, GAS_PRICE, BLOCK_NUMBER, GAS_COST_ETH, 
                DATE, HOUR, DAY_OF_WEEK, MONTH, IS_STANDARD_STAKE
            )
            VALUES (
                s.TRANSACTION_HASH, s.SENDER_ADDRESS, s.RECEIVER_ADDRESS, s.AMOUNT_ETH, 
                s.TIMESTAMP_STR, s.GAS_USED, s.GAS_PRICE, s.BLOCK_NUMBER, s.GAS_COST_ETH, 
                s.DATE, s.HOUR, s.DAY_OF_WEEK, s.MONTH, s.IS_STANDARD_STAKE
            )
        """)
        
        # Count records after merge
        cursor.execute("SELECT COUNT(*) FROM ETH_STAKING_TRANSACTIONS")
        count_after = cursor.fetchone()[0]
        new_records = count_after - count_before
        
        print(f"Successfully processed {num_rows} transaction records")
        print(f"Added {new_records} new records to ETH_STAKING_TRANSACTIONS")
        print(f"Total records in ETH_STAKING_TRANSACTIONS: {count_after}")
        
        # Record the pipeline run at the end
        record_count = len(df)
        record_pipeline_run(conn, "SUCCESS", record_count)
        
        return True
    
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
        
        # Let Snowflake infer the column types
        success, num_chunks, num_rows, output = write_pandas(conn, df, "ETH_STAKING_DAILY_STATS")
        
        if success:
            print(f"Successfully uploaded {num_rows} ETH_STAKING_DAILY_STATS records to Snowflake")
            
            return True
        else:
            print("Failed to upload daily statistics to Snowflake")
            return False
    
    except Exception as e:
        print(f"Error uploading daily statistics: {e}")
        return False

def record_pipeline_run(conn, status="SUCCESS", records_processed=0):
    """Record a pipeline run in the PIPELINE_RUNS table"""
    try:
        cursor = conn.cursor()
        
        # Create the table if it doesn't exist with VARCHAR for timestamp
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS PIPELINE_RUNS (
                run_id INT AUTOINCREMENT, 
                run_time VARCHAR(30),  
                status VARCHAR(50),
                records_processed INT
            )
        """)
        
        # Insert with formatted timestamp string
        cursor.execute("""
            INSERT INTO PIPELINE_RUNS (run_time, status, records_processed) 
            VALUES (TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD HH24:MI:SS'), %s, %s)
            """, 
            (status, records_processed)
        )
        
        # Make sure to commit the transaction
        conn.commit()
        
        print(f"Pipeline run recorded: status={status}, records={records_processed}")
    except Exception as e:
        print(f"Error recording pipeline run: {e}")
        # Don't let this error stop the rest of the process

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
    
    # Truncate tables before loading new data
    if not truncate_tables(conn):
        print("Warning: Failed to truncate tables. Proceeding with data load anyway.")
    
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
