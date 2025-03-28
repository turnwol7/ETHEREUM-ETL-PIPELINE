import pandas as pd
import os
from datetime import datetime

def load_raw_data(filename="staking_transactions.csv"):
    """Load raw staking transactions from CSV file"""
    # First check if file exists in the current directory
    if os.path.exists(filename):
        print(f"Loading data from {filename}...")
        return pd.read_csv(filename)
    
    # Then check if file exists in the root directory (two levels up)
    root_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), filename)
    if os.path.exists(root_path):
        print(f"Loading data from root directory: {root_path}...")
        return pd.read_csv(root_path)
    
    print(f"Error: File {filename} not found in either current or root directory")
    return None

def transform_data(df):
    """Transform and clean the staking transaction data"""
    if df is None or df.empty:
        print("No data to transform")
        return None
    
    # Make an explicit copy to avoid SettingWithCopyWarning
    df = df.copy()
    
    # Convert timestamps
    print("Converting timestamps...")
    # Print raw timestamp for debugging
    print(f"Raw timestamp value: {df['timeStamp'].iloc[0]}")
    
    # The timestamps in the CSV are already in the correct format: 2025-03-09 23:51:35
    # We need to preserve this format rather than trying to convert from epoch time
    if 'timeStamp' in df.columns:
        # Check if the timestamp is already in string format
        if df['timeStamp'].dtype == 'object':
            # If it's already a string in datetime format, use it directly
            df['timestamp'] = df['timeStamp']
            print("Using existing timestamp string format")
        else:
            # If it's a numeric value, convert it to datetime
            try:
                # Convert nanoseconds to seconds if needed
                if df['timeStamp'].iloc[0] > 1e15:
                    df['timestamp'] = pd.to_datetime(df['timeStamp'].astype(float) / 1e9, unit='s')
                    print("Converted nanosecond timestamps to datetime")
                else:
                    df['timestamp'] = pd.to_datetime(df['timeStamp'], unit='s')
                    print("Converted epoch timestamps to datetime")
            except Exception as e:
                print(f"Error converting timestamps: {e}")
                # Use the original timestamp as a fallback
                df['timestamp'] = df['timeStamp']
                print("Using original timestamp as fallback")
    
    # Print the converted timestamp
    print(f"Converted timestamp: {df['timestamp'].iloc[0]}")
    
    # Ensure timestamp is in the correct format for Snowflake
    print(f"Sample timestamp after conversion: {df['timestamp'].iloc[0]}")
    
    # Convert value from Wei to ETH
    if "value" in df.columns:
        print("Converting Wei to ETH...")
        df["amount_eth"] = df["value"].astype(float) / 10**18
    
    # Rename columns to match Snowflake schema
    print("Renaming columns...")
    column_mapping = {
        "from": "sender_address",
        "to": "receiver_address",
        "hash": "transaction_hash",
        "blockNumber": "block_number",
        "gasPrice": "gas_price",
        "gasUsed": "gas_used"
    }
    df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns}, inplace=True)
    
    # Select relevant columns
    relevant_columns = [
        "transaction_hash", "sender_address", "receiver_address", 
        "amount_eth", "timestamp", "gas_used", "gas_price", 
        "block_number"
    ]
    
    # Only keep columns that exist in the dataframe
    columns_to_keep = [col for col in relevant_columns if col in df.columns]
    
    print(f"Selecting {len(columns_to_keep)} relevant columns...")
    # Create a new DataFrame with only the columns we want to keep
    result_df = df[columns_to_keep].copy()
    
    # Add derived columns
    print("Adding derived columns...")
    
    # Calculate gas cost in ETH
    if "gas_price" in result_df.columns and "gas_used" in result_df.columns:
        result_df["gas_cost_eth"] = result_df["gas_price"].astype(float) * result_df["gas_used"].astype(float) / 10**18
    
    # Add date components for easier analysis
    if "timestamp" in result_df.columns:
        result_df["date"] = result_df["timestamp"].dt.date
        result_df["hour"] = result_df["timestamp"].dt.hour
        result_df["day_of_week"] = result_df["timestamp"].dt.day_name()
        result_df["month"] = result_df["timestamp"].dt.month_name()
    
    # Flag for standard staking amount (32 ETH)
    if "amount_eth" in result_df.columns:
        result_df["is_standard_stake"] = (result_df["amount_eth"] == 32.0)
    
    return result_df

def create_daily_summary(df):
    """Create daily summary statistics"""
    if df is None or df.empty or "date" not in df.columns:
        print("Cannot create daily summary - missing required data")
        return None
    
    print("Creating daily summary statistics...")
    daily_stats = df.groupby("date").agg({
        "transaction_hash": "count",
        "amount_eth": ["sum", "mean", "min", "max"],
        "gas_cost_eth": ["sum", "mean"] if "gas_cost_eth" in df.columns else [],
        "is_standard_stake": "sum" if "is_standard_stake" in df.columns else []
    }).reset_index()
    
    # Flatten multi-level columns
    daily_stats.columns = ["_".join(col).strip("_") for col in daily_stats.columns.values]
    
    # Rename columns for clarity
    column_mapping = {
        "transaction_hash_count": "num_transactions",
        "amount_eth_sum": "total_eth_staked",
        "amount_eth_mean": "avg_stake_size",
        "amount_eth_min": "min_stake_size",
        "amount_eth_max": "max_stake_size",
        "gas_cost_eth_sum": "total_gas_cost",
        "gas_cost_eth_mean": "avg_gas_cost",
        "is_standard_stake_sum": "num_standard_stakes"
    }
    
    daily_stats.rename(columns={k: v for k, v in column_mapping.items() if k in daily_stats.columns}, 
                       inplace=True)
    
    # Calculate percentage of standard stakes
    if "num_standard_stakes" in daily_stats.columns and "num_transactions" in daily_stats.columns:
        daily_stats["pct_standard_stakes"] = (
            daily_stats["num_standard_stakes"] / daily_stats["num_transactions"] * 100
        )
    
    # Sort by date in descending order (most recent first)
    daily_stats = daily_stats.sort_values("date", ascending=False)
    
    return daily_stats

def save_transformed_data(df, filename="transformed_staking_data.csv"):
    """Save transformed data to CSV file"""
    if df is None or df.empty:
        print("No data to save")
        return
        
    # Save to current directory
    df.to_csv(filename, index=False)
    print(f"Saved transformed data with {len(df)} rows to {filename}")
    
    # Save to root directory (two levels up)
    root_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), filename)
    df.to_csv(root_path, index=False)
    print(f"Saved transformed data with {len(df)} rows to root directory: {root_path}")

def save_daily_summary(df, filename="daily_staking_stats.csv"):
    """Save daily summary to CSV file"""
    if df is None or df.empty:
        print("No daily summary to save")
        return
        
    # Save to current directory
    df.to_csv(filename, index=False)
    print(f"Saved daily summary with {len(df)} rows to {filename}")
    
    # Save to root directory (two levels up)
    root_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), filename)
    df.to_csv(root_path, index=False)
    print(f"Saved daily summary with {len(df)} rows to root directory: {root_path}")

def main():
    """Main function to transform staking data"""
    # Load raw data from the CSV created by fetch_data.py
    raw_data = load_raw_data()
    
    if raw_data is not None:
        # Transform the data
        transformed_data = transform_data(raw_data)
        
        if transformed_data is not None:
            # Preview the transformed data
            print("\nTransformed data preview:")
            print(transformed_data.head())
            
            # Create daily summary
            daily_summary = create_daily_summary(transformed_data)
            
            if daily_summary is not None:
                print("\nDaily summary preview:")
                print(daily_summary.head())
                
                # Save the results for the load step
                save_transformed_data(transformed_data)
                save_daily_summary(daily_summary)
            
    print("Transformation complete!")

if __name__ == "__main__":
    main()
