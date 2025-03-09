import requests
import os
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("ETHERSCAN_API_KEY")

# Ethereum 2.0 Deposit Contract - This is where all ETH staking happens
ETH2_DEPOSIT_CONTRACT = "0x00000000219ab540356cBB839Cbe05303d7705Fa"

def fetch_staking_data(page=1, limit=100, sort_order="desc"):
    """Fetch staking transactions from the ETH2 deposit contract"""
    url = "https://api.etherscan.io/api"
    params = {
        "module": "account",
        "action": "txlist",
        "address": ETH2_DEPOSIT_CONTRACT,
        "startblock": 0,
        "endblock": 99999999,
        "page": page,
        "offset": limit,
        "sort": sort_order,
        "apikey": API_KEY
    }
    
    response = requests.get(url, params=params)
    data = response.json()
    
    if data["status"] == "1":
        print(f"Successfully fetched {len(data['result'])} transactions")
        return data["result"]
    else:
        print(f"Error fetching data: {data.get('message', 'Unknown error')}")
        return []

def format_transaction_preview(tx):
    """Format a transaction for preview display"""
    # Convert timestamp to readable date
    timestamp = datetime.fromtimestamp(int(tx["timeStamp"]))
    days_ago = (datetime.now() - timestamp).days
    
    # Convert value from Wei to ETH
    value_eth = float(tx["value"]) / 10**18
    
    return {
        "Hash": tx["hash"],
        "From": tx["from"],
        "Value": f"{value_eth} ETH",
        "Date": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "Days Ago": f"{days_ago} days",
        "Block": tx["blockNumber"]
    }

def save_to_csv(transactions, filename="staking_transactions.csv"):
    """Save raw transactions to CSV file"""
    if not transactions:
        print("No transactions to save")
        return
    
    # Save to both the current directory and the root directory
    df = pd.DataFrame(transactions)
    
    # Save to current directory
    df.to_csv(filename, index=False)
    print(f"Saved {len(df)} transactions to {filename}")
    
    # Save to root directory (two levels up)
    root_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), filename)
    df.to_csv(root_path, index=False)
    print(f"Saved {len(df)} transactions to root directory: {root_path}")

def main():
    """Main function to fetch and save staking data"""
    print("Fetching recent ETH2 staking transactions...")
    
    # Fetch only 10 transactions instead of 500
    transactions = fetch_staking_data(page=1, limit=10, sort_order="desc")
    
    if transactions:
        # Preview first transaction
        print("\nLatest transaction:")
        formatted_tx = format_transaction_preview(transactions[0])
        for key, value in formatted_tx.items():
            print(f"{key}: {value}")
        
        # Show a few more recent transactions
        print("\nRecent transactions:")
        for i, tx in enumerate(transactions[1:5]):
            formatted = format_transaction_preview(tx)
            print(f"{i+2}. {formatted['Date']} ({formatted['Days Ago']}) - {formatted['Value']}")
        
        # Save to CSV for the transform step
        save_to_csv(transactions)
        
        # Print the oldest transaction in this batch for reference
        print("\nOldest transaction in this batch:")
        oldest_tx = format_transaction_preview(transactions[-1])
        for key, value in oldest_tx.items():
            print(f"{key}: {value}")

if __name__ == "__main__":
    main()
