#!/bin/zsh

# ETL Pipeline Script for Ethereum Staking Data
# This script runs the full ETL pipeline and dbt models

echo "🚀 Starting Ethereum Staking ETL Pipeline"

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    echo "📦 Activating virtual environment..."
    source venv/bin/activate
fi

# Install required dependencies
# echo "📦 Installing required dependencies..."
# pip install "snowflake-connector-python[pandas]"

# Clean up duplicate CSV files in the ETL directory
echo "🧹 Cleaning up duplicate CSV files..."
python ethereum-staking-pipeline/etl/cleanup_duplicates.py

# Run ETL scripts
echo "📥 Fetching data from Etherscan..."
python ethereum-staking-pipeline/etl/fetch_data.py

# Show the first few lines of the CSV to verify
echo "📄 Verifying fetched data..."
head -n 3 staking_transactions.csv

echo "🔄 Transforming data..."
python ethereum-staking-pipeline/etl/transform_data.py

# Show the first few lines of the transformed CSV
echo "📄 Verifying transformed data..."
head -n 3 transformed_staking_data.csv

echo "📤 Loading data to Snowflake..."
python ethereum-staking-pipeline/etl/load_snowflake.py

# Run dbt models
echo "📊 Running dbt models..."
cd ethereum-staking-pipeline/dbt/eth_staking
dbt run
cd -

echo "✅ ETL Pipeline completed successfully!"
echo "Check your dashboard for updated data."

# Remind about refreshing the frontend
echo "🔄 Remember to refresh your frontend to see the latest data!" 