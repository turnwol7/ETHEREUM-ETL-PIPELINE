# ETL Pipeline

This directory contains the ETL pipeline for extracting Ethereum staking data from Etherscan, transforming it, and loading it to Snowflake.

## Components

- `fetch_data.py`: Extracts staking transactions from Etherscan API
- `transform_data.py`: Transforms and cleans the data
- `load_snowflake.py`: Loads the data to Snowflake
- `dagster_pipeline.py`: Orchestrates the ETL pipeline using Dagster

## Setup

1. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

2. Create a `.env` file with your credentials:
   ```
   ETHERSCAN_API_KEY=your_api_key
   SNOWFLAKE_USER=your_user
   SNOWFLAKE_PASSWORD=your_password
   SNOWFLAKE_ACCOUNT=your_account
   SNOWFLAKE_WAREHOUSE=your_warehouse
   SNOWFLAKE_DATABASE=your_database
   SNOWFLAKE_SCHEMA=your_schema
   SNOWFLAKE_ROLE=your_role
   ```

3. Run the Dagster UI:
   ```
   dagster dev -f dagster_pipeline.py
   ```

4. Open http://localhost:3000 to view and run the pipeline
