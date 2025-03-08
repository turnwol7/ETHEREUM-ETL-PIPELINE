# API

This directory contains a FastAPI backend for serving Ethereum staking data from Snowflake.

## Endpoints

- `/recent-transactions`: Gets the 10 most recent staking transactions
- `/hourly-stats`: Gets hourly statistics for staking activity
- `/pipeline-status`: Gets the status of the data pipeline

## Setup

1. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

2. Create a `.env` file with your Snowflake credentials (copy from the ETL directory)

3. Run the API:
   ```
   uvicorn api:app --reload
   ```

4. Open http://localhost:8000/docs to view the API documentation
