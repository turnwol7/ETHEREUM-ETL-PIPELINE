# dbt Models

This directory contains dbt models for transforming Ethereum staking data in Snowflake.

## Models

- `recent_transactions.sql`: Gets the 10 most recent staking transactions
- `hourly_stats.sql`: Calculates hourly statistics for staking activity

## Setup

1. Install dbt:
   ```
   pip install dbt-snowflake
   ```

2. Configure your Snowflake connection:
   ```
   dbt init
   ```
   Follow the prompts to set up your Snowflake connection.

3. Run the models:
   ```
   dbt run
   ```
