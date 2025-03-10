# Ethereum Staking Pipeline

A complete data pipeline for Ethereum staking data using modern data stack technologies.

## Architecture

- **ETL Pipeline**: Python scripts to extract data from Etherscan, transform it, and load it to Snowflake
- **Orchestration**: Dagster for scheduling and monitoring the pipeline
- **Data Modeling**: dbt for transforming data in Snowflake
- **API**: FastAPI for serving data to the frontend
- **Frontend**: Next.js dashboard deployed on Vercel

## Components

- `/etl`: ETL pipeline code and Dagster orchestration
- `/dbt`: dbt models for data transformation in Snowflake
- `/api`: FastAPI backend for serving data
- `/frontend`: Next.js frontend for visualization

## Usefull commands to get running  

API Server:  
```uvicorn api:app```  

Frontend Server:  
```npm run dev```  

Dagster Server:  
Run in the etl directory  
```dagster dev -f dagster_pipeline.py -p 4000```  

See the README in each directory for specific setup instructions.
