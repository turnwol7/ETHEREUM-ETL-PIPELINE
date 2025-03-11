# Ethereum Staking Pipeline

A complete data pipeline for Ethereum staking data using modern data stack technologies.  

## Ethereum Beacon Chain Contract Address  

This is the contract that we are pulling data from  
```0x00000000219ab540356cBB839Cbe05303d7705Fa```  

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

## Services

Etherscan API  
Snowflake Account  

## Usefull commands to get running  

Manual Upload Script:  
Run in root:  
```./upload_script.sh```

API Server:  
```uvicorn api:app```  

Frontend Server:  
```npm run dev```  

Dagster Server:  
Run in the etl directory  
```dagster dev -f dagster_pipeline.py -p 4000```  

See the README in each directory for specific setup instructions.


Render Deployment URLS

https://ethereum-etl-pipeline-frontend.onrender.com/
https://ethereum-etl-pipeline-dagster.onrender.com  
https://ethereum-etl-pipeline-api.onrender.com/  

Currently I dont pay for upgraded RAM VM's on Render so the front end uses Dagit config, while my dev environment runs the automation with Dagster locally to update the transactions.  The production Dagster version is to heavy for Render VM and crashes due to lack of RAM.

if you go to the dagster endpoint in production you will see a stripped down version of dagster that wont run any orchestration.

We can run the Dagster version locally to prove that this will work if we had more RAM.