# Ethereum Staking Pipeline

This is a blockchain data pipeline that runs an ETL pipeline to process recent transactions on the beacon chain staking contract address to a fullstack front end here. This is an experiment for a Junior Data Engineer interview loop.

https://ethereum-etl-pipeline-frontend.onrender.com/ 

Tools: Python, SQL, Snowflake, DBT, Dagster

## Ethereum Beacon Chain Contract Address  

This is the contract that we are pulling data from  
```0x00000000219ab540356cBB839Cbe05303d7705Fa```  

## Architecture

- **ETL Pipeline**: Python scripts to extract data from Etherscan, transform it, and load it to Snowflake
- **Orchestration**: Dagster for scheduling and monitoring the pipeline
- **Data Modeling**: dbt for transforming data in Snowflake
- **API**: FastAPI for serving data to the frontend
- **Frontend**: Next.js dashboard deployed on Vercel

## Directories

- `/etl`: ETL pipeline code and Dagster orchestration
- `/dbt`: dbt models for data transformation in Snowflake
- `/api`: FastAPI backend for serving data
- `/frontend`: Next.js frontend for visualization  

## Services

You'll need your own API's and accounts for:

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

Currently my deployment service is on the free tier, so you must run the orchestration from your dev environment on the dev-fix branch.

The main branch is deployed but the orchestration is too heavy for my VM's so just run dagster locally to run the 3 minute schedule.

See the README in each directory for specific setup instructions.

Render Deployment

https://ethereum-etl-pipeline-frontend.onrender.com/ 