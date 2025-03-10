# ETHEREUM VALIDATOR DATA ETL PIPELINE  

This blockchain ETL pipeline displays recent transaction data and metrics for the official ETH2 validator contract address.  

This is a learning project that uses.
- Python  
- Snowflake  
- SQL  
- DBT  
- Dagster  
- Next.js  

We have a number of servers to run to see data.

- Metrics API  
uvicorn api:app  

- Frontend  
npm run dev  

- Dagster UI  
dagster dev -f dagster_pipeline.py -p 4000  