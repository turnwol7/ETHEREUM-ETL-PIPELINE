# ETL

This directory holds our ETL logic. We fetch, transform and load to Snowflake here.  

You can run the scripts manually to test CSV outputs. But the main upload script in ./upload_script.sh does everything for us.

To run the dagster_pipeline.py we need to boot up Dagster with:  
```dagster dev -f dagster_pipeline.py -p 4000```  

