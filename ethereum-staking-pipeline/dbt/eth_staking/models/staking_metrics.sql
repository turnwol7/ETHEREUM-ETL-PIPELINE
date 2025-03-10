-- Staking metrics for the dashboard

-- First, get the max timestamp as a variable
WITH max_time AS (
    SELECT TO_TIMESTAMP_NTZ(MAX(TIMESTAMP_STR)) AS max_timestamp
    FROM {{ source('snowflake', 'ETH_STAKING_TRANSACTIONS') }}
)

SELECT
    -- Total ETH staked in the last 24 hours
    COALESCE(SUM(CASE 
        WHEN TO_TIMESTAMP_NTZ(TIMESTAMP_STR) >= DATEADD(hour, -24, max_time.max_timestamp)
        THEN AMOUNT_ETH 
        ELSE 0 
    END), 0) AS TOTAL_ETH_LAST_24H,
    
    -- Total number of transactions in the last 24 hours
    COALESCE(SUM(CASE 
        WHEN TO_TIMESTAMP_NTZ(TIMESTAMP_STR) >= DATEADD(hour, -24, max_time.max_timestamp)
        THEN 1 
        ELSE 0 
    END), 0) AS TOTAL_TXS_LAST_24H,
    
    -- Average ETH staked per transaction in the last 24 hours
    CASE 
        WHEN SUM(CASE WHEN TO_TIMESTAMP_NTZ(TIMESTAMP_STR) >= DATEADD(hour, -24, max_time.max_timestamp) THEN 1 ELSE 0 END) > 0 
        THEN SUM(CASE WHEN TO_TIMESTAMP_NTZ(TIMESTAMP_STR) >= DATEADD(hour, -24, max_time.max_timestamp) THEN AMOUNT_ETH ELSE 0 END) / 
             SUM(CASE WHEN TO_TIMESTAMP_NTZ(TIMESTAMP_STR) >= DATEADD(hour, -24, max_time.max_timestamp) THEN 1 ELSE 0 END)
        ELSE 0 
    END AS AVG_ETH_LAST_24H,
    
    -- Total ETH staked in the last 7 days
    COALESCE(SUM(CASE 
        WHEN TO_TIMESTAMP_NTZ(TIMESTAMP_STR) >= DATEADD(day, -7, max_time.max_timestamp)
        THEN AMOUNT_ETH 
        ELSE 0 
    END), 0) AS TOTAL_ETH_LAST_7D,
    
    -- Total number of transactions in the last 7 days
    COALESCE(SUM(CASE 
        WHEN TO_TIMESTAMP_NTZ(TIMESTAMP_STR) >= DATEADD(day, -7, max_time.max_timestamp)
        THEN 1 
        ELSE 0 
    END), 0) AS TOTAL_TXS_LAST_7D,
    
    -- Average ETH staked per transaction in the last 7 days
    CASE 
        WHEN SUM(CASE WHEN TO_TIMESTAMP_NTZ(TIMESTAMP_STR) >= DATEADD(day, -7, max_time.max_timestamp) THEN 1 ELSE 0 END) > 0 
        THEN SUM(CASE WHEN TO_TIMESTAMP_NTZ(TIMESTAMP_STR) >= DATEADD(day, -7, max_time.max_timestamp) THEN AMOUNT_ETH ELSE 0 END) / 
             SUM(CASE WHEN TO_TIMESTAMP_NTZ(TIMESTAMP_STR) >= DATEADD(day, -7, max_time.max_timestamp) THEN 1 ELSE 0 END)
        ELSE 0 
    END AS AVG_ETH_LAST_7D,
    
    -- Total ETH staked all time
    COALESCE(SUM(AMOUNT_ETH), 0) AS TOTAL_ETH_ALL_TIME,
    
    -- Total number of transactions all time
    COALESCE(COUNT(*), 0) AS TOTAL_TXS_ALL_TIME,
    
    -- Average ETH staked per transaction all time
    CASE 
        WHEN COUNT(*) > 0 
        THEN SUM(AMOUNT_ETH) / COUNT(*) 
        ELSE 0 
    END AS AVG_ETH_ALL_TIME,
    
    -- Total ETH staked in the last hour
    COALESCE(SUM(CASE 
        WHEN TO_TIMESTAMP_NTZ(TIMESTAMP_STR) >= DATEADD(hour, -1, max_time.max_timestamp)
        THEN AMOUNT_ETH 
        ELSE 0 
    END), 0) AS TOTAL_ETH_LAST_HOUR,
    
    -- Total number of transactions in the last hour
    COALESCE(SUM(CASE 
        WHEN TO_TIMESTAMP_NTZ(TIMESTAMP_STR) >= DATEADD(hour, -1, max_time.max_timestamp)
        THEN 1 
        ELSE 0 
    END), 0) AS TOTAL_TXS_LAST_HOUR,
    
    -- Current timestamp for reference
    CURRENT_TIMESTAMP() AS CALCULATED_AT
FROM 
    {{ source('snowflake', 'ETH_STAKING_TRANSACTIONS') }},
    max_time 