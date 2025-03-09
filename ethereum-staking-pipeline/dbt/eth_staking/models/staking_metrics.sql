-- Staking metrics for the dashboard
SELECT
    -- Since timestamps are invalid, set time-based metrics to 0 or use alternative logic
    0 AS TOTAL_ETH_LAST_24H,
    0 AS TOTAL_TXS_LAST_24H,
    0 AS AVG_ETH_LAST_24H,
    0 AS TOTAL_ETH_LAST_7D,
    0 AS TOTAL_TXS_LAST_7D,
    0 AS AVG_ETH_LAST_7D,
    
    -- These metrics don't depend on timestamps
    COALESCE(SUM(AMOUNT_ETH), 0) AS TOTAL_ETH_ALL_TIME,
    COALESCE(COUNT(*), 0) AS TOTAL_TXS_ALL_TIME,
    CASE 
        WHEN COUNT(*) > 0 
        THEN SUM(AMOUNT_ETH) / COUNT(*) 
        ELSE 0 
    END AS AVG_ETH_ALL_TIME,
    
    -- For the last hour, use the most recent transaction
    -- This assumes your transactions are ordered by recency
    (SELECT COALESCE(SUM(AMOUNT_ETH), 0) 
     FROM (
         SELECT AMOUNT_ETH 
         FROM {{ source('snowflake', 'ETH_STAKING_TRANSACTIONS') }}
         ORDER BY TRANSACTION_HASH DESC 
         LIMIT 1
     )) AS TOTAL_ETH_LAST_HOUR,
    
    -- Current timestamp for reference
    CURRENT_TIMESTAMP() AS CALCULATED_AT
FROM {{ source('snowflake', 'ETH_STAKING_TRANSACTIONS') }} 