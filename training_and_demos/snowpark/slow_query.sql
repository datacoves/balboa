-- This is a way to fame a slow snwoflake query
SELECT r.*
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION AS r
,LATERAL (SELECT SYSTEM$WAIT(6)) AS delay
