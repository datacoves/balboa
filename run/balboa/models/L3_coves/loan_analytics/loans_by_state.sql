
        alter dynamic table "BALBOA_STAGING"."L3_LOAN_ANALYTICS"."LOANS_BY_STATE" set
            target_lag = '190 days'
            warehouse = wh_transforming
    