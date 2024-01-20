
        alter dynamic table "BALBOA_STAGING"."L1_LOANS"."PERSONAL_LOANS" set
            target_lag = 'downstream'
            warehouse = wh_transforming
    