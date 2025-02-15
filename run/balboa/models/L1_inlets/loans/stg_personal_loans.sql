
        alter dynamic table "BALBOA_STAGING"."L1_LOANS"."STG_PERSONAL_LOANS" set
            target_lag = 'downstream'
            warehouse = wh_transforming_dynamic_tables
    