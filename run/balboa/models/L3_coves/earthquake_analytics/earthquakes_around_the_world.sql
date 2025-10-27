-- back compat for old kwarg name
  
  begin;
    

        insert into BALBOA_STAGING.L3_EARTHQUAKE_ANALYTICS.earthquakes_around_the_world ("CT", "LOCATION_GEO_POINT", "SIG", "SIG_CLASS", "EARTHQUAKE_DATE", "COUNTRY_CODE")
        (
            select "CT", "LOCATION_GEO_POINT", "SIG", "SIG_CLASS", "EARTHQUAKE_DATE", "COUNTRY_CODE"
            from BALBOA_STAGING.L3_EARTHQUAKE_ANALYTICS.earthquakes_around_the_world__dbt_tmp
        );
    commit;