begin;
    
        
        
    

    

    merge into staging_BALBOA.source_dbt_artifacts.dim_dbt__tests as DBT_INTERNAL_DEST
        using staging_BALBOA.source_dbt_artifacts.dim_dbt__tests__dbt_tmp as DBT_INTERNAL_SOURCE
        on 
            DBT_INTERNAL_SOURCE.manifest_test_id = DBT_INTERNAL_DEST.manifest_test_id
        

    
    when matched then update set
        "MANIFEST_TEST_ID" = DBT_INTERNAL_SOURCE."MANIFEST_TEST_ID","COMMAND_INVOCATION_ID" = DBT_INTERNAL_SOURCE."COMMAND_INVOCATION_ID","DBT_CLOUD_RUN_ID" = DBT_INTERNAL_SOURCE."DBT_CLOUD_RUN_ID","ARTIFACT_RUN_ID" = DBT_INTERNAL_SOURCE."ARTIFACT_RUN_ID","ARTIFACT_GENERATED_AT" = DBT_INTERNAL_SOURCE."ARTIFACT_GENERATED_AT","NODE_ID" = DBT_INTERNAL_SOURCE."NODE_ID","NAME" = DBT_INTERNAL_SOURCE."NAME","DEPENDS_ON_NODES" = DBT_INTERNAL_SOURCE."DEPENDS_ON_NODES","PACKAGE_NAME" = DBT_INTERNAL_SOURCE."PACKAGE_NAME","TEST_PATH" = DBT_INTERNAL_SOURCE."TEST_PATH"
    

    when not matched then insert
        ("MANIFEST_TEST_ID", "COMMAND_INVOCATION_ID", "DBT_CLOUD_RUN_ID", "ARTIFACT_RUN_ID", "ARTIFACT_GENERATED_AT", "NODE_ID", "NAME", "DEPENDS_ON_NODES", "PACKAGE_NAME", "TEST_PATH")
    values
        ("MANIFEST_TEST_ID", "COMMAND_INVOCATION_ID", "DBT_CLOUD_RUN_ID", "ARTIFACT_RUN_ID", "ARTIFACT_GENERATED_AT", "NODE_ID", "NAME", "DEPENDS_ON_NODES", "PACKAGE_NAME", "TEST_PATH")

;
    commit;