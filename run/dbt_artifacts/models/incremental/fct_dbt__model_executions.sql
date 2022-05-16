begin;
    
        
        
    

    

    merge into staging_BALBOA.source_dbt_artifacts.fct_dbt__model_executions as DBT_INTERNAL_DEST
        using staging_BALBOA.source_dbt_artifacts.fct_dbt__model_executions__dbt_tmp as DBT_INTERNAL_SOURCE
        on 
            DBT_INTERNAL_SOURCE.model_execution_id = DBT_INTERNAL_DEST.model_execution_id
        

    
    when matched then update set
        "MODEL_EXECUTION_ID" = DBT_INTERNAL_SOURCE."MODEL_EXECUTION_ID","COMMAND_INVOCATION_ID" = DBT_INTERNAL_SOURCE."COMMAND_INVOCATION_ID","DBT_CLOUD_RUN_ID" = DBT_INTERNAL_SOURCE."DBT_CLOUD_RUN_ID","ARTIFACT_RUN_ID" = DBT_INTERNAL_SOURCE."ARTIFACT_RUN_ID","ARTIFACT_GENERATED_AT" = DBT_INTERNAL_SOURCE."ARTIFACT_GENERATED_AT","WAS_FULL_REFRESH" = DBT_INTERNAL_SOURCE."WAS_FULL_REFRESH","NODE_ID" = DBT_INTERNAL_SOURCE."NODE_ID","THREAD_ID" = DBT_INTERNAL_SOURCE."THREAD_ID","STATUS" = DBT_INTERNAL_SOURCE."STATUS","COMPILE_STARTED_AT" = DBT_INTERNAL_SOURCE."COMPILE_STARTED_AT","QUERY_COMPLETED_AT" = DBT_INTERNAL_SOURCE."QUERY_COMPLETED_AT","TOTAL_NODE_RUNTIME" = DBT_INTERNAL_SOURCE."TOTAL_NODE_RUNTIME","ROWS_AFFECTED" = DBT_INTERNAL_SOURCE."ROWS_AFFECTED","MODEL_MATERIALIZATION" = DBT_INTERNAL_SOURCE."MODEL_MATERIALIZATION","MODEL_SCHEMA" = DBT_INTERNAL_SOURCE."MODEL_SCHEMA","NAME" = DBT_INTERNAL_SOURCE."NAME"
    

    when not matched then insert
        ("MODEL_EXECUTION_ID", "COMMAND_INVOCATION_ID", "DBT_CLOUD_RUN_ID", "ARTIFACT_RUN_ID", "ARTIFACT_GENERATED_AT", "WAS_FULL_REFRESH", "NODE_ID", "THREAD_ID", "STATUS", "COMPILE_STARTED_AT", "QUERY_COMPLETED_AT", "TOTAL_NODE_RUNTIME", "ROWS_AFFECTED", "MODEL_MATERIALIZATION", "MODEL_SCHEMA", "NAME")
    values
        ("MODEL_EXECUTION_ID", "COMMAND_INVOCATION_ID", "DBT_CLOUD_RUN_ID", "ARTIFACT_RUN_ID", "ARTIFACT_GENERATED_AT", "WAS_FULL_REFRESH", "NODE_ID", "THREAD_ID", "STATUS", "COMPILE_STARTED_AT", "QUERY_COMPLETED_AT", "TOTAL_NODE_RUNTIME", "ROWS_AFFECTED", "MODEL_MATERIALIZATION", "MODEL_SCHEMA", "NAME")

;
    commit;