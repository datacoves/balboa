
  create or replace   view BALBOA_STAGING.L1_OBSERVE.test_failures
  
    
    
(
  
    "TEST_NAME" COMMENT $$The name or label of the test. Defaults to 'no_tests' as a placeholder.$$, 
  
    "COUNT_FAILED" COMMENT $$The count of failed tests, initialized to zero as no tests are expected initially.$$, 
  
    "ERROR_STATE" COMMENT $$The error state of the test, set as null because this is a placeholder model.$$, 
  
    "WARNING_STATE" COMMENT $$The warning state of the test, set as null due to the placeholder nature of this data.$$, 
  
    "TESTED_MODELS" COMMENT $$A JSON array of models that were tested, initially empty to reflect the lack of any actual test data yet.$$
  
)

  copy grants as (
    
select
    'no_tests' as test_name,
    0 as count_failed,
    null as error_state,
    null as warning_state,
    parse_json('[]') as tested_models
where 1 = 0
  );

