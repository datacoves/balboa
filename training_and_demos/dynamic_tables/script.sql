-- drop schema balboa_dev.gomezn;

-- =================================================================================
-- Enable Change tracking on raw table
-- =================================================================================
use role loader;
-- source table needs to have change tracking enabled for dynamic tables to refresh
alter table RAW.LOANS.PERSONAL_LOANS set CHANGE_TRACKING = true;



-- =================================================================================
-- First clear out non CA records from source table
-- =================================================================================

use role loader;
delete
from raw.loans.personal_loans
where addr_state != 'CA';


-- =================================================================================
-- Perform dbt build
-- =================================================================================

/*

cd /config/workspace/transform && dbt build  --vars '{"persist_tests": "true", "tests_model": "test_failures"}'

*/

show views in schema balboa_dev.gomezn
    ->> select "name" from $1;

show tables in schema balboa_dev.gomezn
    ->> select "name" from $1;

show dynamic tables in schema balboa_dev.gomezn
    ->> select "name", "rows", "target_lag","refresh_mode","refresh_mode_reason","warehouse" from $1;

-- =================================================================================
-- Show that we only have California in the raw and in the dynamic staging table
-- =================================================================================

-- raw data
select distinct addr_state from raw.loans.personal_loans;

-- dynamic table with a target lag = downstream
select distinct addr_state from BALBOA_DEV.GOMEZN.STG_PERSONAL_LOANS;

-- There are no errors in the errors view summary
select * from balboa_dev.gomezn.stg_test_failures;


-- =================================================================================
-- Reload the data with dlt
-- =================================================================================
/*

/config/workspace/load/dlt/loans_data.py

 */

-- =================================================================================
-- Show that now we have many states and hence errors
-- =================================================================================

-- raw data
select distinct addr_state from raw.loans.personal_loans;

-- dynamic table with a target lag = downstream
select distinct addr_state from BALBOA_DEV.GOMEZN.STG_PERSONAL_LOANS;

-- There are no errors in the errors view summary
select * from balboa_dev.gomezn.stg_test_failures;
