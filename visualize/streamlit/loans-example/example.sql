/*
/config/workspace/visualize/streamlit/start_app.sh

cd $DATACOVES__REPO_PATH/load/dlt
./loans/loans_data.py

https://app.snowflake.com/datacoves/main/#/data/databases/BALBOA/schemas/L3_LOAN_ANALYTICS/dynamic-table/LOANS_BY_STATE__DYNAMIC

dlt pipeline loans_data show

cd $DATACOVES__REPO_PATH/transform
*/

-- These are useful queries to run for demo purposes

use role loader;
use warehouse wh_loading;

-- source table needs to have change tracking enabled
alter table RAW.LOANS.PERSONAL_LOANS set CHANGE_TRACKING = true;

SHOW TABLES LIKE 'PERSONAL_LOANS' IN SCHEMA RAW.LOANS;


-- delete records from a table
delete
from RAW.LOANS.PERSONAL_LOANS
where left(addr_state, 1)> 'A';

-- see the rows in a table
select count(*)
from RAW.LOANS.PERSONAL_LOANS;

select distinct addr_state from raw.loans.personal_loans limit 10;

-- dropping dymanic table
use role analyst;
use warehouse wh_transforming;
drop dynamic table balboa_dev.gomezn.loans_by_state__dynamic;
drop dynamic table balboa_dev.gomezn.loans_by_state__standard;


drop schema balboa_dev.fivetran_centre_straighten_staging;
------
use warehouse wh_orchestrating;
select distinct addr_state from balboa.l1_loans.personal_loans;
drop table balboa.l1_loans.personal_loans;
drop table balboa.l3_loan_analytics.loans_by_state__dynamic;


-- Creating Streamlit App
use role transformer_dbt;
create database balboa_apps;
create schema balboa_apps.resources;
CREATE STAGE balboa_apps.resources.streamlit
    directory = (enable=true)
    file_format = (type=CSV field_delimiter=None record_delimiter=None);

PUT 'file:///config/workspace/visualize/streamlit/loans-example/loans.py' @balboa.apps.streamlit
    overwrite=true auto_compress=false;

drop streamlit balboa_apps.resources.loans;

CREATE STREAMLIT IF NOT EXISTS balboa_apps.resources.loans
    ROOT_LOCATION = '@balboa_apps.resources.streamlit'
    MAIN_FILE = '/loans.py'
    QUERY_WAREHOUSE = "WH_TRANSFORMING";

grant usage on streamlit balboa_apps.resources.loans to role analyst;

-- Cheacking that analyst has access to app
use role analyst;
use database balboa;
use schema apps;
SHOW STREAMLITS;

select * from balboa.l3_loan_analytics.loans_by_state order by NUMBER_OF_LOANS desc;



select * from balboa_dev.gomezn.loans_by_state__standard;
select * from balboa_dev.gomezn.loans_by_state__dynamic;
