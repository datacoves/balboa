-- These are useful queries to run for demo purposes

use role loader;
use warehouse wh_loading;

-- source table needs to have change tracking enabled
alter table RAW.LOANS.PERSONAL_LOANS set CHANGE_TRACKING = true;

-- see the rows in a table
select count(*)
from RAW.LOANS.PERSONAL_LOANS;

-- delete records from a table
delete
from RAW.LOANS.PERSONAL_LOANS
where left(addr_state, 1)> 'A';

-- dropping dymanic table
use role analyst;
use warehouse wh_transforming;
drop dynamic table balboa_dev.gomezn.loans_by_state;

------

-- Creating Streamlit App
use role transformer_dbt;
create database balboa_apps;
create schema balboa_apps.resources;
CREATE STAGE balboa_apps.resources.streamlit
    directory = (enable=true)
    file_format = (type=CSV field_delimiter=None record_delimiter=None);

PUT 'file:///config/workspace/observe/streamlit/loans-example/loans.py' @balboa.apps.streamlit
    overwrite=true auto_compress=false;

drop streamlit balboa_apps.resources.loans;

CREATE STREAMLIT IF NOT EXISTS balboa_apps.resources.loans
    ROOT_LOCATION = '@balboa.apps.streamlit'
    MAIN_FILE = '/loans.py'
    QUERY_WAREHOUSE = "WH_TRANSFORMING";

grant usage on streamlit balboa.apps.loans to role analyst;

-- Cheacking that analyst has access to app
use role analyst;
use database balboa;
use schema apps;
SHOW STREAMLITS;

select * from balboa.l3_loan_analytics.loans_by_state order by NUMBER_OF_LOANS desc;
