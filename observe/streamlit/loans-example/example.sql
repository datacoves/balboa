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
