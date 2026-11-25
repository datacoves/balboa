
  create or replace   view BALBOA_STAGING.L1_ERP.stg_vendors__erp
  
    
    
(
  
    "VENDOR_ID" COMMENT $$Vendor primary key; vendors are identified and counted by this id.$$, 
  
    "VENDOR_NAME" COMMENT $$Vendor display name.$$, 
  
    "VENDOR_CATEGORY" COMMENT $$Vendor spend category.$$, 
  
    "VENDOR_RECORDED_STATUS" COMMENT $$Status flag (active / inactive) as recorded in the ERP. A stored attribute, not a behavioural state.$$, 
  
    "VENDOR_ONBOARDED_DATE" COMMENT $$Date the vendor was onboarded.$$
  
)

  copy grants
  
  
  as (
    select
    vendor_id::varchar as vendor_id,
    vendor_name::varchar as vendor_name,
    category::varchar as vendor_category,
    status::varchar as vendor_recorded_status,
    onboarded_date::date as vendor_onboarded_date
from SEEDS.seed_vendors
  );

