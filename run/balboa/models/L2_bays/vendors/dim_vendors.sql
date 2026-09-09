
  create or replace   view BALBOA_STAGING.L2_VENDORS.dim_vendors
  
    
    
(
  
    "VENDOR_ID" COMMENT $$Vendor primary key and fact join target.$$, 
  
    "VENDOR_NAME" COMMENT $$Vendor display name.$$, 
  
    "CATEGORY" COMMENT $$Vendor spend category.$$, 
  
    "STATUS" COMMENT $$Recorded lifecycle flag, not a behavioral activity measure.$$, 
  
    "ONBOARDED_DATE" COMMENT $$First day of the vendor onboarding month.$$
  
)

  copy grants
  
  
  as (
    select
    vendor_id,
    vendor_name,
    category,
    status,
    onboarded_date
from L1_ERP.stg_vendors
  );

