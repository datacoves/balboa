
  
    

create or replace transient table BALBOA_STAGING.L3_FINANCE.mart_vendor_activity
    copy grants
    
    
    
    

    as (-- Finance view of vendors and whether each is active, per the governed definition in dim_vendor.

select
    vendor_id,
    vendor_name,
    vendor_category,
    is_active_vendor,
    is_dormant_vendor,
    window_po_date_max as vendor_last_po_date,
    window_invoice_date_max as vendor_last_invoice_date,
    vendor_recorded_status,
    is_recorded_status_mismatch,
    activity_as_of_date,
    activity_window_start_date
from L2_VENDOR.dim_vendor
    )
;


  