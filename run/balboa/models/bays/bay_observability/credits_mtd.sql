
  create or replace  view staging_BALBOA.bay_observability.credits_mtd
  
    
    
(
  
    
      MTD_CREDITS_USED
    
    , 
  
    
      PREVIOUS_MTD_CREDITS_USED
    
    
  
)

  copy grants as (
    select
    credits_used as mtd_credits_used,
    (
        select sum(credits_used) as credits_used_sum
        from
            bay_observability.stg_warehouse_metering_history
        where
            timestampdiff(month, start_time, current_date) = 1
            and day(current_date) >= day(start_time)
    ) as previous_mtd_credits_used
from
    bay_observability.stg_warehouse_metering_history
-- left join prev_month on this_month.start_time = prev_month.pm_start_time
where
    timestampdiff(month, start_time, current_date) = 0
  );
