
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
            (
                timestampdiff(year, current_date, start_time) = 0 --year(current_date) = year(start_time)
                and timestampdiff(month, start_time, current_date) = 1 --month(current_date) - 1 = month(start_time)
                and timestampdiff(day, start_time, current_date) >= 0 --day(current_date) >= day(start_time)
            )
            or (
                timestampdiff(year, current_date, start_time) = -1 --year(current_date) - 1 = year(start_time) --
                and month(current_date) - 1 = 0
                and month(start_time) = 12
                and day(current_date) >= day(start_time)
            )
    ) as previous_mtd_credits_used
from

    
        bay_observability.stg_warehouse_metering_history
    

-- left join prev_month on this_month.start_time = prev_month.pm_start_time
where
    year(current_date) = year(start_time)
    and month(current_date) = month(start_time)
  );
