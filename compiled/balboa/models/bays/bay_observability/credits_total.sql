select sum(credits_used) as credits_used
from
    
    
        BALBOA.bay_observability.stg_warehouse_metering_history
    

where
    timestampdiff(month, start_time, current_date) <= 12 and timestampdiff(month, start_time, current_date) > 0