-- Snowflake semantic view DDL (tables/dimensions/metrics) is not supported by
-- the sqlfluff snowflake dialect, so linting is disabled for this model.
-- noqa:disable=all


tables (
    active_vendor_periods as BALBOA.L3_ACCOUNTS_PAYABLE.mart_active_vendors_by_period
)

dimensions (
    active_vendor_periods.period_grain as period_grain,
    active_vendor_periods.period_end_date as period_end_date,
    active_vendor_periods.vendor_id as vendor_id,
    active_vendor_periods.vendor_name as vendor_name,
    active_vendor_periods.category as category,
    active_vendor_periods.recorded_status as recorded_status
)

metrics (
    active_vendor_periods.active_vendors_weekly as count(distinct case
        when active_vendor_periods.period_grain = 'week' then active_vendor_periods.vendor_id
    end),
    active_vendor_periods.active_vendors_monthly as count(distinct case
        when active_vendor_periods.period_grain = 'month' then active_vendor_periods.vendor_id
    end),
    active_vendor_periods.active_vendors_quarterly as count(distinct case
        when active_vendor_periods.period_grain = 'quarter' then active_vendor_periods.vendor_id
    end)
)