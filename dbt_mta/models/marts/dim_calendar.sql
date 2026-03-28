with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast(current_date() as date)"
    ) }}
),

enriched as (
    select
        cast(date_day as date) as date_day,
        extract(year from date_day) as year,
        extract(quarter from date_day) as quarter,
        extract(month from date_day) as month,
        format_date('%B', date_day) as month_name,
        format_date('%A', date_day) as day_name,
        extract(dayofweek from date_day) in (1, 7) as is_weekend
    from date_spine
)

select * from enriched
