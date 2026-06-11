with fact_with_dims as(

    select
        f.created_date,
        b.borough_name,
        ct.category,
        f.is_resolved,
        f.is_open,
        f.response_time_hours,
        ct.is_quality_of_life,
        ct.is_emergency
    from {{ ref('fct_complaints') }} f
    join {{ ref('dim_borough') }} b
    on f.borough_sk = b.borough_sk
    join {{ ref('dim_complaint_type') }} ct
    on f.complaint_type_sk = ct.complaint_type_sk
),

aggregated as (
    select

        created_date,
        borough_name,
        category,

        count(*) as total_complaints,
        count_if(is_resolved) as resolved_complaints,
        count_if(is_open) as open_complaints,

        count_if(is_quality_of_life) as quality_of_life_complaints,
        count_if(is_emergency) as emergency_complaints,


        round(100.0 * count_if(is_resolved) / nullif(count(*), 0),1) as pct_resolved,

        round(percentile_cont(0.5) within group (order by response_time_hours),1) as median_response_hours,
        round(percentile_cont(0.9) within group (order by response_time_hours),1) as p90_response_hours,
        max(response_time_hours) as max_response_hours,
        current_timestamp()::timestamp_ntz as report_loaded_at

    from fact_with_dims
    group by created_date, borough_name, category
)
select*from aggregated