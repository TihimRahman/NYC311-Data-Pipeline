
with complaints as (

    select*
    from {{ ref('stg_nyc311__complaints') }}
),
enriched as(
    select
        complaint_id,
        created_at,
        closed_at,
        resolution_updated_at,
        agency_code,
        agency_name,
        complaint_type,
        complaint_descriptor,
        status,
        resolution_description,
        incident_zip,
        incident_address,
        street_name,
        city,
        borough,
        community_board,
        council_district,
        latitude,
        longitude,
        submission_channel,
        
        cast(created_at as date) as created_date,
        date_trunc('month', created_at)::date  as created_month,
        date_trunc('week',  created_at)::date  as created_week,
        extract(year from created_at)          as created_year,
        extract(hour from created_at)          as created_hour,
        dayname(created_at)                    as created_day_of_week,

        case 
            when closed_at is not null and closed_at >=created_at 
                then datediff('hour', created_at, closed_at)
            else null
            end as response_time_hours,
        
        case 
            when closed_at is not null and closed_at >=created_at 
                then datediff('day', created_at, closed_at)
            else null
            end as response_time_days,
        
        case
            when status in ('Closed', 'Resolved') and closed_at is not null
                then true
            else false
            end as is_resolved,

        case 
            when status in ('Open', 'Pending', 'Started', 'In Progress', 'Assigned') 
            or closed_at is null
                then true
            else false
            end as is_open,

        datediff('day', created_at, current_timestamp()) as days_since_created,

        case
            when datediff('day', created_at, current_timestamp()) = 0 then 'Same Day' 
            when datediff('day', created_at, current_timestamp()) <= 7 then 'Same Week'
            when datediff('day', created_at, current_timestamp()) <= 30 then 'Within Month'
            when datediff('day', created_at, current_timestamp()) <= 90 then 'Within Quarter'
        else 'Over Quarter'
        end as complaint_age_bucket,


        case
            when latitude is not null and longitude is not null
                then true
            else false
            end as has_geolocation,

        case 
            when borough is not null
                then true
            else false
            end as has_borough,

        extracted_at,
        source_filename,
        source_relation

    from complaints
)
select*from enriched