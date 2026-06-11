
{{
    config(
        materialized='incremental',
        unique_key='complaint_id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}


with enriched as (

    select*
    from {{ ref('int_complaints__enriched') }}



    {% if is_incremental() %}
    where extracted_at > (select coalesce(max(extracted_at), '1900-01-01'::timestamp_ntz) from {{ this }})
    {% endif %}

),


dim_borough_lookup as (

    select
        borough_sk,
        borough_name

        from {{ ref('dim_borough') }}

),

dim_complaint_type_lookup as (

    select
        complaint_type_sk,
        complaint_type
    from {{ ref('dim_complaint_type') }}

),

unknown_borough_sk as (

    select
        borough_sk

    from {{ ref('dim_borough') }}
    where borough_name = 'Unknown'

),

unknown_complaint_type_sk as (

    select complaint_type_sk
    from {{ ref('dim_complaint_type') }}
    where complaint_type = 'Unknown'

),

joined as (

    select

        {{ dbt_utils.generate_surrogate_key(['e.complaint_id']) }} as complaint_sk,
        coalesce(b.borough_sk, ub.borough_sk) as borough_sk,
        coalesce(ct.complaint_type_sk, uct.complaint_type_sk) as complaint_type_sk,

        e.complaint_id,
        e.agency_code,
        e.agency_name,
        e.status,
        e.submission_channel,
        e.incident_zip,
        e.incident_address,
        e.community_board,
        e.council_district,
        e.latitude,
        e.longitude,
        e.created_at,
        e.created_date,
        e.created_week,
        e.created_month,
        e.created_year,
        e.created_hour,
        e.created_day_of_week,
        e.closed_at,
        e.resolution_updated_at,
        e.response_time_hours,
        e.response_time_days,
        e.days_since_created,
        e.is_resolved,
        e.is_open,
        e.has_geolocation,
        e.has_borough,
        e.complaint_age_bucket,
        e.resolution_description,
        e.extracted_at,
        current_timestamp()::timestamp_ntz as fct_loaded_at

    from enriched e
    left join dim_borough_lookup b
    on e.borough = b.borough_name
    left join dim_complaint_type_lookup ct
    on e.complaint_type = ct.complaint_type 
    cross join unknown_borough_sk ub
    cross join unknown_complaint_type_sk uct

)
select*from joined
