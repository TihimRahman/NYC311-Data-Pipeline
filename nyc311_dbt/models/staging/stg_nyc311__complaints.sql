with base as (

    select*from {{ ref('stg_nyc311__base') }}

),

cleaned as(

    select
        trim(complaint_id) as complaint_id,
        
        created_at,
        closed_at,
        resolution_updated_at,

        nullif(trim(agency_code), '')        as agency_code,
        nullif(trim(agency_name), '')        as agency_name,

        initcap(nullif(trim(complaint_type),''))            as complaint_type,
        initcap(nullif(trim(complaint_descriptor),''))      as complaint_descriptor,
        initcap(nullif(trim(status),''))                    as status,
        nullif(trim(resolution_description), '')            as resolution_description,

        nullif(trim(incident_zip), '')                      as incident_zip,
        nullif(trim(incident_address), '')                  as incident_address,
        nullif(trim(street_name), '')                       as street_name,
        nullif(trim(city), '')                              as city,

    
        case 
            when upper(trim(borough)) in ('UNSPECIFIED', '', 'NA', 'N/A') then null
            else initcap(trim(borough))
        end as borough,

        nullif(trim(community_board), '')                   as community_board,
        nullif(trim(council_district), '')                  as council_district,

        case when
            latitude between 40.4 and 41.0 then latitude
            else null
        end as latitude,

        case when
            longitude between -74.3 and -73.7 then longitude
            else null
        end as longitude,

        initcap(nullif(trim(submission_channel),''))        as submission_channel,

        extracted_at,
        source_filename,
        source_relation

        from base

),

deduplicated as (

    select
        *,
        row_number() over (partition by complaint_id order by source_filename desc) as _dedup_rank
    from cleaned
),

filtered as (

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
        extracted_at,
        source_filename,
        source_relation
    from deduplicated
    where _dedup_rank = 1
        and complaint_id is not null
        and created_at is not null
)

select*from filtered