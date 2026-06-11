with borough_metadata as(
    select
        borough_name as borough_name,
        borough_code,
        county_name,
        population_2020,
        area_sq_mi
    from {{ ref('borough_lookup') }}
),

observed_boroughs as (

     select distinct
        borough as borough_name
    from {{ ref('stg_nyc311__complaints') }}
    where borough is not null
),

joined as (

    select 
        o.borough_name,
        meta.borough_code,
        meta.county_name,
        meta.population_2020,
        meta.area_sq_mi,
        case 
            when meta.borough_name is null then true
            else false
        end as is_missing_metadata
    from observed_boroughs o
    left join borough_metadata meta
    on o.borough_name = meta.borough_name
),

unknown_data as (

    select
        borough_name,
        borough_code,
        county_name,
        population_2020,
        area_sq_mi,
        is_missing_metadata
    from joined
    union all
    select
        'Unknown',
        'UN',
        'Unknown',
        null,
        null,
        false
),

final as (


    select
        {{ dbt_utils.generate_surrogate_key(['borough_name']) }} as borough_sk,
        
        borough_name,
        borough_code,
        county_name,
        population_2020,
        area_sq_mi,

        case 
            when area_sq_mi is not null and area_sq_mi > 0
            then round(population_2020/area_sq_mi, 0)
            else null
        end as population_density_per_sq_mi,

        is_missing_metadata,

        current_timestamp()::timestamp_ntz as dim_loaded_at
    from unknown_data

 
)
select*from final