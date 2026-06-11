with type_metadata as(

    select
        complaint_type,
        category,
        is_quality_of_life,
        is_emergency

    from {{ ref('complaint_type_lookup') }}

),

observed_types as (

    select distinct
        complaint_type
    from {{ ref('stg_nyc311__complaints')}}
    where complaint_type is not null

),

joined as (

    select
        o.complaint_type,
        meta.category,
        meta.is_quality_of_life,
        meta.is_emergency,
        case
            when meta.complaint_type is null then true
            else false
        end as is_missing_metadata

    from observed_types o
    left join type_metadata meta
    on o.complaint_type = meta.complaint_type

),

unknown as (

    select
        complaint_type,
        coalesce(category, 'Other')         as category,
        coalesce(is_quality_of_life, false) as is_quality_of_life,
        coalesce(is_emergency, false)       as is_emergency,
        is_missing_metadata 
    from joined
    union all
    select
        'Unknown',
        'Other',  
        false,     
        false,
        false    
),

final as(

    select
        {{ dbt_utils.generate_surrogate_key(['complaint_type']) }} as complaint_type_sk,

        complaint_type,

        category,
        is_quality_of_life,
        is_emergency,

        is_missing_metadata,

        current_timestamp()::timestamp_ntz as dim_loaded_at

    from unknown
)

select*from final