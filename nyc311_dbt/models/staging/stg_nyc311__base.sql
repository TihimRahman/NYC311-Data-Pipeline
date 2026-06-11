
with source as(

    select
        raw_data,
        filename,
        _dbt_source_relation,
        _dbt_extracted_at
    from {{ source('nyc311', 'complaints_raw') }}


),

flattened as(

    select
        raw_data:unique_key::string as complaint_id,
        
        try_to_timestamp_ntz(raw_data:created_date::string) as created_at,
        try_to_timestamp_ntz(raw_data:closed_date::string)  as closed_at,
        try_to_timestamp_ntz(raw_data:resolution_action_updated_date::string) as resolution_updated_at,

        raw_data:agency::string                             as agency_code,
        raw_data:agency_name::string                        as agency_name,

        raw_data:complaint_type::string                     as complaint_type,
        raw_data:descriptor::string                         as complaint_descriptor,
        raw_data:status::string                             as status,
        raw_data:resolution_description::string             as resolution_description,

        raw_data:incident_zip::string                       as incident_zip,
        raw_data:incident_address::string                   as incident_address,
        raw_data:street_name::string                        as street_name,
        raw_data:city::string                               as city,
        raw_data:borough::string                            as borough,
        raw_data:community_board::string                    as community_board,
        raw_data:council_district::string                   as council_district,

        try_to_decimal(raw_data:latitude::string, 18, 12)   as latitude,
        try_to_decimal(raw_data:longitude::string, 18, 12)  as longitude,

        raw_data:open_data_channel_type::string             as submission_channel,

        _dbt_extracted_at                                   as extracted_at,
        filename                                            as source_filename,
        _dbt_source_relation                                as source_relation

    from source

)

select*from flattened