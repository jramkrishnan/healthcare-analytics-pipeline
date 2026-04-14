-- stg_hospitals.sql
-- Cleans and standardises raw hospital data

with source as (
    select * from {{ source('raw', 'hospitals') }}
),

cleaned as (
    select
        hospital_id,
        trim(hospital_name)                              as hospital_name,
        upper(trim(state))                               as state,
        trim(city)                                       as city,
        bed_count,
        hospital_type,
        teaching_flag,

        -- Derived size bucket for easier analysis
        case
            when bed_count < 100  then 'Small'
            when bed_count < 300  then 'Medium'
            when bed_count < 600  then 'Large'
            else                       'Extra-Large'
        end                                              as hospital_size,

        loaded_at
    from source
    where hospital_id is not null
)

select * from cleaned
