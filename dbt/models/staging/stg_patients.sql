-- stg_patients.sql
-- Cleans patient demographics and adds age bucket

with source as (
    select * from {{ source('raw', 'patients') }}
),

cleaned as (
    select
        patient_id,
        age,
        upper(gender)                                    as gender,
        initcap(race)                                    as race,
        initcap(insurance_type)                          as insurance_type,
        zip_code,
        upper(state)                                     as state,

        -- Age stratification used in readmission models
        case
            when age < 18  then 'Pediatric'
            when age < 45  then 'Adult'
            when age < 65  then 'Middle-Aged'
            when age < 75  then 'Senior'
            else                'Elderly'
        end                                              as age_group,

        -- Medicare-eligible flag (65+)
        (age >= 65)                                      as medicare_eligible,

        loaded_at
    from source
    where patient_id is not null
      and age between 0 and 120
)

select * from cleaned
