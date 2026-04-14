-- stg_admissions.sql
-- Cleans admission records and derives key clinical fields

with source as (
    select * from {{ source('raw', 'admissions') }}
),

cleaned as (
    select
        admission_id,
        patient_id,
        hospital_id,
        admit_date,
        discharge_date,
        initcap(admission_type)                          as admission_type,
        upper(trim(primary_diagnosis_code))              as primary_diagnosis_code,
        primary_diagnosis_desc,
        los_days,
        readmitted_30_days,
        initcap(discharge_disposition)                   as discharge_disposition,
        total_charges,
        icu_hours,

        -- Derived booleans
        (icu_hours > 0)                                  as had_icu_stay,
        (los_days > 7)                                   as long_stay_flag,

        -- LOS bucket (used in cost/readmission models)
        case
            when los_days <= 1  then '0-1 days'
            when los_days <= 3  then '2-3 days'
            when los_days <= 7  then '4-7 days'
            when los_days <= 14 then '8-14 days'
            else                     '15+ days'
        end                                              as los_bucket,

        -- Calendar fields useful for time-series analysis
        extract(year  from admit_date)::int              as admit_year,
        extract(month from admit_date)::int              as admit_month,
        extract(quarter from admit_date)::int            as admit_quarter,
        to_char(admit_date, 'YYYY-MM')                   as admit_month_key,

        loaded_at
    from source
    where admission_id is not null
      and admit_date   is not null
      and (discharge_date is null or discharge_date >= admit_date)
)

select * from cleaned
