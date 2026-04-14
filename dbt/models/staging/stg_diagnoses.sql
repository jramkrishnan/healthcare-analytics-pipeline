-- stg_diagnoses.sql
-- Cleans secondary diagnosis codes

with source as (
    select * from {{ source('raw', 'diagnoses') }}
),

cleaned as (
    select
        diagnosis_id,
        admission_id,
        upper(trim(icd_code))                            as icd_code,
        icd_description,
        diagnosis_rank,
        diagnosis_type,

        -- ICD-10 chapter derived from first character (high-level grouping)
        case left(upper(trim(icd_code)), 1)
            when 'A' then 'Infectious & Parasitic'
            when 'B' then 'Infectious & Parasitic'
            when 'C' then 'Neoplasms'
            when 'D' then 'Blood & Immune'
            when 'E' then 'Endocrine & Metabolic'
            when 'F' then 'Mental & Behavioral'
            when 'G' then 'Nervous System'
            when 'H' then 'Eye/Ear'
            when 'I' then 'Circulatory System'
            when 'J' then 'Respiratory System'
            when 'K' then 'Digestive System'
            when 'L' then 'Skin & Subcutaneous'
            when 'M' then 'Musculoskeletal'
            when 'N' then 'Genitourinary'
            when 'O' then 'Pregnancy & Childbirth'
            when 'P' then 'Perinatal'
            when 'Q' then 'Congenital Malformations'
            when 'R' then 'Symptoms & Signs'
            when 'S' then 'Injury & Trauma'
            when 'T' then 'Injury & Trauma'
            when 'Z' then 'Health Status Factors'
            else          'Other'
        end                                              as icd_chapter,

        loaded_at
    from source
    where diagnosis_id is not null
)

select * from cleaned
