-- int_patient_admissions.sql
-- Joins admissions with patient demographics and hospital attributes.
-- Used as the base for both readmission and cost marts.

with admissions as (
    select * from {{ ref('stg_admissions') }}
),

patients as (
    select * from {{ ref('stg_patients') }}
),

hospitals as (
    select * from {{ ref('stg_hospitals') }}
),

joined as (
    select
        -- Admission keys
        a.admission_id,
        a.admit_date,
        a.discharge_date,
        a.admit_year,
        a.admit_month,
        a.admit_quarter,
        a.admit_month_key,
        a.admission_type,
        a.primary_diagnosis_code,
        a.primary_diagnosis_desc,
        a.los_days,
        a.los_bucket,
        a.readmitted_30_days,
        a.discharge_disposition,
        a.total_charges,
        a.icu_hours,
        a.had_icu_stay,
        a.long_stay_flag,

        -- Patient attributes
        p.patient_id,
        p.age,
        p.age_group,
        p.gender,
        p.race,
        p.insurance_type,
        p.medicare_eligible,
        p.state                                          as patient_state,

        -- Hospital attributes
        h.hospital_id,
        h.hospital_name,
        h.state                                          as hospital_state,
        h.hospital_type,
        h.hospital_size,
        h.teaching_flag,
        h.bed_count

    from admissions a
    left join patients  p using (patient_id)
    left join hospitals h using (hospital_id)
)

select * from joined
