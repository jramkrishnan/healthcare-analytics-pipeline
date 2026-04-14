-- mart_patient_summary.sql
-- One row per patient with lifetime clinical metrics.
-- Useful for cohort analysis and patient-level risk stratification.

with features as (
    select * from {{ ref('int_readmission_features') }}
),

patient_level as (
    select
        patient_id,
        age,
        age_group,
        gender,
        race,
        insurance_type,
        medicare_eligible,
        patient_state,

        -- Visit history
        count(admission_id)                              as total_admissions,
        min(admit_date)                                  as first_admission_date,
        max(admit_date)                                  as last_admission_date,
        max(admit_date) - min(admit_date)                as days_in_system,

        -- Readmission stats
        sum(readmitted_30_days::int)                     as total_readmissions,
        round(
            avg(readmitted_30_days::int) * 100, 2
        )                                                as personal_readmission_rate_pct,

        -- LOS and cost
        round(avg(los_days), 2)                          as avg_los,
        sum(los_days)                                    as total_inpatient_days,
        round(avg(total_charges), 2)                     as avg_charge_per_visit,
        sum(total_charges)                               as total_lifetime_charges,

        -- ICU usage
        sum(had_icu_stay::int)                           as icu_visits,
        sum(icu_hours)                                   as total_icu_hours,

        -- Comorbidity burden
        round(avg(comorbidity_count), 2)                 as avg_comorbidity_count,
        max(comorbidity_count)                           as max_comorbidity_count,

        -- Risk
        round(avg(readmission_risk_score), 2)            as avg_risk_score,
        max(readmission_risk_score)                      as peak_risk_score,

        -- Most common primary diagnosis
        mode() within group (order by primary_diagnosis_code)
                                                         as most_common_diagnosis,

        -- High-utiliser flag: top-10% by admission count or total charges
        (count(admission_id) >= 5 or sum(total_charges) >= 100000)
                                                         as high_utiliser

    from features
    group by
        patient_id, age, age_group, gender, race,
        insurance_type, medicare_eligible, patient_state
)

select * from patient_level
order by total_lifetime_charges desc nulls last
