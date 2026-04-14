-- int_readmission_features.sql
-- Builds a feature-rich table per admission for readmission risk analysis.
-- Computes comorbidity count, prior admission history, and risk score.

with base as (
    select * from {{ ref('int_patient_admissions') }}
),

-- Count secondary diagnoses per admission (proxy for comorbidity burden)
comorbidity_counts as (
    select
        admission_id,
        count(*)                                         as comorbidity_count
    from {{ ref('stg_diagnoses') }}
    group by 1
),

-- Count prior admissions per patient (lookback over all data)
prior_admissions as (
    select
        patient_id,
        admit_date,
        count(*) over (
            partition by patient_id
            order by admit_date
            rows between unbounded preceding and 1 preceding
        )                                                as prior_admission_count
    from base
),

-- Mark if patient was discharged to a skilled nursing facility or similar
high_risk_disposition as (
    select
        admission_id,
        discharge_disposition in ('Snf','Rehab','Home Health')
                                                         as complex_discharge
    from base
),

enriched as (
    select
        b.*,
        coalesce(cc.comorbidity_count, 0)                as comorbidity_count,
        coalesce(pa.prior_admission_count, 0)            as prior_admission_count,
        coalesce(hrd.complex_discharge, false)           as complex_discharge,

        -- Simple weighted readmission risk score (0–100)
        round(
            least(100,
                (coalesce(cc.comorbidity_count, 0)  * 4.0)
              + (coalesce(pa.prior_admission_count, 0) * 3.0)
              + (b.los_days                          * 1.5)
              + (case when b.had_icu_stay       then 10 else 0 end)
              + (case when b.medicare_eligible  then  8 else 0 end)
              + (case when b.long_stay_flag     then  5 else 0 end)
              + (case when coalesce(hrd.complex_discharge, false) then 6 else 0 end)
            )
        , 1)                                             as readmission_risk_score,

        -- Risk tier
        case
            when (coalesce(cc.comorbidity_count, 0)  * 4.0)
               + (coalesce(pa.prior_admission_count, 0) * 3.0)
               + (b.los_days * 1.5)
               + (case when b.had_icu_stay then 10 else 0 end)
               + (case when b.medicare_eligible then 8 else 0 end) < 20
                                                         then 'Low'
            when (coalesce(cc.comorbidity_count, 0)  * 4.0)
               + (coalesce(pa.prior_admission_count, 0) * 3.0)
               + (b.los_days * 1.5)
               + (case when b.had_icu_stay then 10 else 0 end)
               + (case when b.medicare_eligible then 8 else 0 end) < 50
                                                         then 'Medium'
            else                                              'High'
        end                                              as risk_tier

    from base               b
    left join comorbidity_counts    cc  using (admission_id)
    left join prior_admissions      pa  using (patient_id, admit_date)
    left join high_risk_disposition hrd using (admission_id)
)

select * from enriched
