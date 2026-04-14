-- mart_readmission_analysis.sql
-- Hospital-level 30-day readmission rates by diagnosis, risk tier, and time.
-- This is the primary table for the readmission KPI dashboard.

with features as (
    select * from {{ ref('int_readmission_features') }}
),

-- Hospital-level readmission summary
hospital_summary as (
    select
        hospital_id,
        hospital_name,
        hospital_state,
        hospital_type,
        hospital_size,
        teaching_flag,
        admit_year,
        admit_quarter,

        count(*)                                              as total_admissions,
        sum(readmitted_30_days::int)                          as readmissions,
        round(
            avg(readmitted_30_days::int) * 100, 2
        )                                                     as readmission_rate_pct,

        round(avg(los_days), 2)                               as avg_los_days,
        round(avg(total_charges), 2)                          as avg_charges,
        round(avg(readmission_risk_score), 2)                 as avg_risk_score,

        sum(case when risk_tier = 'High'   then 1 else 0 end) as high_risk_patients,
        sum(case when risk_tier = 'Medium' then 1 else 0 end) as medium_risk_patients,
        sum(case when risk_tier = 'Low'    then 1 else 0 end) as low_risk_patients,

        round(avg(comorbidity_count), 2)                      as avg_comorbidities,
        sum(had_icu_stay::int)                                as icu_admissions,
        round(avg(icu_hours) filter (where had_icu_stay), 1)  as avg_icu_hours

    from features
    group by 1,2,3,4,5,6,7,8
),

-- Diagnosis-level breakdown
diagnosis_summary as (
    select
        primary_diagnosis_code,
        primary_diagnosis_desc,
        admit_year,

        count(*)                                              as total_admissions,
        sum(readmitted_30_days::int)                          as readmissions,
        round(avg(readmitted_30_days::int) * 100, 2)          as readmission_rate_pct,
        round(avg(los_days), 2)                               as avg_los_days,
        round(avg(total_charges), 2)                          as avg_charges

    from features
    group by 1,2,3
),

-- Overall aggregate for top-line metrics
overall as (
    select
        admit_year,
        admit_month_key,
        count(*)                                              as total_admissions,
        sum(readmitted_30_days::int)                          as readmissions,
        round(avg(readmitted_30_days::int) * 100, 2)          as readmission_rate_pct,
        round(avg(total_charges), 2)                          as avg_charges
    from features
    group by 1,2
)

-- Final mart: hospital + year + quarter grain
select
    hs.*,
    -- National context: overall readmission rate for the same year
    o.readmission_rate_pct                                    as national_avg_readmission_rate,
    hs.readmission_rate_pct - o.readmission_rate_pct          as vs_national_avg

from hospital_summary hs
left join overall o
    on hs.admit_year     = o.admit_year
    and o.admit_month_key = hs.admit_year::text || '-06'   -- mid-year snapshot

order by admit_year, readmission_rate_pct desc
