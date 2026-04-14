-- mart_cost_analysis.sql
-- Joins Medicare cost data with hospital attributes for cost-efficiency analysis.
-- Surfaces DRG-level benchmarks, payment gaps, and cost outliers.

with costs as (
    select * from {{ ref('stg_medicare_costs') }}
),

hospitals as (
    select * from {{ ref('stg_hospitals') }}
),

-- DRG national averages (benchmark)
drg_benchmarks as (
    select
        drg_code,
        drg_description,
        year,
        round(avg(avg_covered_charges), 2)              as national_avg_charges,
        round(avg(avg_total_payments), 2)               as national_avg_payments,
        round(avg(avg_medicare_payments), 2)            as national_avg_medicare,
        sum(total_discharges)                           as national_total_discharges
    from costs
    group by 1,2,3
),

-- Hospital-level cost efficiency
hospital_costs as (
    select
        c.hospital_id,
        h.hospital_name,
        h.state,
        h.hospital_type,
        h.hospital_size,
        h.teaching_flag,
        c.drg_code,
        c.drg_description,
        c.year,
        c.total_discharges,
        c.avg_covered_charges,
        c.avg_total_payments,
        c.avg_medicare_payments,
        c.medicare_payment_ratio,
        c.cost_payment_gap,
        c.volume_tier,

        -- Benchmarks from national rollup
        b.national_avg_charges,
        b.national_avg_payments,
        b.national_avg_medicare,

        -- How much this hospital charges vs national average
        round(c.avg_covered_charges - b.national_avg_charges, 2)        as charge_vs_national,
        round(
            (c.avg_covered_charges - b.national_avg_charges)
            / nullif(b.national_avg_charges, 0) * 100
        , 2)                                                             as charge_pct_vs_national,

        -- Payment efficiency flag
        case
            when c.avg_covered_charges > b.national_avg_charges * 1.25
            then 'High Cost Outlier'
            when c.avg_covered_charges < b.national_avg_charges * 0.75
            then 'Low Cost Outlier'
            else 'Within Benchmark'
        end                                                              as cost_efficiency_flag,

        -- Revenue exposure per discharge
        round(c.avg_covered_charges - c.avg_total_payments, 2)          as write_off_per_discharge,
        round(
            (c.avg_covered_charges - c.avg_total_payments)
            * c.total_discharges
        , 2)                                                             as total_write_off_estimate

    from costs      c
    left join hospitals     h using (hospital_id)
    left join drg_benchmarks b using (drg_code, year)
)

select * from hospital_costs
order by year, total_write_off_estimate desc nulls last
