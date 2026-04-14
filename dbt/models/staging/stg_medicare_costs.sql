-- stg_medicare_costs.sql
-- Cleans Medicare cost data and adds payment efficiency metrics

with source as (
    select * from {{ source('raw', 'medicare_costs') }}
),

cleaned as (
    select
        cost_id,
        hospital_id,
        drg_code,
        drg_description,
        total_discharges,
        avg_covered_charges,
        avg_total_payments,
        avg_medicare_payments,
        year,

        -- Payment ratio: what Medicare pays vs billed charges
        round(
            avg_medicare_payments::numeric / nullif(avg_covered_charges, 0),
            4
        )                                                as medicare_payment_ratio,

        -- Cost vs payment gap (hospital cost exposure)
        round(avg_covered_charges - avg_total_payments, 2) as cost_payment_gap,

        -- Volume tier
        case
            when total_discharges < 50   then 'Low Volume'
            when total_discharges < 200  then 'Mid Volume'
            else                              'High Volume'
        end                                              as volume_tier,

        loaded_at
    from source
    where cost_id is not null
      and avg_covered_charges > 0
)

select * from cleaned
