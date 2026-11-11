-- Which device is good at which job: speed per device kind, compute engine
-- (js vs wasm on phones, numpy, torch) and job type.
select
    device_kind,
    device_class,
    compute_engine,
    job_type,
    is_gpu_job,
    is_simulated,
    count(*)                                         as jobs,
    sum(units)                                       as units,
    sum(seconds)                                     as busy_seconds,
    sum(units) * 60 / nullif(sum(seconds), 0)        as units_per_busy_min,
    avg(seconds_per_unit)                            as avg_seconds_per_unit
from {{ ref('fct_jobs') }}
group by 1, 2, 3, 4, 5, 6
