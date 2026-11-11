-- One row per device registration with its capabilities and lifetime stats.
with job_stats as (
    select
        client_id,
        count(*)                                     as jobs_done,
        sum(units)                                   as units_done,
        sum(seconds)                                 as busy_seconds,
        sum(case when is_gpu_job then units end)     as gpu_units_done
    from {{ ref('stg_grid__jobs') }}
    group by client_id
)

select
    c.*,
    case
        when c.has_gpu then 'gpu'
        when c.is_browser then 'browser'
        when c.is_native then 'native_cpu'
        else 'other'
    end                                              as device_class,
    coalesce(j.jobs_done, 0)                         as jobs_done,
    coalesce(j.units_done, 0)                        as units_done,
    coalesce(j.busy_seconds, 0)                      as busy_seconds,
    coalesce(j.gpu_units_done, 0)                    as gpu_units_done,
    -- measured speed while working, used for the "ideal" throughput
    j.units_done * 60 / nullif(j.busy_seconds, 0)    as units_per_busy_min
from {{ ref('stg_grid__clients') }} c
left join job_stats j using (client_id)
