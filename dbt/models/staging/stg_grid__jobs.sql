-- One row per completed job.
with src as (
    select *
    from {{ source('grid_raw', 'jobs') }}
    where true
    qualify row_number() over (partition by job_id order by _line desc) = 1
)

select
    job_id,
    run_id,
    type                                     as job_type,
    units,
    coalesce(gpu_job, false)                 as is_gpu_job,
    client_id,
    started                                  as started_epoch,
    finished                                 as finished_epoch,
    {{ epoch_to_ts('finished') }}            as finished_at,
    seconds,
    units / nullif(seconds, 0)               as units_per_second
from src
