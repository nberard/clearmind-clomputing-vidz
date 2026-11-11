-- One row per finished pipeline run. Raw loads are append-only, so keep the latest line per key.
with src as (
    select *
    from {{ source('grid_raw', 'runs') }}
    where true
    qualify row_number() over (partition by run_id order by _line desc) = 1
)

select
    run_id,
    preset,
    started                                  as started_epoch,
    finished                                 as finished_epoch,
    coalesce(drained, finished)              as drained_epoch,
    {{ epoch_to_ts('started') }}             as started_at,
    {{ epoch_to_ts('finished') }}            as finished_at,
    finished - started                       as duration_s,
    total_units,
    jobs                                     as job_count
from src
