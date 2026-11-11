-- Throughput samples taken every 5 seconds during a run.
with src as (
    select *
    from {{ source('grid_raw', 'samples') }}
    where true
    qualify row_number() over (partition by _line order by _line) = 1
)

select
    run_id,
    t                                        as sample_epoch,
    {{ epoch_to_ts('t') }}                   as sample_at,
    rate                                     as units_per_min,
    clients                                  as devices_online,
    busy                                     as devices_busy
from src
