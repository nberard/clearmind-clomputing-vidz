-- Device joins/leaves and run lifecycle events.
with src as (
    select *
    from {{ source('grid_raw', 'events') }}
    where true
    qualify row_number() over (partition by _line order by _line) = 1
)

select
    _line                                    as event_id,
    run_id,
    t                                        as event_epoch,
    {{ epoch_to_ts('t') }}                   as event_at,
    type                                     as event_type,
    client_id,
    online                                   as devices_online,
    text
from src
