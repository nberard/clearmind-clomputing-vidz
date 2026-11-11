-- When each device was online: from a join event to its next leave (open if still online).
with joins_leaves as (
    select
        client_id,
        event_type,
        event_epoch,
        lead(event_type)  over (partition by client_id order by event_epoch, event_id) as next_type,
        lead(event_epoch) over (partition by client_id order by event_epoch, event_id) as next_epoch
    from {{ ref('stg_grid__events') }}
    where event_type in ('join', 'leave')
      and client_id is not null
)

select
    s.client_id,
    s.event_epoch                                             as online_from,
    case when s.next_type = 'leave' then s.next_epoch end     as online_until,
    c.device_kind,
    c.has_gpu,
    c.is_simulated
from joins_leaves s
join {{ ref('stg_grid__clients') }} c using (client_id)
where s.event_type = 'join'
