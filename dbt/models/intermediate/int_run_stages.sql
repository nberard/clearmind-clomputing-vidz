-- Split each run into stages: a new stage starts whenever a device joins or
-- leaves. Measurement stops when the queue is drained (idle tail excluded).
with runs as (
    select run_id, started_epoch, drained_epoch
    from {{ ref('stg_grid__runs') }}
),

boundaries as (
    select run_id, started_epoch as boundary from runs
    union all
    select run_id, drained_epoch from runs
    union all
    select r.run_id, e.event_epoch
    from runs r
    join {{ ref('stg_grid__events') }} e
      on e.event_type in ('join', 'leave')
     and e.event_epoch > r.started_epoch
     and e.event_epoch < r.drained_epoch
),

ranges as (
    select
        run_id,
        boundary                                                     as stage_start,
        lead(boundary) over (partition by run_id order by boundary)  as stage_end
    from boundaries
)

select
    run_id,
    row_number() over (partition by run_id order by stage_start)    as stage_num,
    stage_start,
    stage_end,
    stage_end - stage_start                                          as stage_seconds,
    -- measuring window skips the warm-up share of the stage
    stage_start + (stage_end - stage_start) * {{ var('stage_warmup_share') }} as measure_start
from ranges
where stage_end is not null
  and stage_end - stage_start >= 1   -- devices joining together merge into one stage
