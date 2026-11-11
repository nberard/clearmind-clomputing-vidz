-- Throughput of every stage of every run, the devices online during it, and
-- the speedup versus the first stage that did any work.
with stages as (
    select * from {{ ref('int_run_stages') }}
),

roster as (
    select
        st.run_id,
        st.stage_num,
        count(*)                                                     as devices,
        sum(case when s.has_gpu then 1 else 0 end)                   as gpu_devices,
        sum(case when s.is_simulated then 1 else 0 end)              as simulated_devices,
        string_agg(s.device_kind, '+' order by s.device_kind)        as device_mix,
        sum(d.units_per_busy_min)                                    as ideal_units_per_min
    from stages st
    join {{ ref('int_device_sessions') }} s
      on s.online_from <= st.stage_start + 0.5
     and (s.online_until is null or s.online_until >= st.stage_end - 0.5)
    left join {{ ref('dim_devices') }} d on d.client_id = s.client_id
    group by st.run_id, st.stage_num
),

work as (
    select
        st.run_id,
        st.stage_num,
        sum(j.units)                                                 as units_measured,
        count(j.job_id)                                              as jobs_measured
    from stages st
    join {{ ref('stg_grid__jobs') }} j
      on j.run_id = st.run_id
     and j.finished_epoch >= st.measure_start
     and j.finished_epoch <  st.stage_end
    group by st.run_id, st.stage_num
),

measured as (
    select
        st.run_id,
        st.stage_num,
        st.stage_start - r.started_epoch                             as starts_after_s,
        st.stage_seconds,
        coalesce(ro.devices, 0)                                      as devices,
        coalesce(ro.gpu_devices, 0)                                  as gpu_devices,
        coalesce(ro.simulated_devices, 0)                            as simulated_devices,
        coalesce(ro.device_mix, 'none')                              as device_mix,
        ro.ideal_units_per_min,
        coalesce(w.jobs_measured, 0)                                 as jobs_measured,
        coalesce(w.units_measured, 0) * 60
            / nullif(st.stage_end - st.measure_start, 0)             as units_per_min
    from stages st
    join {{ ref('stg_grid__runs') }} r using (run_id)
    left join roster ro on ro.run_id = st.run_id and ro.stage_num = st.stage_num
    left join work w on w.run_id = st.run_id and w.stage_num = st.stage_num
),

baseline as (
    -- first stage of each run that did any work
    select m.run_id, m.units_per_min as baseline_units_per_min
    from measured m
    join (
        select run_id, min(stage_num) as stage_num
        from measured
        where units_per_min > 0
        group by run_id
    ) f on f.run_id = m.run_id and f.stage_num = m.stage_num
)

select
    m.*,
    m.units_per_min / nullif(b.baseline_units_per_min, 0)            as speedup,
    -- 1.0 = devices add up perfectly; lower = overhead or idle devices
    m.units_per_min / nullif(m.ideal_units_per_min, 0)               as scaling_efficiency
from measured m
left join baseline b using (run_id)
