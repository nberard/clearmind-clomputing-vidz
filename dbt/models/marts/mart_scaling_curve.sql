-- The headline graph: throughput and speedup by number of devices online,
-- across all runs. Split by GPU count and by real vs simulated devices.
select
    devices,
    gpu_devices,
    case when simulated_devices = devices then 'simulated'
         when simulated_devices = 0 then 'real'
         else 'mixed' end                            as device_source,
    count(distinct run_id)                           as runs,
    count(*)                                         as stages,
    avg(units_per_min)                               as avg_units_per_min,
    avg(speedup)                                     as avg_speedup,
    max(speedup)                                     as max_speedup,
    avg(scaling_efficiency)                          as avg_scaling_efficiency
from {{ ref('fct_stage_throughput') }}
where devices > 0
group by 1, 2, 3
