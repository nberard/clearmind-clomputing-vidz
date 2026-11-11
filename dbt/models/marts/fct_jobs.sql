-- One row per completed job, with the device that ran it.
select
    j.*,
    d.device_name,
    d.device_kind,
    d.device_class,
    d.compute_engine,
    d.is_simulated,
    j.seconds / nullif(j.units, 0)                   as seconds_per_unit
from {{ ref('stg_grid__jobs') }} j
left join {{ ref('dim_devices') }} d using (client_id)
