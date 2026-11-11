-- One row per device registration (a device gets a new client_id each time it registers).
with src as (
    select *
    from {{ source('grid_raw', 'clients') }}
    where true
    qualify row_number() over (partition by client_id order by _line desc) = 1
)

select
    client_id,
    name                                     as device_name,
    kind                                     as device_kind,
    joined                                   as joined_epoch,
    {{ epoch_to_ts('joined') }}              as joined_at,
    cpu_score,
    coalesce(has_gpu, false)                 as has_gpu,
    gpu_name,
    vram_gb,
    gpu_score,
    coalesce(native, false)                  as is_native,
    coalesce(is_browser, false)              as is_browser,
    coalesce(is_simulated, false)            as is_simulated,
    cores,
    ram_gb,
    os,
    engine                                   as compute_engine
from src
