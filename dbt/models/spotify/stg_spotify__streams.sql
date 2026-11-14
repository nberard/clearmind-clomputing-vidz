-- One row per play from the Spotify export. Plays under 30 s are skips.
select
    {{ epoch_to_ts('played_at') }}           as played_at,
    artist_name,
    track_name,
    ms_played,
    ms_played / 60000.0                      as minutes_played,
    ms_played >= 30000                       as is_real_listen
from {{ source('grid_raw', 'spotify_streams') }}
where played_at is not null
  and artist_name is not null
