-- Who you really listen to (skips under 30 s excluded).
select
    artist_name,
    count(*)                                         as plays,
    count(distinct track_name)                       as tracks,
    round(sum(minutes_played), 1)                    as minutes,
    min(played_at)                                   as first_played,
    max(played_at)                                   as last_played
from {{ ref('stg_spotify__streams') }}
where is_real_listen
group by artist_name
