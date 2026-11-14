-- Style references for the original music generation: your most replayed
-- tracks, with library and playlist signals. Only these metadata guide the
-- style profile; nothing from the tracks themselves goes into the video.
with plays as (
    select artist_name, track_name,
           count(*) as plays, round(sum(minutes_played), 1) as minutes,
           max(played_at) as last_played
    from {{ ref('stg_spotify__streams') }}
    where is_real_listen
    group by 1, 2
),

playlists as (
    select artist_name, track_name,
           count(distinct playlist_name)                          as playlist_count,
           max(case when is_reggae_playlist then 1 else 0 end)   as in_reggae_playlist
    from {{ ref('stg_spotify__playlist_tracks') }}
    group by 1, 2
),

liked as (
    select distinct artist_name, track_name, 1 as is_liked
    from {{ ref('stg_spotify__library') }}
),

tracks as (
    select artist_name, track_name from plays
    union distinct select artist_name, track_name from playlists
    union distinct select artist_name, track_name from liked
)

select
    t.artist_name,
    t.track_name,
    coalesce(p.plays, 0)                             as plays,
    coalesce(p.minutes, 0)                           as minutes,
    p.last_played,
    coalesce(l.is_liked, 0) = 1                      as is_liked,
    coalesce(pl.playlist_count, 0)                   as playlist_count,
    coalesce(pl.in_reggae_playlist, 0) = 1           as in_reggae_playlist,
    -- simple affinity score: replays weigh most, then likes and playlists
    coalesce(p.plays, 0) + 5 * coalesce(l.is_liked, 0) + 3 * coalesce(pl.playlist_count, 0) as affinity_score
from tracks t
left join plays p     on p.artist_name = t.artist_name and p.track_name = t.track_name
left join playlists pl on pl.artist_name = t.artist_name and pl.track_name = t.track_name
left join liked l     on l.artist_name = t.artist_name and l.track_name = t.track_name
