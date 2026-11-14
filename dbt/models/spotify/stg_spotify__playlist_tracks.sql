-- Tracks in your playlists. The export has no genres, so a playlist whose
-- name mentions a reggae style flags its tracks as reggae.
select
    playlist_name,
    position,
    artist_name,
    album_name,
    track_name,
    track_uri,
    {{ regex_match('lower(playlist_name)', var('reggae_playlist_pattern')) }} as is_reggae_playlist
from {{ source('grid_raw', 'spotify_playlist_tracks') }}
where track_name is not null
