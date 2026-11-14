-- Liked songs.
select distinct artist_name, album_name, track_name, track_uri
from {{ source('grid_raw', 'spotify_library_tracks') }}
where track_name is not null
