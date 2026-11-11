-- One row per inspiration post saved in an Instagram collection.
with src as (
    select *
    from {{ source('grid_raw', 'sources') }}
    where true
    qualify row_number() over (partition by media_id order by _line) = 1
)

select
    media_id,
    code,
    collection_id,
    collection_name,
    case media_type when 1 then 'photo' when 2 then 'video' when 8 then 'carousel' else 'other' end as media_kind,
    author,
    caption,
    like_count,
    {{ epoch_to_ts('taken_at') }}            as posted_at,
    {{ epoch_to_ts('received_at') }}         as received_at,
    has_image
from src
