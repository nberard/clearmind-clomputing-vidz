-- How the inspiration corpus grows: new saved posts per day and collection.
select
    cast(received_at as date)                        as received_date,
    collection_name,
    count(*)                                         as new_posts,
    sum(case when media_kind = 'video' then 1 else 0 end) as videos,
    count(distinct author)                           as authors
from {{ ref('stg_grid__sources') }}
group by 1, 2
