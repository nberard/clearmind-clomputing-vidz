-- When you listen: by month and hour of day (UTC), for the "summer vibe".
select
    extract(month from played_at)                    as month_num,
    extract(hour from played_at)                     as hour_utc,
    count(*)                                         as plays,
    round(sum(minutes_played), 1)                    as minutes
from {{ ref('stg_spotify__streams') }}
where is_real_listen
group by 1, 2
