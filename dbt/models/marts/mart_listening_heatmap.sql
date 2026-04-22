-- Play counts by hour-of-day and day-of-week. useful for visualising when you listen most
select
    extract(dow from played_at)     as day_of_week,   -- 0=Sunday, 6=Saturday
    extract(hour from played_at)    as hour_of_day,
    count(*)                        as play_count
from {{ ref('int_deduped_play_events') }}
group by day_of_week, hour_of_day
order by day_of_week, hour_of_day
