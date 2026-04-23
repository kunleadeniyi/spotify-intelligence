-- no_future_timestamps
select event_id
from {{ ref('stg_play_events') }}
where played_at > now()
