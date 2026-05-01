/* need to materialise this because it is used in the dashboard */
{{ config(materialized='view', schema='staging') }}

/*
A new session starts when the gap between consecutive plays exceeds 30 minutes.
*/
with lagged as (
    select
        *,
        lag(played_at) over (order by played_at) as prev_played_at
    from {{ ref('int_deduped_play_events') }}
),

session_starts as (
    select
        *,
        case
            when prev_played_at is null
              or extract(epoch from (played_at - prev_played_at)) > 1800
            then 1
            else 0
        end as is_session_start
    from lagged
),

session_ids as (
    select
        *,
        sum(is_session_start) over (order by played_at rows unbounded preceding) as session_id
    from session_starts
)

select
    event_id,
    track_id,
    track_name,
    primary_artist_id,
    primary_artist_name,
    played_at,
    duration_ms,
    session_id
from session_ids