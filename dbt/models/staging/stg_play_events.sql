with source as (
    select * from {{ source('raw', 'play_events') }}
),
renamed as (
    select 
        event_id,
        track_id,
        played_at,
        ingested_at,
        raw_payload -> 'track' ->> 'name'                       as track_name,
        raw_payload -> 'track' -> 'album' ->> 'name'            as album_name,
        raw_payload -> 'track' -> 'album' ->> 'release_date'    as album_release_date,
        (raw_payload -> 'track' ->> 'duration_ms')::int         as duration_ms,
        (raw_payload -> 'track' ->> 'popularity')::int          as popularity,
        (raw_payload -> 'track' ->> 'explicit')::boolean        as is_explicit,
        raw_payload -> 'track' -> 'artists' -> 0 ->> 'id'       as primary_artist_id,
        raw_payload -> 'track' -> 'artists' -> 0 ->> 'name'     as primary_artist_name
    from source
)
select * from renamed