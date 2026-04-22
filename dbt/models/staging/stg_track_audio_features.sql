with source as (
    select * from {{ source('raw', 'track_audio_features') }}
),
renamed as (
    select
        track_id,
        fetched_at,
        (features ->> 'danceability')::float       as danceability,
        (features ->> 'energy')::float             as energy,
        (features ->> 'valence')::float            as valence,
        (features ->> 'tempo')::float              as tempo,
        (features ->> 'acousticness')::float       as acousticness,
        (features ->> 'instrumentalness')::float   as instrumentalness,
        (features ->> 'speechiness')::float        as speechiness,
        (features ->> 'loudness')::float           as loudness,
        (features ->> 'synthetic')::boolean        as is_synthetic
    from source
)

select * from renamed
