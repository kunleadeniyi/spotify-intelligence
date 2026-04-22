with source as (
    select * from {{ source('raw', 'artist_genres') }}
),
renamed as (
    select
        artist_id,
        genres,
        fetched_at
    from source
)

select * from renamed
