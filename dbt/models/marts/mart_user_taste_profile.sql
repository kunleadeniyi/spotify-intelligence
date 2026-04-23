-- Average and variance of audio features across all plays. Represents the user's overall taste
select
    'default_user'                  as user_id,
    now()                           as computed_at,
    -- adding above columns because feature store needs a time and an entity key. 
    -- In this case the user is the entity and there is only one user as it is a personal project.
    avg(danceability)               as avg_danceability,
    avg(energy)                     as avg_energy,
    avg(valence)                    as avg_valence,
    avg(tempo)                      as avg_tempo,
    avg(acousticness)               as avg_acousticness,
    avg(instrumentalness)           as avg_instrumentalness,
    avg(speechiness)                as avg_speechiness,
    avg(loudness)                   as avg_loudness,
    variance(danceability)          as var_danceability,
    variance(energy)                as var_energy,
    variance(valence)               as var_valence,
    variance(tempo)                 as var_tempo,
    variance(acousticness)          as var_acousticness,
    variance(instrumentalness)      as var_instrumentalness,
    variance(speechiness)           as var_speechiness,
    variance(loudness)              as var_loudness
from {{ ref('mart_user_play_history') }}
where danceability is not null

