select track_id
from {{ ref('stg_track_audio_features') }}
where
    danceability    < 0 or danceability    > 1 or
    energy          < 0 or energy          > 1 or
    valence         < 0 or valence         > 1 or
    acousticness    < 0 or acousticness    > 1 or
    instrumentalness < 0 or instrumentalness > 1 or
    speechiness     < 0 or speechiness     > 1 or
    tempo           < 0
