import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplay"
user_table_drop = "DROP TABLE IF EXISTS dim_user"
song_table_drop = "DROP TABLE IF EXISTS dim_song"
artist_table_drop = "DROP TABLE IF EXISTS dim_artist"
time_table_drop = "DROP TABLE IF EXISTS dim_time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events (
    artist VARCHAR ,
    auth VARCHAR ,
    firstName VARCHAR ,
    gender VARCHAR ,
    itemInSession SMALLINT ,
    lastName VARCHAR ,
    length FLOAT ,
    level VARCHAR ,
    location VARCHAR ,
    method VARCHAR ,
    page VARCHAR ,
    registration DECIMAL ,
    sessionId INT ,
    song VARCHAR ,
    status SMALLINT ,
    ts BIGINT ,
    userAgent VARCHAR ,
    userid INT
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    num_songs INT,
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration FLOAT,
    year SMALLINT
);
""")

songplay_table_create = ("""
CREATE TABLE fact_songplay (
    songplay_id BIGINT IDENTITY(0,1) NOT NULL,
    start_time TIMESTAMP,
    user_id INT,
    level VARCHAR,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id INT,
    location VARCHAR,
    user_agent VARCHAR,
    
    PRIMARY KEY (songplay_id),
    FOREIGN KEY (user_id) REFERENCES dim_user(user_id),
    FOREIGN KEY (song_id) REFERENCES dim_song(song_id),
    FOREIGN KEY (artist_id) REFERENCES dim_artist(artist_id)
);
""")

user_table_create = ("""
CREATE TABLE dim_user (
    user_id INT,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    level VARCHAR,
    
    PRIMARY KEY (user_id)
);
""")

song_table_create = ("""
CREATE TABLE dim_song (
    song_id VARCHAR,
    title VARCHAR,
    artist_id VARCHAR,
    year SMALLINT,
    duration FLOAT,
    
    PRIMARY KEY (song_id),
    FOREIGN KEY (artist_id) REFERENCES dim_artist(artist_id)
);
""")

artist_table_create = ("""
CREATE TABLE dim_artist (
    artist_id VARCHAR,
    name VARCHAR,
    location VARCHAR,
    latitude FLOAT,
    longitude FLOAT,
    
    PRIMARY KEY (artist_id)
);
""")

time_table_create = ("""
CREATE TABLE dim_time (
    start_time TIMESTAMP,
    hour SMALLINT,
    day SMALLINT,
    week SMALLINT,
    month SMALLINT,
    year SMALLINT,
    weekday BOOLEAN,
    
    PRIMARY KEY (start_time)
);
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events 
    FROM {}
    iam_role {}
    FORMAT AS json {} 
    REGION {} ;
""").format(
    config['S3']['LOG_DATA'], 
    config['IAM_ROLE']['ARN'], 
    config['S3']['LOG_JSONPATH'],
    config['S3']['REGION']
    )

staging_songs_copy = ("""
    COPY staging_songs 
    FROM {}
    iam_role {}
    FORMAT AS json 'auto'
    REGION {} ;
""").format(
    config['S3']['SONG_DATA'], 
    config['IAM_ROLE']['ARN'],
    config['S3']['REGION']
    )

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO fact_songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
(
    SELECT
    timestamp 'EPOCH' + (e.ts/1000 * INTERVAL '1 second') as start_time,
    e.userid as user_id,
    e.level,
    s.song_id,
    s.artist_id,
    e.sessionId as session_id,
    e.location,
    e.userAgent as user_agent
    FROM staging_events e
    LEFT JOIN staging_songs s 
        ON s.title = e.song AND s.artist_name = e.artist
) 
""")

user_table_insert = ("""
INSERT INTO dim_user (user_id, first_name, last_name, gender, level)
(
    SELECT DISTINCT
        e.userid as user_id, 
        e.firstName,
        e.lastName,
        e.gender,
        e.level
    FROM staging_events e
    WHERE e.userid IS NOT NULL
)
""")

song_table_insert = ("""
INSERT INTO dim_song (song_id, title, artist_id, year, duration)
(
    SELECT DISTINCT
        s.song_id as song_id,
        s.title,
        s.artist_id,
        s.year,
        s.duration
    FROM staging_songs s
)
""")

artist_table_insert = ("""
INSERT INTO dim_artist (artist_id, name, location, latitude, longitude)
(
    SELECT DISTINCT
        s.artist_id,
        s.artist_name as name,
        s.artist_location as location,
        s.artist_latitude as latitude,
        s.artist_longitude as longitude
    FROM staging_songs s
)
""")

time_table_insert = ("""
INSERT INTO dim_time (start_time, hour, day, week, month, year, weekday)
(
    WITH temp AS (
        SELECT DISTINCT 
            timestamp 'EPOCH' + (ts/1000 * INTERVAL '1 second') AS ts
        FROM staging_events
    )
    SELECT
        t.ts as start_time,
        EXTRACT(hour from t.ts) as hour,
        EXTRACT(day from t.ts) as day,
        EXTRACT(week from t.ts) as week,
        EXTRACT(month from t.ts) as month,
        EXTRACT(year from t.ts) as year,
        CASE
            WHEN DATE_PART(dayofweek, t.ts) < 6 
            THEN TRUE ELSE FALSE 
        END AS weekday
    FROM temp t
)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, song_table_drop, artist_table_drop, user_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, time_table_insert, artist_table_insert, song_table_insert, songplay_table_insert]
