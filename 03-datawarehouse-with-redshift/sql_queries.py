import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA = config.get('S3','LOG_DATA')
SONG_DATA = config.get('S3', 'SONG_DATA')
ARN = config.get('IAM_ROLE', 'ARN')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
REGION = config.get('S3', 'REGION')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender CHAR,
    itemInSession INTEGER,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    sessionId INTEGER,
    song VARCHAR,
    status INTEGER,
    ts BIGINT,
    userAgent VARCHAR,
    userId INTEGER
);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(
    song_id VARCHAR PRIMARY KEY, 
    artist_id VARCHAR, 
    artist_latitude FLOAT, 
    artist_location VARCHAR, 
    artist_longitude FLOAT, 
    artist_name VARCHAR, 
    duration FLOAT, 
    num_songs INTEGER, 
    title VARCHAR, 
    year INTEGER)
""")

#facttable
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(
    songplay_id int IDENTITY PRIMARY KEY, 
    start_time timestamp NOT NULL REFERENCES time(start_time) SORTKEY, 
    user_id int NOT NULL REFERENCES users(user_id), 
    level text NOT NULL, 
    song_id text NOT NULL REFERENCES songs(song_id), 
    artist_id text NOT NULL REFERENCES artists(artist_id), 
    session_id int NOT NULL, 
    location text NOT NULL, 
    user_agent text NOT NULL)
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
    user_id int PRIMARY KEY, 
    first_name text NOT NULL, 
    last_name text NOT NULL, 
    gender text NOT NULL, 
    level text NOT NULL)
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
    song_id text PRIMARY KEY, 
    title text NOT NULL, 
    artist_id text NOT NULL, 
    year int NOT NULL, 
    duration float NOT NULL)
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists
    (
        artist_id VARCHAR PRIMARY KEY ,
        name VARCHAR,
        location VARCHAR NULL,
        latitude FLOAT NULL,
        longitude FLOAT NULL
    )
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
    start_time  TIMESTAMP PRIMARY KEY DISTKEY SORTKEY,
    hour INTEGER NOT NULL,
    day INTEGER NOT NULL,
    week INTEGER NOT NULL,
    month INTEGER NOT NULL,
    year INTEGER ENCODE BYTEDICT ,
    weekday VARCHAR(9) ENCODE BYTEDICT
)
""")

# STAGING TABLES

#move data from s3 to redshift staging tables
staging_events_copy = ("""COPY staging_events
    FROM {} 
    iam_role {} 
    region {} 
    FORMAT AS JSON {};
""").format(LOG_DATA, ARN, REGION, LOG_JSONPATH)

#move data from s3 to redshift staging tables
staging_songs_copy = ("""COPY staging_songs
    FROM {} 
    iam_role {} 
    region {} 
    FORMAT AS JSON 'auto';
""").format(SONG_DATA, ARN, REGION)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
                            SELECT DISTINCT TIMESTAMP 'epoch' + (se.ts/1000) * INTERVAL '1 second', se.userId, se.level, ss.song_id, ss.artist_id, se.sessionId, se.location, se.userAgent
                            FROM staging_events se 
                            INNER JOIN staging_songs ss 
                                ON se.song = ss.title AND se.artist = ss.artist_name AND se.length = ss.duration
                            WHERE se.page = 'NextSong';
""")

user_table_insert = ("""INSERT INTO users(user_id, first_name, last_name, gender, level)
    SELECT se.userId, se.firstName, se.lastName, se.gender, se.level
    FROM staging_events se
    WHERE
        se.page = 'NextSong' AND
        se.userId NOT IN (SELECT DISTINCT user_id FROM users);
""")


song_table_insert = ("""INSERT INTO songs(song_id, title, artist_id, year, duration) 
    SELECT DISTINCT ss.song_id, ss.title, ss.artist_id, ss.year, ss.duration
    FROM staging_songs ss;
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT ss.artist_id, ss.artist_name, ss.artist_location, ss.artist_latitude, ss.artist_longitude
    FROM staging_songs ss
    WHERE
        ss.artist_id NOT IN (SELECT DISTINCT artist_id FROM artists);
""")

time_table_insert = ("""INSERT INTO time
SELECT DISTINCT
       start_time,
       EXTRACT(HOUR FROM start_time) AS hour,
       EXTRACT(DAY FROM start_time) AS day,
       EXTRACT(WEEKS FROM start_time) AS week,
       EXTRACT(MONTH FROM start_time) AS month,
       EXTRACT(YEAR FROM start_time) AS year,
       to_char(start_time, 'Day') AS weekday       
FROM songplays;
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, time_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, artist_table_drop, time_table_drop,  song_table_drop, user_table_drop ,  ]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, songplay_table_insert, time_table_insert]
