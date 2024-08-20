staging_events_table_DROP = """
DROP TABLE IF EXISTS STAGING_EVENTS;
"""

staging_events_table_create= """
CREATE TABLE IF NOT EXISTS STAGING_EVENTS(
artist VARCHAR,
auth VARCHAR,
firstName VARCHAR,
gender CHAR(1),
itemInSession INTEGER,
lastName VARCHAR,
length NUMERIC,
level VARCHAR,
location VARCHAR,
method VARCHAR ,
page VARCHAR,
registration NUMERIC,
sessionId INTEGER,
song VARCHAR,
status INTEGER,
ts BIGINT,
userAgent VARCHAR,
userId INTEGER
);
"""
staging_songs_table_DROP = """
DROP TABLE IF EXISTS STAGING_songs;
"""

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging_songs (
num_songs INTEGER,
artist_id VARCHAR,
artist_latitude FLOAT,
artist_longitude FLOAT,
artist_location VARCHAR,
artist_name VARCHAR,
song_id VARCHAR,
title VARCHAR,
duration FLOAT,
year INT

);
"""

songplay_table_DROP = """
DROP TABLE IF EXISTS songplays;
"""

songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplays (
songplay_id VARCHAR,
start_time TIMESTAMP,
user_id INTEGER,
level VARCHAR,
song_id VARCHAR,
artist_id VARCHAR,
session_id INTEGER,
location VARCHAR,
user_agent VARCHAR
);
"""

songplay_table_insert = """
INSERT INTO songplays (
            songplay_id,
            start_time,
            user_id,
            level,
            song_id,
            artist_id,
            session_id,
            location,
            user_agent
        )
        Select  md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration;
    """

user_table_DROP = """
DROP TABLE IF EXISTS USERS;
"""
user_table_create = """
CREATE TABLE IF NOT EXISTS USERS(
user_id INTEGER PRIMARY KEY, 
first_name VARCHAR, 
last_name VARCHAR, 
gender CHAR(1), 
level VARCHAR
);
"""

user_table_insert = """
        INSERT INTO USERS
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong';
    """

song_table_DROP = """
DROP TABLE IF EXISTS SONGS;
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS SONGS(
song_id VARCHAR PRIMARY KEY,
title VARCHAR,
artist_id VARCHAR,
year INT,
duration FLOAT
);
"""

song_table_insert = """
        INSERT INTO SONGS
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
        WHERE SONG_ID IS NOT NULL;
    """

artist_table_drop = """
DROP TABLE IF EXISTS ARTISTS;
"""
artist_table_create = """
CREATE TABLE IF NOT EXISTS ARTISTS(
artist_id VARCHAR PRIMARY KEY ,
name VARCHAR,
location VARCHAR,
latitude FLOAT,
longitude FLOAT
);
"""

artist_table_insert = """
        INSERT INTO ARTISTS
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
        WHERE artist_id IS  NOT NULL;
    """

time_table_DROP = """
DROP TABLE IF EXISTS TIME;
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS TIME(
start_time TIMESTAMP PRIMARY KEY ,
hour INTEGER,
day INTEGER,
week INTEGER,
month INTEGER,
year INTEGER,
weekday VARCHAR
);
"""

time_table_insert = """
        INSERT INTO TIME
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays;
    """

