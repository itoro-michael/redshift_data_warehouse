import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS tbl_staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS tbl_staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS tbl_songplay;"
user_table_drop = "DROP TABLE IF EXISTS tbl_user;"
song_table_drop = "DROP TABLE IF EXISTS tbl_song;"
artist_table_drop = "DROP TABLE IF EXISTS tbl_artist;"
time_table_drop = "DROP TABLE IF EXISTS tbl_time;"

# CREATE TABLES
staging_events_table_create=    ("""
                                    CREATE TABLE IF NOT EXISTS tbl_staging_events
                                    (
                                        staging_events_id bigint IDENTITY(0,1) NOT NULL,
                                        artist varchar,
                                        auth varchar,
                                        first_name varchar,
                                        gender varchar,
                                        item_in_session varchar,
                                        last_name varchar,
                                        length varchar,
                                        level varchar,
                                        location varchar,
                                        method varchar,
                                        page varchar,
                                        registration numeric,
                                        session_id int,
                                        song varchar,
                                        status int,
                                        ts bigint,
                                        user_agent varchar,
                                        user_id int
                                    );
                             """)

staging_songs_table_create =    ("""
                                    CREATE TABLE IF NOT EXISTS tbl_staging_songs
                                    (
                                        song_id varchar NOT NULL,
                                        artist_id varchar,
                                        artist_latitude numeric,
                                        artist_location varchar,
                                        artist_longitude numeric,
                                        artist_name varchar,
                                        duration numeric,
                                        num_songs int,
                                        title varchar,
                                        year int
                                    );  
                             """)

songplay_table_create = ("""
                            CREATE TABLE IF NOT EXISTS tbl_songplay 
                            (
                                songplay_id bigint IDENTITY(0,1) NOT NULL,
                                start_time TIMESTAMP sortkey, 
                                user_id int NOT NULL, 
                                level varchar, 
                                song_id varchar distkey,
                                artist_id varchar, 
                                session_id int, 
                                location varchar, 
                                user_agent varchar
                            );                      
                     """)

user_table_create = ("""
                        CREATE TABLE IF NOT EXISTS tbl_user 
                        (
                            user_id varchar sortkey, 
                            first_name varchar, 
                            last_name varchar,
                            gender varchar, 
                            level varchar
                        ) diststyle all;
                 """)

song_table_create = ("""
                        CREATE TABLE IF NOT EXISTS tbl_song 
                        (
                            song_id varchar sortkey distkey, 
                            title varchar, 
                            artist_id varchar,
                            year int, 
                            duration numeric
                        );
                 """)

artist_table_create =   ("""
                            CREATE TABLE IF NOT EXISTS tbl_artists 
                            (
                                artist_id varchar sortkey, 
                                name varchar, 
                                location varchar,
                                latitude decimal, 
                                longitude numeric
                            ) diststyle all;
                     """)

time_table_create = ("""
                        CREATE TABLE IF NOT EXISTS tbl_time 
                        (
                            start_time TIMESTAMP sortkey, 
                            hour int, 
                            day int, 
                            week int,
                            month int, 
                            year int, 
                            weekday int
                        ) diststyle all;
                 """)

# STAGING TABLES
staging_events_copy =   ("""
                            copy tbl_staging_events
                            from {}
                            iam_role {} 
                            json {};
                     """).format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE','ARN'), 
                                 config.get('S3','LOG_JSONPATH'))

staging_songs_copy =    ("""
                            copy tbl_staging_songs
                            from {}
                            iam_role {} 
                            json 'auto';
                     """).format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN'))

# FINAL TABLES
    
songplay_table_insert = ("""
                            INSERT INTO tbl_songplay    (   start_time,
                                                            user_id,
                                                            level,
                                                            song_id,
                                                            artist_id,
                                                            session_id,
                                                            location,
                                                            user_agent
                                                        )
                            SELECT  DISTINCT TIMESTAMP 'epoch' + ste.ts/1000 * interval '1 second'  AS start_time,
                                    ste.user_id AS user_id,
                                    ste.level AS level,
                                    sts.song_id AS song_id,
                                    sts.artist_id AS artist_id,                                    
                                    ste.session_id AS session_id,
                                    ste.location AS location,
                                    ste.user_agent AS user_agent
                            FROM tbl_staging_events ste
                            JOIN tbl_staging_songs sts 
                            ON ste.song = sts.title
                            AND ste.length = sts.duration
                            AND ste.artist = sts.artist_name
                            AND ste.page = 'NextSong';
                     """)

user_table_insert = ("""
                        INSERT INTO tbl_user    (   
                                                    user_id, 
                                                    first_name, 
                                                    last_name,
                                                    gender, 
                                                    level
                                                )
                        SELECT  DISTINCT user_id,
                                first_name,
                                last_name,
                                gender,
                                level
                        FROM tbl_staging_events;
                 """)                

song_table_insert = ("""
                        INSERT INTO tbl_song    (   
                                                    song_id, 
                                                    title, 
                                                    artist_id,
                                                    year, 
                                                    duration
                                                )
                        SELECT  DISTINCT song_id,
                                title,
                                artist_id,
                                year,
                                duration
                        FROM tbl_staging_songs;
                 """)

artist_table_insert =   ("""
                            INSERT INTO tbl_artists (   
                                                        artist_id, 
                                                        name, 
                                                        location,
                                                        latitude, 
                                                        longitude
                                                    )
                            SELECT  DISTINCT artist_id,
                                    artist_name AS name,
                                    artist_location AS location,
                                    artist_latitude AS latitude,
                                    artist_longitude AS longitude
                            FROM tbl_staging_songs;
                     """)

time_table_insert = ("""
                            INSERT INTO tbl_time    (   
                                                        start_time,
                                                        hour,
                                                        day,
                                                        week,
                                                        month,
                                                        year,
                                                        weekday
                                                    )
                            SELECT  DISTINCT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time,
                                    EXTRACT(hour FROM TIMESTAMP 'epoch' + ts/1000 * interval '1 second') AS hour,
                                    EXTRACT(day FROM TIMESTAMP 'epoch' + ts/1000 * interval '1 second') AS day,
                                    EXTRACT(week FROM TIMESTAMP 'epoch' + ts/1000 * interval '1 second') AS week,
                                    EXTRACT(month FROM TIMESTAMP 'epoch' + ts/1000 * interval '1 second') AS month,
                                    EXTRACT(year FROM TIMESTAMP 'epoch' + ts/1000 * interval '1 second') AS year,
                                    EXTRACT(weekday FROM TIMESTAMP 'epoch' + ts/1000 * interval '1 second') AS weekday
                            FROM tbl_staging_events ste
                            WHERE ste.page = 'NextSong';
                 """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, 
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, 
                      user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
