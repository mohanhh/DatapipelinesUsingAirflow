'''
All insert sql statements needed to insert data in to fact and dimension tables
''' 
class SqlQueries:
    # Insert data to singplay table
    songplay_table_insert = ("""
    insert 
    into
        public.songplays
        (playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent) 
        (SELECT
                md5(events.sessionid || events.start_time) playid,
                events.start_time as start_time, 
                events.userid as userid, 
                events.level as level, 
                songs.song_id as songid, 
                songs.artist_id as artistid, 
                events.sessionid as sessionid, 
                events.location as location, 
                events.useragent as user_agent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM public.staging_events
            WHERE page='NextSong') events
            LEFT JOIN public.staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration)
    """)

    #Query for user_table
    user_table_insert = ("""insert into public.users (userid, 
    first_name, 
    last_name, 
    gender, 
    level) 
        (SELECT distinct userid, firstname as first_name, lastname last_name, gender as gender, level as level
        FROM public.staging_events
        WHERE page='NextSong')
    """)

    # insert in to song_table
    song_table_insert = ("""insert into public.songs (songid, title, artistid, duration, year)
        (SELECT distinct song_id as songid, title as title, artist_id as artistid, duration as duration, year as year
        FROM public.staging_songs)
    """)

    # insert in to artist_table
    artist_table_insert = ("""insert into public.artists (artistid, name, location, lattitude, longitude) 
        (SELECT distinct artist_id as artistid, artist_name as name, artist_location as location, artist_latitude as lattitude, artist_longitude as longitude
        FROM public.staging_songs)
    """)

    # insert data in to time_table
    time_table_insert = ("""insert into  public.time (start_time, hour, day, week, month, year, weekday) 
        (SELECT start_time, extract(hour from start_time) as hour, extract(day from start_time) as day, extract(week from start_time) as week_time, 
               extract(month from start_time) as month, extract(year from start_time) as year, extract(dayofweek from start_time) as dayofweek
        FROM songplays)
    """)
    # Select query to read everything from select song play staging table
    select_song_play_events = ("""select artist, auth, firstName, gender, itemInSession, lastName, length, level, location, method, page, registration , sessionId, song, status, ts, userAgent, userId from staging_events_table order by ts desc""")
    # FIND SONGS

    song_select = ("""select s1.song_id as songid, a1.artist_id as artistid from songs s1 join artists a1 on s1.artist_id=a1.artist_id where s1.title=%s and a1.name=%s and s1.duration=%s""")



    