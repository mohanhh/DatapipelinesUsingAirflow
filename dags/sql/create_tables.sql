Drop table if exists public.staging_events;

Drop table if exists public.staging_songs;

Drop table if exists public.songplays;

drop table if exists public.users cascade;

drop table if exists public.songs cascade;

drop table if exists public.artists cascade;

drop table if exists public.time;


CREATE TABLE if not exists public.artists (
	artistid varchar(256) NOT NULL,
	name varchar(256),
	location varchar(256),
	lattitude numeric(18,0),
	longitude numeric(18,0)
);

CREATE TABLE if not exists public.songplays (
	playid varchar(32) NOT NULL,
	start_time timestamp NOT NULL,
	userid int4 NOT NULL,
	"level" varchar(256),
	songid varchar(256),
	artistid varchar(256),
	sessionid int4,
	location varchar(256),
	user_agent varchar(256),
	CONSTRAINT songplays_pkey PRIMARY KEY (playid)
);

CREATE TABLE if not exists public.songs (
	songid varchar(256) NOT NULL,
	title varchar(256),
	artistid varchar(256),
	"year" int4,
	duration numeric(18,0),
	CONSTRAINT songs_pkey PRIMARY KEY (songid)
);


create table if not exists public.staging_events
    (artist varchar(256), 
    auth varchar(256), 
    firstName varchar(256), 
    gender varchar(10), 
    itemInSession int, 
    lastName varchar(256), 
    length numeric(30, 3), 
    level varchar(64), 
    location varchar(100), 
    method varchar(10), 
    page varchar(32), 
    registration decimal(30, 3), 
    sessionId bigint, 
    song varchar(256), 
    status int, 
    ts bigint sortkey, 
    userAgent varchar(256), 
    userId bigint) diststyle auto;


CREATE TABLE if not exists public.staging_songs (
	num_songs int4,
	artist_id varchar(256),
	artist_name varchar(256),
	artist_latitude numeric(18,0),
	artist_longitude numeric(18,0),
	artist_location varchar(256),
	song_id varchar(256),
	title varchar(256),
	duration numeric(18,0),
	"year" int4
);
CREATE TABLE if not exists public."time" (
	start_time timestamp NOT NULL,
	"hour" int4,
	"day" int4,
	week int4,
	"month" varchar(256),
	"year" int4,
	weekday varchar(256),
	CONSTRAINT time_pkey PRIMARY KEY (start_time)
);

CREATE TABLE if not exists public.users (
	userid int4 NOT NULL,
	first_name varchar(256),
	last_name varchar(256),
	gender varchar(256),
	"level" varchar(256),
	CONSTRAINT users_pkey PRIMARY KEY (userid)
);





