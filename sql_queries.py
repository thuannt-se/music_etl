# DROP TABLES

songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

songplay_table_create = ("""create table if not exists songplays(start_time numeric PRIMARY KEY, 
                        user_id varchar, level varchar, song_id varchar, artist_id varchar, session_id int, location varchar, user_agent varchar)""")

user_table_create = ("""create table if not exists users(user_id varchar PRIMARY KEY, first_name varchar, last_name varchar, gender varchar, level varchar)""")

song_table_create = ("""create table if not exists songs(song_id varchar PRIMARY KEY, title varchar, artist_id varchar, year int, duration numeric)""")

artist_table_create = ("""create table if not exists artists(artist_id varchar PRIMARY KEY, name varchar, location varchar, latitude numeric, longitude numeric)""")

time_table_create = ("""create table if not exists times(start_time numeric primary key, hour int, day int, week int, month int, year int, weekday varchar)""")

temp_table_json_holder = ("""create temporary table temp_raw_data(values text) on commit drop;
                            copy temp_raw_data from %s;""")
# INSERT RECORDS

songplay_table_insert = ("""insert into songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
                            values(%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""")

song_table_insert = ("""insert into songs(song_id, title, artist_id, year, duration) values (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""")

artist_table_insert = ("""insert into artists(artist_id, name, location, latitude, longitude) values (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""")

user_table_insert = ("""insert into users(user_id, first_name, last_name, gender, level) values %s ON CONFLICT DO NOTHING""")

time_table_insert = ("""insert into times(start_time, hour, day, week, month, year, weekday) values %s ON CONFLICT DO NOTHING""")
# INSERT RECORDS USING COPY

# Insert user table
user_table_insert_with_copy = ("""insert into users(user_id, first_name, last_name, gender, level)
                            select  e->> 'userId' as user_id,
                                    e->> 'firstName' as first_name,
                                    e->> 'lastName' as last_name,
                                    e->> 'gender' as gender,
                                    e->> 'level' as level
                            from (
                                select values::json as e from temp_raw_data where is_valid_json(values)
                            ) s ON CONFLICT DO NOTHING
""")
time_table_insert_with_copy = ("""insert into times(start_time, hour, day, week, month, year, weekday)
                                        select  (e->> 'ts')::numeric as start_time,
                                            EXTRACT(HOUR FROM to_timestamp((e->> 'ts')::numeric/1000)) as hour,
                                            EXTRACT(DAY FROM  to_timestamp((e->> 'ts')::numeric/1000)) as day,
                                            EXTRACT(WEEK FROM  to_timestamp((e->> 'ts')::numeric/1000)) as week,
                                            EXTRACT(MONTH FROM  to_timestamp((e->> 'ts')::numeric/1000)) as month,
                                            EXTRACT(YEAR FROM  to_timestamp((e->> 'ts')::numeric/1000)) as year,
                                            EXTRACT(ISODOW  FROM  to_timestamp((e->> 'ts')::numeric/1000)) as weekday
                                    from (
                                        select values::json as e from temp_raw_data where is_valid_json(values)
                                    ) s ON CONFLICT DO NOTHING                            
                                """)

# FIND SONGS

song_select = ("""SELECT s.song_id, ar.artist_id FROM artists ar join songs s on ar.artist_id = s.artist_id
	                where s.title = %s and ar.name = %s and s.duration = %s""")
# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

# CREATE FUNCTION TO CHECK VALID JSON
function_is_valid_json = ("""create or replace function is_valid_json(p_json text)
                                  returns boolean
                                as
                                $$
                                begin
                                  return (p_json::json is not null);
                                exception 
                                  when others then
                                     return false;  
                                end;
                                $$
                                language plpgsql
                                immutable;""")