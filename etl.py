import os
import glob
import psycopg2
import psycopg2.extras as extras
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath, *args):
    # open song file
    df = pd.read_json(filepath, typ='series')
    # insert song record
    song_data = (df["song_id"], df["title"], df["artist_id"], df["year"], df["duration"])
    artist_data = (df["artist_id"], df["artist_name"], df["artist_location"], df["artist_latitude"], df["artist_longitude"])
    cur.execute(song_table_insert, song_data)
    # insert artist record
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath, *args):
    '''
        Process create insert record into users, times, songplays tables from json file

                Parameters:
                        cur (cursor): postgre cursor
                        filepath (string): file path to json file
                        *args(int): selection mode for insert operation:
                                    - 1: Insert operation using postgre copy command
                                    - default: batch insert all record found in file

    '''
    df = pd.read_json(filepath, lines=True)
     # filter by NextSong action
    df_next_song = df[df.page.eq('NextSong')]
    # convert timestamp column to datetime
    t = pd.to_datetime(df_next_song['ts'], unit='ms')
    #  # insert time data records
    if(args[0] == 1) :
        # Accept to lose some records that have invalid json
        cur.execute(function_is_valid_json)
        cur.execute(temp_table_json_holder, (filepath,))
        cur.execute(user_table_insert_with_copy)
        cur.execute(time_table_insert_with_copy)
    else:
        # use ts as primary key (Not actually unique but we accept it for this project)
        time_data = {'start_time': df_next_song['ts'], 'hour': t.dt.hour,
                     'day': t.dt.day, 'week': t.dt.week,
                     'month': t.dt.month, 'year': t.dt.year, 'weekday': t.dt.day_name()}
        time_df = pd.DataFrame(time_data)
        tuples_time = [tuple(x) for x in time_df.to_numpy()]
        user_df = pd.DataFrame({'user_id': df_next_song['userId'], 'first_name': df_next_song['firstName'],
                                'last_name': df_next_song['lastName'], 'gender': df_next_song['gender'],
                                'level': df_next_song['level']})
        tuples_user = [tuple(x) for x in user_df.to_numpy()]
        extras.execute_values(cur, time_table_insert, tuples_time)
        extras.execute_values(cur, user_table_insert, tuples_user)

    for index, row in df.iterrows():
         # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        if results:
            print('result', results)
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row['ts'], row['userId'],
                         row['level'], songid, artistid, row['sessionId'],
                         row['location'], row['userAgent'])
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process

    for i, datafile in enumerate(all_files, 1):
        try:
            func(cur, datafile, 2)
            conn.commit()
            print('{}/{} files processed.'.format(i, num_files))
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error occurs: %s" % error)
            conn.rollback()

def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=admin")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()