import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Process songs files and insert records into db
    :param cur: cursor
    :param filepath: song_file path, source for db
    """
    # open song file
    df = pd.read_json(filepath, lines=True)
    
    # insert artist record
    for index, row in df.iterrows():
        artist_data = (row.artist_id, row.artist_name, row.artist_location, row.artist_latitude, row.artist_longitude)
        song_data = (row.song_id, row.title, row.artist_id, row.year, row.duration)
        cur.execute(artist_table_insert, artist_data)
        cur.execute(song_table_insert, song_data)
        


def process_log_file(cur, filepath):
    """
    Process event log file and insert records into db
    :param cur: cursor
    :param filepath: log_file path, source for db
    """
    # open log file
    df = pd.read_json(filepath, lines = True)

    # filter by NextSong action
    df = df[df.page == "NextSong"] 

    # convert timestamp column to datetime
    t = df.ts.apply(lambda ts: pd.to_datetime(ts*10e5))
    
    # insert time data records
    time_data = (t.dt.values.values, t.dt.hour.values, t.dt.day.values, t.dt.week.values, t.dt.month.values, t.dt.year.values, t.dt.weekday.values)
    column_labels = ('timestamp', 'hour', 'day', 'week', 'year', 'month', 'weekday')
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)


    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        #songplay_data = 
        ts = pd.to_datetime(row.ts*10e5)
        songplay_data = (index, ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Process all songs and events log files
    :param cur: database cursor
    :param conn: database connection
    :param filepath: directory where thie files which should be processed
    :param func: function which handle separate file
    """
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
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()