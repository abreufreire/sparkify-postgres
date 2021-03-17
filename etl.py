#!/usr/bin/env python
# -*- coding: utf-8 -*-


import os
import glob
from io import StringIO
import pandas as pd
from sql_queries import *
from transaction_postgres import *
import json


def get_json_files(filepath):
    """
    gets all json files from directory
    :param filepath: path to song & log json files
    :return: json files
    """

    json_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            json_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(json_files)
    print('{} files found in {}'.format(num_files, filepath))

    return json_files


def get_json_data(filepath):
    """
    gets dataframe with json files
    :param filepath: path to song or log json files
    :return: dataframe with all json files
    """

    all_files = get_json_files(filepath)

    # iterate over files & process
    json_data = []
    for i, datafile in enumerate(all_files, 1):
        with open(datafile, mode='r+') as jf:
            lines = [ln for ln in jf.readlines() if ln.strip()]
            #lines = [ln.rstrip() for ln in jf]
            for ln in lines:
                ln_dict = validate_json(ln)
                if ln_dict:
                    json_data.append(ln_dict)

    return pd.DataFrame(json_data)


def validate_json(json_data):
    """
    validates data in json file
    :param json_data: data in json file
    :return:
    """

    try:
        return json.loads(json_data)
    except json.decoder.JSONDecodeError:
        print(f"json is invalid, passing: \"{json_data}\"")
        return None


##########

def filter_log_data(df):
    """
    filters by NextSong action
    :param df: dataframe
    :return: dataframe rows filtered by NextSong value
    """

    next_song_df = df.loc[df['page'] == 'NextSong']
    # print('rows with next song: ')
    return next_song_df


def convert_time_data(df, time_columns):
    """
    decomposes timestamp column (start_time) into hour, day, week_of_year, month, year, weekday new columns
    :param df: dataframe with a timestamp column
    :return: dataframe with decomposed time columns
    """

    # decompose time data (start_time/ts is bigint dtype)
    timestamp = df['ts']
    time_data = []
    for i, ts in timestamp.items():
        # convert timestamp (ts) column to datetime
        t = pd.to_datetime(ts, unit='ms')
        time_data.append([ts, t.hour, t.day, t.week, t.month, t.year, t.dayofweek])
    # print(time_data)

    time_df = pd.DataFrame(time_data, columns=time_columns, dtype='float64')

    return time_df


def clean_data(df, col_id=None):
    """
    cleans rows with null & duplicate values
    :param df: dataframe (original)
    :param col_id: column/key of table
    :return: dataframe with cleaned rows
    """

    if col_id:
        try:
            df = df.dropna(axis=0, subset=[col_id], inplace=False)  # job on rows: axis=0 or 'index' (default)
        except Exception as e:
            print(e)

        try:
            df = df.drop_duplicates(subset=[col_id], inplace=False)
        except Exception as e:
            print(e)

    # drops empty rows
    df = df.dropna(how='all')

    if col_id == 'userId':
        # make name consistency
        df = df.rename(columns={
            "userId": "user_id",
            "firstName": "first_name",
            "lastName": "last_name"}
        )

    return df


def get_clean_data(df, columns, col_id=None):
    """
    selects columns from dataframe with cleaned rows
    :param df:
    :param columns:
    :param col_id: column/key of table
    :return: dataframe with cleaned rows
    """

    df = df[columns]
    df = clean_data(df, col_id)

    return df


def extract_song_id_artist_id(next_song_df, conn_pg):
    """
    returns for each songplays record the song_id & artist_id values
    :param next_song_df: dataframe filtered by NextSong action
    :param conn_pg: connection to Postgres
    :return:
    """

    songs_artists_dict = {'song_id': [], 'artist_id': []}

    # load songplay records to database
    for index, row in next_song_df.iterrows():
        results = conn_pg.execute_select(song_select, (row.song, row.artist, row.length))

        if results:
            song_id, artist_id = results
        else:
            song_id, artist_id = None, None

        songs_artists_dict['song_id'].append(song_id)
        songs_artists_dict['artist_id'].append(artist_id)

        return songs_artists_dict


def get_songplays_data(next_song_df, conn_pg):
    """
    combines songs & artists dataframes to build songplays
    :param next_song_df:
    :param conn_pg:
    :return:
    """

    songs_artists_dict = extract_song_id_artist_id(next_song_df, conn_pg)
    songs_artists_df = pd.DataFrame(songs_artists_dict)

    songplays_data = pd.concat([next_song_df, songs_artists_df], axis=1)  # concat using columns

    return songplays_data


def write_df_to_file(df):
    """
    writes dataframe to string buffer object
    :param df: dataframe
    :return: buffer
    """
    buf = StringIO()
    df.to_csv(
        buf,
        sep='\t',
        na_rep='Unknown',  # missing value: Unknown
        index=None,
        header=None,
        float_format='%.f',
        encoding='utf-8'
    )

    buf.seek(0)  # put writer at beginning of file

    return buf


def load_data_to_db(conn_pg, execute_insert, df):
    """
    inserts dataframe rows into database
    :param conn_pg: connection to Postgres
    :param execute_insert: query to insert data
    :param df: dataframe
    :return:
    """
    for _index, row in df.iterrows():
        conn_pg.execute_insert(execute_insert, row)


def bulk_df_to_db(conn_pg, df, cols, table, table_cols):
    """
    gets dataframe into cleaned file & loads it as bulk into database
    :param conn_pg:connection to Postgres
    :param df: dataframe
    :param cols: dataframe columns
    :param table: table songplays
    :param table_cols: table columns (songplays)
    :return:
    """
    df = get_clean_data(df, cols)
    df_file = write_df_to_file(df)
    conn_pg.execute_copy(df_file, table, table_cols)


def main():

    with ConnectPostgres() as conn_pg:

        ## SONG DATA
        song_data = get_json_data(filepath='data/song_data')
        # print(song_data)

        song_columns = ['song_id', 'title', 'artist_id', 'year', 'duration']
        artist_columns = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']

        song_data_table = get_clean_data(song_data, song_columns, 'song_id')
        load_data_to_db(conn_pg, song_table_insert, song_data_table)

        artist_data_table = get_clean_data(song_data, artist_columns, 'artist_id')
        load_data_to_db(conn_pg, artist_table_insert, artist_data_table)


        ## LOG DATA
        log_data = get_json_data(filepath='data/log_data')

        time_columns = ['start_time', 'hour', 'day', 'week_of_year', 'month', 'year', 'weekday']
        user_columns = ['userId', 'firstName', 'lastName', 'gender', 'level']

        log_data_by_next_song = filter_log_data(log_data)

        time_data = convert_time_data(log_data_by_next_song, time_columns)

        time_data_table = get_clean_data(time_data, time_columns, 'start_time')
        load_data_to_db(conn_pg, time_table_insert, time_data_table)


        ## SONGPLAYS
        songplays_data = get_songplays_data(log_data_by_next_song, conn_pg)

        user_data_table = get_clean_data(songplays_data, user_columns, 'userId')
        load_data_to_db(conn_pg, user_table_insert, user_data_table)

        songplays_columns = ['ts', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent']

        songplays_table_columns = ['start_time', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 'location', 'user_agent']

        bulk_df_to_db(conn_pg, songplays_data, songplays_columns, 'songplays', songplays_table_columns)

        print("Data processing done.")


if __name__ == "__main__":
    main()
