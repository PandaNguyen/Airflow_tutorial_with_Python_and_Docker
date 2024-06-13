import sqlalchemy
import pandas as pd
import sqlite3
from Extract import return_dataframe
from Transform import Data_Quality, Transform_df

DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"

def create_tables(cursor):
    sql_query_1 = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """
    sql_query_2 = """
    CREATE TABLE IF NOT EXISTS fav_artist(
        timestamp VARCHAR(200),
        ID VARCHAR(200),
        artist_name VARCHAR(200),
        count VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (ID)
    )
    """
    cursor.execute(sql_query_1)
    cursor.execute(sql_query_2)

def load_data(engine, load_df, Transformed_df):
    try:
        load_df.to_sql("my_played_tracks", engine, index=False, if_exists='append')
    except Exception as e:
        print(f"Error loading my_played_tracks: {e}")

    try:
        Transformed_df.to_sql("fav_artist", engine, index=False, if_exists='append')
    except Exception as e:
        print(f"Error loading fav_artist: {e}")

def main():
    # Extract
    load_df = return_dataframe()
    if not Data_Quality(load_df):
        raise ValueError("Failed at Data Validation")

    # Transform
    Transformed_df = Transform_df(load_df)

    # Load
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    with sqlite3.connect('my_played_tracks.sqlite') as conn:
        cursor = conn.cursor()
        create_tables(cursor)
        print("Opened database successfully")

        load_data(engine, load_df, Transformed_df)

        print("Close database successfully")

if __name__ == "__main__":
    main()
