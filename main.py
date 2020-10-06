import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import json
from datetime import datetime
import datetime
import sqlite3
import requests

DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"
USER_ID = "Mr.R"
TOKEN = "BQBNhkMYSrIVKcY-gdW5rggF9jB2GFSlYjU19aSJ3HvRcHYns2Ano_bovcjbzO3Fn3dE71JH2k3C2-FtBTn3q9OrN0uBC6N_Zlp_BS6Brvhx1owxwBwnptCHNOV3KhgXpUoshuRGgKVIEURbpXnoj43QkF9Jj-1jAqgb5pyvAiy84DVkajI"

#generate your tokens here: https://developer.spotify.com/console/get-current-user-playlists/
#Note: You need a Spotify account 


def check_if_is_valid_data(df: pd.DataFrame) -> bool:
    #check if is empty
    if df.empty:
        print("No Songs Downloaded. Finishing execution")
        return False
    
    # Primary Key check 
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary key is vaiolated")
    
    #check for nulls
    if df.isnull().values.any():
        raise Exception("NUll valued found")

    #check that all timestamps are of yesterday's date
    yesterday = datetime.datetime.now() - datetime.timedelta(days = 1)
    yesterday = yesterday.replace(hour= 0, minute = 0, second= 0, microsecond= 0)

    timestamps = df["timestamp"].tolist()

    '''for timestamps in timestamps:
        if datetime.datetime.strptime(timestamps, "%Y-%m-%d") != yesterday:
            raise Exception("at least one of the returned songs does not come from within the last 24 hours")
        return True'''



if __name__ == "__main__":

    #extract part from ETL Process
    headers = {
        "acept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token} ".format(token=TOKEN)

    }

    today =  datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time = yesterday_unix_timestamp), headers = headers)

    data = r.json()

    
    song_names = []
    artist_names = []
    playeded_at_list = []
    timestamps = []

    for song in data ["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        playeded_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])


    song_dic = {
        "song_name" : song_names,
        "artist_name" : artist_names,
        "played_at" : playeded_at_list,
        "timestamp" : timestamps
    }

    song_df = pd.DataFrame(song_dic, columns= ["song_name", "artist_name", "played_at", "timestamp"])

    #validate
    if check_if_is_valid_data(song_df):
        print("Data valid, proceed to Load stage")

    # load
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('my_played_tracks.sqlite')
    cursor = conn.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at  VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """

    cursor.execute(sql_query)
    print("Opened the database sucessfully")

    try:
        song_df.to_sql("my_played_tracks", engine, index=False, if_exists='append')
    except:
        print("Data already exists in database")

    conn.close()
    print("Close database sucessfully")
    
    
    # Job scheaduling to implement

    #...