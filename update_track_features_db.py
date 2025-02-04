"""
spotify log and database tools
"""

from pathlib import Path
import requests
from functools import wraps
import json
import pandas as pd
import numpy as np
from decouple import config
import psycopg2
from sqlalchemy import create_engine, text as sqltxt


tracklog_jsonfile = Path("./data/track_log.json")

keys = {
    "acousticness": "REAL",
    "danceability": "REAL",
    "energy": "REAL",
    "instrumentalness": "REAL",
    "key": "INTEGER",
    "liveness": "REAL",
    "loudness": "REAL",
    "mode": "INTEGER",
    "speechiness": "REAL",
    "tempo": "REAL",
    "time_signature": "INTEGER",
    "valence": "REAL",
}

pd.set_option("mode.chained_assignment", None)

api_key = config("SOUNDSTAT_KEY")
api_html = "https://soundstat.info/api/v1/track/"
headers = {"accept": "application/json", "x-api-key": api_key}


def get_track_info(track: str) -> json:
    response = requests.get(f"{api_html}{track}", headers=headers)
    return response.json()


class DbConnect:

    def __init__(self, host, port, db_user, db_user_pw, database):
        self.host = host
        self.port = port
        self.db_user = db_user
        self.db_user_pw = db_user_pw
        self.database = database

    def connect(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            connect_string = (
                f"host='{self.host}' dbname='{self.database}'"
                f"user='{self.db_user}' password='{self.db_user_pw}'"
            )
            result = None
            try:
                connection = psycopg2.connect(connect_string)
                cursor = connection.cursor()
                result = func(*args, cursor, **kwargs)
                connection.commit()

            except psycopg2.Error as error:
                print(f"error while connect to PostgreSQL {self.database}: " f"{error}")

            finally:
                if connection:
                    cursor.close()
                    connection.close()

            return result

        return wrapper

    def get_engine(self):
        return create_engine(
            f"postgresql://{self.db_user}:{self.db_user_pw}"
            f"@{self.host}:{self.port}/{self.database}"
        )


class SpotifyDb:

    db_connect = DbConnect(
        config("DB_HOST"),
        config("PORT"),
        config("DB_USERNAME"),
        config("DB_PASSWORD"),
        config("DATABASE"),
    )

    def __init__(self):
        self.track_info = "track_info"
        self.engine = self.db_connect.get_engine()

    @db_connect.connect
    def create_track_features_table(self, cursor):
        sql_str = (
            f"CREATE TABLE {self.track_info} ("
            f"track_id VARCHAR(25) PRIMARY KEY, "
            f"title VARCHAR(250), "
            f"artist VARCHAR(250), "
        )
        sql_str += ", ".join([key + " " + val for key, val in keys.items()]) + ");"
        cursor.execute(sql_str)

    def append_df(self, df: pd.DataFrame) -> None:
        sql_str = f"select * from {self.track_info};"
        db_df = pd.read_sql(sql_str, self.engine)
        with self.engine.connect() as con:
            con.execute(sqltxt(f"delete from {self.track_info}"))
            con.commit()

        if not db_df.empty:
            db_df = pd.concat([db_df, df], ignore_index=True)

        else:
            db_df = df

        db_df.drop_duplicates(subset=["track_id"], keep="first", inplace=True)
        db_df.to_sql(self.track_info, self.engine, if_exists="append", index=False)

    def extract_track_info(self, track_id: str) -> pd.DataFrame:
        sql_str = f"select * from {self.track_info} where track_id = '{track_id}';"
        return pd.read_sql(sql_str, self.engine)


class SpotifyLog:

    def __init__(self, filename):
        self.spotify_db = SpotifyDb()
        with open(filename, "rt") as json_file:
            log_dict = json.load(json_file)
        self.tracklog_df = pd.DataFrame(log_dict["tracks"])
        self.account = log_dict["spotify_account"]

    def store_to_db(self, tracklog_df):
        db_df = pd.DataFrame(columns=["track_id", "title", "artist", *keys])
        existing_features = pd.notnull(tracklog_df["acousticness"])
        db_df.track_id = tracklog_df[existing_features].id
        db_df.title = tracklog_df[existing_features].name
        db_df.artist = tracklog_df[existing_features].artist
        for key in keys:
            db_df[key] = self.tracklog_df[existing_features][key]

        self.spotify_db.append_df(db_df)

    def create_initial_db(self):
        self.store_to_db(self.tracklog_df)

    def check_missing(self) -> pd.DataFrame:
        print(f"{self.tracklog_df.shape[0]=:,}")
        missing_features = pd.isnull(self.tracklog_df["acousticness"])
        missing_df = self.tracklog_df[missing_features]
        tracks_missing_features_before = missing_df.shape[0]
        print(f"{tracks_missing_features_before=:,}")

        # try to get track features from the track played earlier
        count = 0
        for i, row in enumerate(missing_df.iterrows()):
            index = row[0]
            track_id = row[1]["id"]
            result_df = self.spotify_db.extract_track_info(track_id)
            if not result_df.empty:
                count += 1
                features = {key: result_df.iloc[-1][key] for key in keys}
                self.tracklog_df.loc[index, features.keys()] = features.values()

        self.tracklog_df["played_at"] = pd.to_datetime(
            self.tracklog_df["played_at"], format="%Y-%B-%d %H:%M:%S"
        )
        print(f"processed: {i}, resolved: {count}")
        missing_features = pd.isnull(self.tracklog_df["acousticness"])
        missing_df = self.tracklog_df[missing_features]
        tracks_missing_features_after = missing_df.shape[0]
        print(f"{tracks_missing_features_after=:,}")
        resolved = tracks_missing_features_before - tracks_missing_features_after
        print(f"{resolved=:,} ({100 * resolved / tracks_missing_features_before:.1f}%)")
        self.tracklog_df.to_csv(
            tracklog_jsonfile.parent / Path(tracklog_jsonfile.stem + "_01.csv")
        )

        return missing_df

    def soundstat_db_update(self, missing_df: pd.DataFrame):
        tracks_missing_features_before = missing_df.shape[0]
        print(f"{tracks_missing_features_before=:,}")
        count = 0
        for i, row in enumerate(missing_df.iterrows()):
            index = row[0]
            track_id = row[1]["id"]
            track_info = get_track_info(track_id)
            if track_features := track_info.get("features", None):
                count += 1
                print(f"{i=:04}, {count=:04}", end="\r")
                features = {key: track_features.get(key, None) for key in keys}
                self.tracklog_df.loc[index, features.keys()] = features.values()

        self.store_to_db(self.tracklog_df)
        print(f"\nprocessed: {i}, resolved: {count}")
        missing_features = pd.isnull(self.tracklog_df["acousticness"])
        missing_df = self.tracklog_df[missing_features]
        tracks_missing_features_after = missing_df.shape[0]
        print(f"{tracks_missing_features_after=:,}")
        resolved = tracks_missing_features_before - tracks_missing_features_after
        print(f"{resolved=:,} ({100 * resolved / tracks_missing_features_before:.1f}%)")
        self.tracklog_df.to_csv(
            tracklog_jsonfile.parent / Path(tracklog_jsonfile.stem + "_02.csv")
        )

    def tracklog_to_json(self):
        track_log = {}
        self.tracklog_df.played_at = self.tracklog_df.played_at.dt.strftime(
            "%Y-%B-%d %H:%M:%S"
        )
        self.tracklog_df = self.tracklog_df.replace(np.nan, None)
        tracks = self.tracklog_df.to_dict(orient="records")
        track_log["spotify_accounts"] = self.account
        track_log["tracks"] = tracks
        with open(
            tracklog_jsonfile.parent / Path(tracklog_jsonfile.stem + "_01.json"), "wt"
        ) as jf:
            json.dump(track_log, jf)


if __name__ == "__main__":
    sdb = SpotifyDb()
    sdb.create_track_features_table()
    sl = SpotifyLog(tracklog_jsonfile)
    # sl.create_initial_db()
    missing_df = sl.check_missing()
    sl.soundstat_db_update(missing_df)
    sl.tracklog_to_json()
