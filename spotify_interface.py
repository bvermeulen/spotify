import requests
from functools import wraps
from dataclasses import dataclass, asdict
import json
import pandas as pd
from decouple import config
import psycopg2
from sqlalchemy import create_engine, text as sqltxt


@dataclass
class TrackRecord:
    played_at: str
    name: str
    id: int
    artist: str
    play_time: str
    acousticness: float
    danceability: float
    energy: float
    instrumentalness: float
    key: int
    liveness: float
    loudness: float
    mode: int
    speechiness: float
    tempo: float
    time_signature: int
    valence: float

    def as_dict(self):
        return asdict(self)


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
convert_keys = ["acousticness", "energy", "instrumentalness", "loudness"]

pd.set_option("mode.chained_assignment", None)

api_key = config("SOUNDSTAT_KEY")
api_html = "https://soundstat.info/api/v1/track/"
headers = {"accept": "application/json", "x-api-key": api_key}


def get_track_info(track: str) -> dict | None:
    try:
        response = requests.get(f"{api_html}{track}", headers=headers)
        return response.json()

    except Exception:
        return None


def convert_to_spotify(features):
    if not features:
        return

    """Based on email from SoundStat dd. 6 February"""
    features[convert_keys[0]] *= 0.005
    features[convert_keys[1]] *= 2.25
    features[convert_keys[2]] *= 0.03
    features[convert_keys[3]] = -(1 - features[convert_keys[3]]) * 14
    return features


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
            f"id SERIAL PRIMARY KEY, "
            f"track_id VARCHAR(25) UNIQUE, "
            f"title VARCHAR(250), "
            f"artist VARCHAR(250), "
        )
        sql_str += ", ".join([key + " " + val for key, val in keys.items()]) + ", "
        sql_str += f"spotify_flag BOOL);"
        cursor.execute(sql_str)

    def append_df(self, df: pd.DataFrame) -> None:
        sql_str = f"select * from {self.track_info};"
        with self.engine.connect() as conn:
            db_df = pd.read_sql(sql_str, con=conn)

        try:
            db_df.drop("id", axis=1, inplace=True)

        except KeyError:
            pass

        with self.engine.connect() as con:
            con.execute(sqltxt(f"truncate table {self.track_info} restart identity"))
            con.commit()

        if not db_df.empty:
            db_df = pd.concat([db_df, df], ignore_index=True)

        else:
            db_df = df

        db_df.drop_duplicates(subset=["track_id"], keep="first", inplace=True)
        with self.engine.being() as conn:
            db_df.to_sql(self.track_info, con=conn, if_exists="append", index=False)

    def extract_track_info(self, track_id: str) -> dict:
        track_dict_list = None
        sql_str = f"select * from {self.track_info} where track_id = '{track_id}';"
        with self.engine.connect() as conn:
            track_dict_list = pd.read_sql(sql_str, con=conn).to_dict(orient="records")
        if not track_dict_list:
            return {}

        else:
            return track_dict_list[0]

    def extract_all_trackinfo(self) -> pd.DataFrame:
        sql_str = f"select * from {self.track_info};"
        with self.engine.connect() as conn:
            return pd.read_sql(sql_str, con=conn)

    def add_track(self, track_record: TrackRecord):
        df = pd.DataFrame()
        sql_str = (
            f"select * from {self.track_info} where track_id = '{track_record.id}';"
        )
        with self.engine.connect() as conn:
            df = pd.read_sql(sql_str, con=conn)

        if not track_record.acousticness or not df.empty:
            return

        db_df = pd.DataFrame(
            columns=["track_id", "title", "artist", *keys.keys(), "spotify_flag"]
        )
        db_df.loc[0, "track_id"] = track_record.id
        db_df.loc[0, "title"] = track_record.name
        db_df.loc[0, "artist"] = track_record.artist
        for key in keys.keys():
            db_df.loc[0, [key]] = getattr(track_record, key)

        if track_record.time_signature:
            db_df.loc[0, "spotify_flag"] = True

        else:
            db_df.loc[0, "spotify_flag"] = False

        with self.engine.connect() as conn:
            db_df.to_sql(self.track_info, con=conn, if_exists="append", index=False)
