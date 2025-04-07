"""
spotify log and database tools
"""

from pathlib import Path
import json
import pandas as pd
import numpy as np
import spotify_interface as iface


tracklog_jsonfile = Path("./data/track_log.json")


class SpotifyLog:

    def __init__(self, filename):
        self.spotify_db = iface.SpotifyDb()
        with open(filename, "rt") as json_file:
            log_dict = json.load(json_file)
        self.tracklog_df = pd.DataFrame(log_dict["tracks"])
        self.account = log_dict["spotify_account"]

    def store_to_db(self, tracklog_df):
        db_df = pd.DataFrame(
            columns=["track_id", "title", "artist", *iface.keys.keys(), "spotify_flag"]
        )
        existing_features = pd.notnull(tracklog_df["acousticness"])
        tracklog_existing_df = tracklog_df[existing_features]
        db_df.track_id = tracklog_existing_df.id
        db_df.title = tracklog_existing_df.name
        db_df.artist = tracklog_existing_df.artist
        for key in iface.keys.keys():
            db_df[key] = self.tracklog_df[key]

        spotify_flag = pd.notnull(db_df["time_signature"])
        db_df.loc[spotify_flag, "spotify_flag"] = True

        # loudness is positive the soundstat values have not been converted to spotify values
        soundstat_df = db_df.loc[db_df["loudness"] >= 0]
        for row in soundstat_df.iterrows():
            index = row[0]
            c_features = iface.convert_to_spotify(
                {k: row[1][k] for k in iface.convert_keys}
            )
            for k, v in c_features.items():
                db_df.loc[index, k] = v

        # set the spotify flag false in case there is no time_signature
        spotify_flag = pd.isnull(db_df["time_signature"])
        db_df.loc[spotify_flag, "spotify_flag"] = False

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
            track_info = self.spotify_db.extract_track_info(track_id)
            if track_info:
                count += 1
                features = {key: track_info[key] for key in iface.keys}
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
            track_info = iface.get_track_info(track_id)
            if track_features := iface.convert_to_spotify(track_info.get("features")):
                count += 1
                print(f"{i=:04}, {count=:04}", end="\r")
                features = {key: track_features.get(key, None) for key in iface.keys}
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
        track_log["spotify_account"] = self.account
        track_log["tracks"] = tracks
        with open(
            tracklog_jsonfile.parent / Path(tracklog_jsonfile.stem + "_01.json"), "wt"
        ) as jf:
            json.dump(track_log, jf)


if __name__ == "__main__":
    db = iface.SpotifyDb()
    db.create_track_features_table()
    sl = SpotifyLog(tracklog_jsonfile)
    # sl.create_initial_db()
    missing_df = sl.check_missing()
    # sl.soundstat_db_update(missing_df)
    # sl.tracklog_to_json()
