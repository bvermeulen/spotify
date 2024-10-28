from dataclasses import dataclass, asdict
import datetime
import time
from pathlib import Path
import json
from decouple import config
import spotipy
from spotipy.oauth2 import SpotifyOAuth

"""
Audio features explanation
acousticness: A confidence measure from 0.0 to 1.0 of whether the track is acoustic. 1.0 represents
  high confidence the track is acoustic.
danceability: Danceability describes how suitable a track is for dancing based on a combination of
  musical elements including tempo, rhythm stability, beat strength, and overall regularity. A value
  of 0.0 is least danceable and 1.0 is most danceable.
energy: Energy is a measure from 0.0 to 1.0 and represents a perceptual measure of intensity and
  activity. Typically, energetic tracks feel fast, loud, and noisy. For example, death metal has high
  energy, while a Bach prelude scores low on the scale. Perceptual features contributing to this
  attribute include dynamic range, perceived loudness, timbre, onset rate, and general entropy.
instrumentalness: Predicts whether a track contains no vocals. "Ooh" and "aah" sounds are treated as
  instrumental in this context. Rap or spoken word tracks are clearly "vocal". The closer the
  instrumentalness value is to 1.0, the greater likelihood the track contains no vocal content.
  Values above 0.5 are intended to represent instrumental tracks, but confidence is higher as the
  value approaches 1.0.
key: The key the track is in. Integers map to pitches using standard Pitch Class notation.
  E.g. 0 = C, 1 = C♯/D♭, 2 = D, and so on. If no key was detected, the value is -1.
liveness: Detects the presence of an audience in the recording. Higher liveness values represent an
  increased probability that the track was performed live. A value above 0.8 provides strong likelihood
  that the track is live.
loudness: The overall loudness of a track in decibels (dB). Loudness values are averaged across the
  entire track and are useful for comparing relative loudness of tracks. Loudness is the quality of a
  sound that is the primary psychological correlate of physical strength (amplitude). Values typically
  range between -60 and 0 db.
mode: Mode indicates the modality (major or minor) of a track, the type of scale from which its melodic
  content is derived. Major is represented by 1 and minor is 0.
speechiness: Speechiness detects the presence of spoken words in a track. The more exclusively
  speech-like the recording (e.g. talk show, audio book, poetry), the closer to 1.0 the attribute value.
  Values above 0.66 describe tracks that are probably made entirely of spoken words. Values between 0.33
  and 0.66 describe tracks that may contain both music and speech, either in sections or layered,
  including such cases as rap music. Values below 0.33 most likely represent music and other
  non-speech-like tracks.
tempo: The overall estimated tempo of a track in beats per minute (BPM). In musical terminology, tempo
  is the speed or pace of a given piece and derives directly from the average beat duration.
time_signatur: A measure from 0.0 to 1.0 describing the musical positiveness conveyed by a track. Tracks
  with high valence sound more positive (e.g. happy, cheerful, euphoric), while tracks with low valence
  sound more negative (e.g. sad, depressed, angry).
"""
TRACK_LOGFILE = Path("logs/track_log.json")
MODIFIED_TRACK_LOGFILE = Path("logs/track_log_modified.json")
BATCH_LENGTH = 100
DELAY = 30
SCOPE = "user-read-currently-playing"
SPOTIFY_CLIENT_ID = config("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = config("SPOTIFY_CLIENT_SECRET")
SPOTIFY_REDIRECT_URI = config("SPOTIFY_REDIRECT_URI")

spotify_authorization = SpotifyOAuth(
    SPOTIFY_CLIENT_ID,
    SPOTIFY_CLIENT_SECRET,
    SPOTIFY_REDIRECT_URI,
    scope=SCOPE,
    # show_dialog=True,
    # open_browser=False,
)
spotify = spotipy.Spotify(auth_manager=spotify_authorization)


af_empty = {
    "duration_ms": 0,
    "acousticness": None,
    "danceability": None,
    "energy": None,
    "instrumentalness": None,
    "key": None,
    "liveness": None,
    "loudness": None,
    "mode": None,
    "speechiness": None,
    "tempo": None,
    "time_signature": None,
    "valence": None,
}


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


def read_track_logfile():
    with open(TRACK_LOGFILE, "r") as json_file:
        return json.load(json_file)


def update_track_logfile(tracks_log):
    with open(MODIFIED_TRACK_LOGFILE, "w") as json_file:
        json.dump(tracks_log, json_file)


def print_track(track):
    print(f"{track = }")


def get_audio_features(tracks: list) -> list:
    try:
        return spotify.audio_features(tracks=tracks)

    except Exception as e:
        print(f"get_track_audio_features, exception {e}, exit the program ...")
        exit()


def main():
    tracks_log = read_track_logfile()
    tracks = tracks_log["tracks"]
    modified_log = {}
    modified_log["spotify_account"] = tracks_log["spotify_account"]
    modified_log["tracks"] = []
    batch = 1
    nr_tracks = len(tracks)
    start = 0
    end = min(start + BATCH_LENGTH, nr_tracks)
    # work in batches of BATCH_LENGTH
    while start < end:

        all_play_time = all(t.get("play_time") for t in tracks[start:end])
        all_acousticness = all(t.get("acousticness") for t in tracks[start:end])
        if not all_play_time or not all_acousticness:
            # get audio features if any in this batch does not contain play_time or acousticness
            tracks_id = [track["id"] for track in tracks[start:end]]
            tracks_af = get_audio_features(tracks_id)
            for index, af in enumerate(tracks_af, start):
                if af is None:
                    af = af_empty
                play_time = tracks[index].get("play_time")
                if not play_time:
                    play_time = f"{datetime.timedelta(seconds=round(af["duration_ms"] * 0.001, 0))}"

                modified_track_record = TrackRecord(
                    id=tracks[index]["id"],
                    played_at=tracks[index]["played_at"],
                    artist=tracks[index]["artist"],
                    name=tracks[index]["name"],
                    play_time=play_time,
                    acousticness=af["acousticness"],
                    danceability=af["danceability"],
                    energy=af["energy"],
                    instrumentalness=af["instrumentalness"],
                    key=af["key"],
                    liveness=af["liveness"],
                    loudness=af["loudness"],
                    mode=af["mode"],
                    speechiness=af["speechiness"],
                    tempo=af["tempo"],
                    time_signature=af["time_signature"],
                    valence=af["valence"],
                )
                modified_log["tracks"].append(modified_track_record.as_dict())

            time.sleep(2)

        else:
            for index in range(start, end):
                modified_log["tracks"].append(tracks[index])

        print(f"Processed batch: {batch:3} ({start:6}: {end:6})", end="\r")
        update_track_logfile(modified_log)
        start += BATCH_LENGTH
        end = min(start + BATCH_LENGTH, nr_tracks)
        batch += 1

    print(f"\ncompleted {batch - 1} batches, {nr_tracks} tracks")


if __name__ == "__main__":
    main()
