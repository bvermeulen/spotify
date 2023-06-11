from dataclasses import dataclass, asdict
import time
import datetime
from pathlib import Path
import json
from decouple import config
import spotipy
from spotipy.oauth2 import SpotifyOAuth

TRACK_LOGFILE= Path('logs/track_log.json')
TIME_DELAY = 5
MINIMUM_PLAY_TIME = 20
SCOPE = 'user-read-currently-playing'
SPOTIFY_CLIENT_ID = config('SPOTIFY_CLIENT_ID')
SPOTIFY_CLIENT_SECRET = config('SPOTIFY_CLIENT_SECRET')
SPOTIFY_REDIRECT_URI = config('SPOTIFY_REDIRECT_URI')

spotify_authorization = SpotifyOAuth(
    SPOTIFY_CLIENT_ID,
    SPOTIFY_CLIENT_SECRET,
    SPOTIFY_REDIRECT_URI,
    scope=SCOPE,
    #show_dialog=True,
    #open_browser=False,
)
spotify = spotipy.Spotify(auth_manager=spotify_authorization)


@dataclass
class TrackRecord():
    played_at: str
    name: str
    id: int
    artist: str
    play_time: str

    def as_dict(self):
        return asdict(self)


def read_track_logfile():
    with open(TRACK_LOGFILE, 'r') as json_file:
        return json.load(json_file)


def update_track_logfile(tracks_log):
    with open(TRACK_LOGFILE, 'w') as json_file:
        json.dump(tracks_log, json_file)


def print_track(track):
    print(f'{track = }')


def get_track_update():
    track = None
    try:
        track = spotify.current_user_playing_track()

    except Exception as e:
        print(f'Exception occured at {time.ctime()}: {e}')
        time.sleep(60)

    return track


def main():
    tracks_log = read_track_logfile()
    track_id = None

    while True:
        track = get_track_update()
        new_track = (
            track and track['item'] and track_id != track['item']['id']
        )

        # if there is a new_track or no track is playing then log the previous
        # track only if played for 20s or more
        if new_track or (track is None and track_id):
            if track_id:
                play_time = (datetime.datetime.now() - track_played_at).total_seconds()

            else:
                play_time = 0

            if play_time > MINIMUM_PLAY_TIME:
                track_record = TrackRecord(
                    played_at=track_played_at.strftime("%Y-%B-%d %H:%M:%S"),
                    id=track_id,
                    artist=track_artist,
                    name=track_name,
                    play_time=datetime.datetime.utcfromtimestamp(play_time).strftime('%H:%M:%S')
                )
                print_track(track_record)
                tracks_log['tracks'].append(track_record.as_dict())
                update_track_logfile(tracks_log)

        # update track attributes
        if new_track:
            track_id = track['item']['id']
            track_played_at = datetime.datetime.now()
            track_name = track['item']['name']
            track_artist = track['item']['artists'][0]['name']

        if track is None:
            track_id = None

        time.sleep(TIME_DELAY)


if __name__ == '__main__':
    main()
