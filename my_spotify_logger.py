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
SCOPE = 'user-read-currently-playing'
SPOTIFY_CLIENT_ID = config('SPOTIFY_CLIENT_ID')
SPOTIFY_CLIENT_SECRET = config('SPOTIFY_CLIENT_SECRET')
SPOTIFY_REDIRECT_URI = config('SPOTIFY_REDIRECT_URI')

spotify_authorization = SpotifyOAuth(
    SPOTIFY_CLIENT_ID,
    SPOTIFY_CLIENT_SECRET,
    SPOTIFY_REDIRECT_URI,
    scope=SCOPE,
)
spotify = spotipy.Spotify(auth_manager=spotify_authorization)


@dataclass
class TrackRecord():
    played_at: str
    name: str
    id: int
    artist: str

    def as_dict(self):
        return asdict(self)


def read_track_logfile():
    with open(TRACK_LOGFILE, 'r') as json_file:
        return json.load(json_file)


def update_track_logfile(tracks_log):
    with open(TRACK_LOGFILE, 'w') as json_file:
        json.dump(tracks_log, json_file)


def print_track(track):
    print(f'{track.played_at}, id: {track.id}\n'
          f'artist: {track.artist}, {track.name}')


def main():
    tracks_log = read_track_logfile()
    track_id = None
    valid_track = False

    while True:
        new_track = spotify.current_user_playing_track()
        valid_track = (
            new_track and new_track['item'] and track_id != new_track['item']['id']
        )

        if valid_track:
            track_id = new_track['item']['id']
            track_record = TrackRecord(
                played_at=datetime.datetime.now().strftime("%Y-%B-%d %H:%M:%S"),
                id=track_id,
                artist=new_track['item']['artists'][0]['name'],
                name=new_track['item']['name'],
            )
            print_track(track_record)
            tracks_log['tracks'].append(track_record.as_dict())
            update_track_logfile(tracks_log)

        time.sleep(TIME_DELAY)

if __name__ == '__main__':
    main()