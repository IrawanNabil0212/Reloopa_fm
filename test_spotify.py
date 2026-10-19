from dotenv import load_dotenv
import os
from spotipy import Spotify
from spotipy.oauth2 import SpotifyClientCredentials

load_dotenv()
cid = os.getenv("SPOTIFY_CLIENT_ID")
sec = os.getenv("SPOTIFY_CLIENT_SECRET")
print("ID terbaca:", bool(cid), "| panjang:", len(cid or ""))
print("Secret terbaca:", bool(sec), "| panjang:", len(sec or ""))

sp = Spotify(auth_manager=SpotifyClientCredentials(client_id=cid, client_secret=sec))
try:
    t = sp.track("2LmwUKeGCJZUKHKwOrX3Xb")
    print("BERHASIL:", t["name"])
except Exception as e:
    print("GAGAL:", repr(e))