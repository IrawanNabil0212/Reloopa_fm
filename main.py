"""
Spotify Album Downloader & Zipper - FastAPI backend (dengan fitur Premium).
Versi dioptimasi: metadata via scraping halaman publik Spotify, unduhan yt-dlp paralel,
cache audio, ZIP tanpa kompresi. Tanpa spotdl, tanpa spotipy, tanpa kredensial Spotify.

Jalankan:
    uvicorn main:app --host 0.0.0.0 --port 8000

Variabel lingkungan (opsional):
    AUDIO_BITRATE                              -> kbps hasil konversi (default 192)
    TRACK_TIMEOUT                              -> batas waktu unduh per lagu (default 180 detik)
    YTDLP_JS_RUNTIME                           -> runtime JS untuk yt-dlp, mis. "node" (default: deno otomatis)
    AUDIO_FORMAT                               -> mp3 (default), flac, ogg, opus, m4a, wav
    MAX_CONCURRENT_JOBS                        -> jumlah job paralel (default 2)
    DOWNLOAD_THREADS                           -> lagu yang diunduh bersamaan (default 4)
    CACHE_MAX_GB                               -> batas ukuran cache_audio/ (default 5, 0 = tanpa batas)
    ZIP_TTL_FREE / ZIP_TTL_PREMIUM             -> umur file zip (default 600 / 3600 detik)
    GOOGLE_CLIENT_ID, SESSION_SECRET           -> login Google & cookie sesi
    MIDTRANS_SERVER_KEY, MIDTRANS_CLIENT_KEY   -> pembayaran (lihat payments.py)
    FREE_LIMIT / PREMIUM_WEEKLY_LIMIT / PASS_LIMIT -> batas kuota (lihat quota.py)
    RATE_LIMIT_IP / RATE_LIMIT_USER            -> maks permintaan unduh per 10 menit
"""

from __future__ import annotations

import asyncio
import itertools
import json
import logging
import os
import re
import shutil
import threading
import time
import uuid
import zipfile
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()  # harus sebelum membaca os.getenv di bawah

from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from google.auth.exceptions import TransportError
from google.auth.transport import requests as google_requests
from google.oauth2 import id_token as google_id_token
from pydantic import BaseModel
import requests
import yt_dlp
from bs4 import BeautifulSoup
from mutagen import File as MutagenFile
from mutagen.id3 import APIC, ID3
from requests.exceptions import RequestException
from starlette.middleware.sessions import SessionMiddleware

import db
import quota
import cover  # <- custom cover (Premium), penyematan via mutagen
from db import init_db, is_premium, upsert_user
from ratelimit import RateLimiter, client_ip
from payments import router as payments_router


# --------------------------------------------------------------------------- #
# Konfigurasi
# --------------------------------------------------------------------------- #
BASE_DIR = Path(__file__).resolve().parent
DOWNLOADS_DIR = BASE_DIR / "downloads"
ZIPS_DIR = BASE_DIR / "zips"
CACHE_DIR = BASE_DIR / "cache_audio"  # cache lagu hasil unduhan (permanen, dibatasi ukuran)
STATIC_DIR = BASE_DIR / "static"  # berisi index.html (frontend)

ZIP_TTL_FREE = int(os.getenv("ZIP_TTL_FREE", "600"))        # 10 menit
ZIP_TTL_PREMIUM = int(os.getenv("ZIP_TTL_PREMIUM", "3600"))  # 1 jam
MAX_CONCURRENT_JOBS = int(os.getenv("MAX_CONCURRENT_JOBS", "2"))
AUDIO_FORMAT = os.getenv("AUDIO_FORMAT", "mp3")
DOWNLOAD_THREADS = max(1, int(os.getenv("DOWNLOAD_THREADS", "4")))
CACHE_MAX_BYTES = int(float(os.getenv("CACHE_MAX_GB", "5")) * 1024**3)
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
SESSION_SECRET = os.getenv("SESSION_SECRET", "dev-only-ganti-di-production")

AUDIO_BITRATE = os.getenv("AUDIO_BITRATE", "192")  # kbps untuk mp3/m4a/ogg/opus
TRACK_TIMEOUT = int(os.getenv("TRACK_TIMEOUT", "180"))  # batas waktu unduh per lagu (detik)
YTDLP_JS_RUNTIME = os.getenv("YTDLP_JS_RUNTIME", "")  # kosong = otomatis (deno); atau "node"

# Header untuk mengambil halaman publik Spotify (bahasa Inggris supaya <title> konsisten)
HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# Tipe link yang diizinkan. Hapus "playlist"/"track" jika hanya ingin album.
SPOTIFY_URL_RE = re.compile(
    r"^https?://open\.spotify\.com/(?:intl-[a-z]{2}/)?"
    r"(?P<kind>album|playlist|track)/(?P<id>[A-Za-z0-9]{22})(?:\?.*)?$"
)
SESSION_ID_RE = re.compile(r"^[0-9a-f]{32}$")
TRACK_ID_RE = re.compile(r"^[A-Za-z0-9]{22}$")

TERMINAL_STATUSES = {"done", "error"}

# Rate limit: per IP (anti bypass akun baru) dan per user, jendela 10 menit
IP_LIMITER = RateLimiter(int(os.getenv("RATE_LIMIT_IP", "30")), 600)
USER_LIMITER = RateLimiter(int(os.getenv("RATE_LIMIT_USER", "10")), 600)
AUTH_LIMITER = RateLimiter(20, 600)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("spotify-zipper")


# --------------------------------------------------------------------------- #
# State sesi (in-memory)
# --------------------------------------------------------------------------- #
@dataclass
class Session:
    user_id: str = ""
    ttl: int = ZIP_TTL_FREE  # umur file ZIP (detik), bergantung tier
    status: str = "queued"  # queued | downloading | zipping | done | error
    events: list[dict] = field(default_factory=list)
    cond: asyncio.Condition = field(default_factory=asyncio.Condition)


SESSIONS: dict[str, Session] = {}
_BG_TASKS: set[asyncio.Task] = set()  # simpan referensi supaya task tidak di-GC

# Antrean prioritas: Premium/Pass (0) diproses lebih dulu dari Free (1).
# Dalam prioritas yang sama, urutan masuk (FIFO).
JOB_QUEUE: asyncio.PriorityQueue | None = None
_JOB_SEQ = itertools.count()
_WORKERS: list[asyncio.Task] = []


class DownloadError(Exception):
    """Error yang pesannya aman ditampilkan ke user."""


# --------------------------------------------------------------------------- #
# Pengunduh audio: yt-dlp (cari di YouTube -> unduh -> konversi dengan ffmpeg)
# --------------------------------------------------------------------------- #
# Pool global: batas total unduhan yang berjalan bersamaan di SEMUA job.
DL_POOL = ThreadPoolExecutor(max_workers=DOWNLOAD_THREADS, thread_name_prefix="ytdlp")
_PP_CODEC = {"ogg": "vorbis"}  # nama codec yt-dlp bila berbeda dari ekstensi
_NAME_LOCK = threading.Lock()


def _ffmpeg_location() -> str | None:
    if shutil.which("ffmpeg"):
        return None  # yt-dlp akan menemukannya sendiri di PATH
    try:  # pakai ffmpeg yang dulu diunduh spotdl (kalau masih ada)
        from spotdl.utils.ffmpeg import get_local_ffmpeg

        path = get_local_ffmpeg()
        return str(path) if path else None
    except Exception:
        return None


FFMPEG_LOCATION = _ffmpeg_location()


def _search_query(meta: dict) -> str:
    artists = ", ".join(meta.get("artists") or [])
    name = meta.get("name") or ""
    return f"{artists} - {name}" if artists else name


def _download_one(track_id: str, meta: dict, work_dir: Path) -> Path:
    """Blocking: cari di YouTube lalu unduh + konversi. Mengembalikan path file hasil."""
    query = _search_query(meta).strip()
    if not query:
        raise FileNotFoundError("nama lagu kosong")

    # Filter durasi supaya tidak terambil mix/live/video panjang (penyebab unduhan "macet").
    dur = int(meta.get("duration") or 0)
    if dur > 0:
        flt = f"duration >= {max(30, int(dur * 0.7))} & duration <= {dur + 90} & !is_live"
    else:
        flt = "duration <= 900 & !is_live"

    deadline = time.monotonic() + TRACK_TIMEOUT

    def _hook(_d) -> None:
        if time.monotonic() > deadline:
            raise TimeoutError(f"unduhan melebihi {TRACK_TIMEOUT} detik")

    opts = {
        "format": "bestaudio/best",
        "outtmpl": str(work_dir / f"{track_id}.%(ext)s"),
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "socket_timeout": 15,
        "retries": 3,
        "fragment_retries": 3,
        "concurrent_fragment_downloads": 4,
        "match_filter": yt_dlp.utils.match_filter_func(flt),
        "max_downloads": 1,                  # berhenti setelah satu kandidat berhasil
        "max_filesize": 50 * 1024 * 1024,    # tolak file raksasa
        "progress_hooks": [_hook],
        "postprocessors": [
            {
                "key": "FFmpegExtractAudio",
                "preferredcodec": _PP_CODEC.get(AUDIO_FORMAT, AUDIO_FORMAT),
                "preferredquality": AUDIO_BITRATE,
            }
        ],
    }
    if FFMPEG_LOCATION:
        opts["ffmpeg_location"] = FFMPEG_LOCATION
    if YTDLP_JS_RUNTIME:
        opts["js_runtimes"] = {YTDLP_JS_RUNTIME: {}}
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            ydl.download([f"ytsearch5:{query}"])  # 5 kandidat, yang lolos filter pertama diambil
    except yt_dlp.utils.DownloadCancelled:  # termasuk MaxDownloadsReached (normal)
        pass
    for f in work_dir.glob(f"{track_id}.*"):
        if f.suffix.lower() == f".{AUDIO_FORMAT}":
            return f
    raise FileNotFoundError(f"tidak ada hasil yang cocok: {query}")


def _fetch_cover(url: str | None) -> bytes | None:
    if not url:
        return None
    try:
        resp = _http.get(url, headers=HTTP_HEADERS, timeout=8)
        resp.raise_for_status()
        return resp.content if len(resp.content) < 5 * 1024 * 1024 else None
    except Exception as exc:
        log.warning("Gagal mengambil cover album: %r", exc)
        return None


def _tag_mp3(path: Path, meta: dict, album: dict, cover_art: bytes | None) -> None:
    """Tulis tag ID3 (judul, artis, album, cover bawaan) dengan mutagen. Hanya untuk MP3."""
    try:
        audio = MutagenFile(str(path), easy=True)
        if audio is None:
            return
        if audio.tags is None:
            audio.add_tags()
        audio["title"] = [meta.get("name") or path.stem]
        if meta.get("artists"):
            audio["artist"] = [", ".join(meta["artists"])]
        if album.get("kind") == "album" and album.get("title"):
            audio["album"] = [album["title"]]
            if album.get("artist"):
                audio["albumartist"] = [album["artist"]]
            if album.get("year"):
                audio["date"] = [str(album["year"])]
        audio.save()
        if cover_art:
            id3 = ID3(str(path))
            id3.delall("APIC")
            mime = "image/png" if cover_art[:4] == b"\x89PNG" else "image/jpeg"
            id3.add(APIC(encoding=3, mime=mime, type=3, desc="Cover", data=cover_art))
            id3.save(v2_version=3)
    except Exception as exc:
        log.warning("Gagal menulis tag untuk %s: %r", path.name, exc)


def _process_track(
    track_id: str, meta: dict, album: dict, cover_art: bytes | None, work_dir: Path, out_dir: Path
) -> None:
    """Berjalan di DL_POOL: unduh -> tag -> simpan ke cache -> pindah ke folder sesi."""
    t0 = time.perf_counter()
    path = _download_one(track_id, meta, work_dir)
    log.info("yt-dlp selesai %.1f detik: %s", time.perf_counter() - t0, _search_query(meta))
    if AUDIO_FORMAT == "mp3":
        _tag_mp3(path, meta, album, cover_art)
    cache_put(track_id, path)  # salinan asli (tanpa custom cover)
    artists = ", ".join(meta.get("artists") or [])
    stem = safe_filename(f"{artists} - {meta.get('name')}" if artists else (meta.get("name") or track_id))
    with _NAME_LOCK:
        dest = unique_path(out_dir, stem, path.suffix.lstrip("."), track_id)
        shutil.move(str(path), str(dest))


# --------------------------------------------------------------------------- #
# Metadata: scraping halaman publik Spotify (tanpa API, tanpa kredensial)
#   1. halaman embed  -> JSON <script id="__NEXT_DATA__"> (judul, artis, daftar lagu, cover)
#   2. fallback       -> tag OpenGraph halaman biasa (+ satu request per lagu untuk album/playlist)
# Struktur halaman ini TIDAK resmi dan bisa berubah kapan saja.
# --------------------------------------------------------------------------- #
META_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="scrape")
_http = requests.Session()
_TRACK_URI_RE = re.compile(r"^spotify:track:([A-Za-z0-9]{22})$")
_TRACK_URL_RE = re.compile(r"/track/([A-Za-z0-9]{22})")


def _http_get(url: str) -> str:
    resp = _http.get(url, headers=HTTP_HEADERS, timeout=6)
    resp.raise_for_status()
    return resp.text


def _split_artists(text: str | None) -> list[str]:
    return [a.strip() for a in re.split(r",[\s\u00a0]*", text or "") if a.strip()]


def _largest_image(sources) -> str | None:
    best = None
    for src in sources or []:
        if isinstance(src, dict) and src.get("url"):
            if best is None or (src.get("width") or 0) > (best.get("width") or 0):
                best = src
    return best["url"] if best else None


def _find_entity(obj, kind: str, sid: str) -> dict | None:
    """Cari objek entity (uri == spotify:<kind>:<id>) di dalam JSON __NEXT_DATA__."""
    want = f"spotify:{kind}:{sid}"
    stack = [obj]
    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            if cur.get("uri") == want and (cur.get("name") or cur.get("title")):
                return cur
            stack.extend(cur.values())
        elif isinstance(cur, list):
            stack.extend(cur)
    return None


def parse_embed_entity(entity: dict, kind: str, sid: str) -> dict:
    title = entity.get("name") or entity.get("title")
    artists_field = entity.get("artists")
    if isinstance(artists_field, list):
        artist_list = [a.get("name") for a in artists_field if isinstance(a, dict) and a.get("name")]
    else:
        artist_list = _split_artists(entity.get("subtitle"))
    release = entity.get("releaseDate")
    iso = release.get("isoString") if isinstance(release, dict) else None
    cover = (
        _largest_image((entity.get("coverArt") or {}).get("sources"))
        or _largest_image((entity.get("visualIdentity") or {}).get("image"))
    )
    tracks: list[dict] = []
    if kind == "track":
        tracks.append(
            {"id": sid, "name": title or "", "artists": artist_list, "duration": (entity.get("duration") or 0) // 1000}
        )
    else:
        for item in entity.get("trackList") or []:
            m = _TRACK_URI_RE.match(str(item.get("uri") or ""))
            if m:
                tracks.append(
                    {
                        "id": m.group(1),
                        "name": item.get("title") or "",
                        "artists": _split_artists(item.get("subtitle")),
                        "duration": (item.get("duration") or 0) // 1000,
                    }
                )
    return {
        "kind": kind,
        "title": title,
        "artist": ", ".join(artist_list) or None,
        "year": int(iso[:4]) if iso and iso[:4].isdigit() else None,
        "cover_url": cover,
        "tracks": tracks,
        "source": "embed",
    }


def parse_open_graph(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    def meta(key: str) -> str | None:
        tag = soup.find("meta", attrs={"property": key}) or soup.find("meta", attrs={"name": key})
        return tag.get("content") if tag and tag.get("content") else None

    page_title = soup.title.get_text() if soup.title else ""
    m = re.search(r" by (.+?) \| Spotify", page_title)  # "X - song and lyrics by Y | Spotify"
    track_ids = []
    for tag in soup.find_all("meta", attrs={"name": "music:song"}) + soup.find_all("meta", attrs={"property": "music:song"}):
        mm = _TRACK_URL_RE.search(tag.get("content") or "")
        if mm and mm.group(1) not in track_ids:
            track_ids.append(mm.group(1))
    return {
        "title": meta("og:title"),
        "artist": m.group(1).strip() if m else None,
        "cover_url": meta("og:image"),
        "track_ids": track_ids,
    }


def _scrape_sync(kind: str, sid: str) -> dict:
    """Blocking: coba halaman embed dulu, lalu fallback OpenGraph."""
    try:
        html = _http_get(f"https://open.spotify.com/embed/{kind}/{sid}")
        tag = BeautifulSoup(html, "html.parser").find("script", id="__NEXT_DATA__")
        if tag and (tag.string or tag.get_text()):
            entity = _find_entity(json.loads(tag.string or tag.get_text()), kind, sid)
            if entity:
                result = parse_embed_entity(entity, kind, sid)
                if result["tracks"]:
                    return result
        log.warning("Halaman embed %s/%s tidak berisi data lagu, pakai fallback OpenGraph.", kind, sid)
    except requests.HTTPError:
        raise
    except Exception as exc:
        log.warning("Parsing embed gagal (%r), pakai fallback OpenGraph.", exc)

    og = parse_open_graph(_http_get(f"https://open.spotify.com/{kind}/{sid}"))
    artists = [og["artist"]] if og["artist"] else []
    if kind == "track":
        tracks = [{"id": sid, "name": og["title"] or "", "artists": artists, "duration": 0}]
    else:
        def one(tid: str) -> dict:
            try:
                t = parse_open_graph(_http_get(f"https://open.spotify.com/track/{tid}"))
                return {"id": tid, "name": t["title"] or "", "artists": [t["artist"]] if t["artist"] else [], "duration": 0}
            except Exception:
                return {"id": tid, "name": "", "artists": [], "duration": 0}

        with ThreadPoolExecutor(max_workers=8) as pool:
            tracks = [t for t in pool.map(one, og["track_ids"][:200]) if t["name"]]
    return {
        "kind": kind,
        "title": og["title"],
        "artist": og["artist"],
        "year": None,
        "cover_url": og["cover_url"],
        "tracks": tracks,
        "source": "opengraph",
    }


ALBUM_CACHE: dict[str, tuple[float, dict]] = {}
ALBUM_CACHE_TTL = 3600  # 1 jam
ALBUM_CACHE_MAX = 500
_INFLIGHT: dict[str, asyncio.Task] = {}  # request identik yang sedang berjalan digabung


# --------------------------------------------------------------------------- #
# Cache audio (cache_audio/{track_id}.{format})
# --------------------------------------------------------------------------- #
_BAD_FN_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')


def safe_filename(name: str) -> str:
    return _BAD_FN_CHARS.sub("_", name).strip(" .")[:150] or "track"


def unique_path(folder: Path, stem: str, ext: str, track_id: str) -> Path:
    p = folder / f"{stem}.{ext}"
    if p.exists():
        p = folder / f"{stem} [{track_id[:6]}].{ext}"
    return p


def cache_path(track_id: str) -> Path:
    return CACHE_DIR / f"{track_id}.{AUDIO_FORMAT}"


def cache_put(track_id: str, src: Path) -> None:
    """Simpan salinan ke cache secara atomik (aman bila ada proses lain yang membaca)."""
    if not TRACK_ID_RE.match(track_id):
        return
    try:
        tmp = CACHE_DIR / f".{uuid.uuid4().hex}.part"
        shutil.copy2(src, tmp)
        os.replace(tmp, cache_path(track_id))
    except OSError as exc:
        log.warning("Gagal menyimpan cache %s: %s", track_id, exc)


def restore_from_cache(
    selected_ids: list[str], track_meta: dict[str, dict], out_dir: Path
) -> tuple[list[str], list[str]]:
    """
    Salin lagu yang sudah ada di cache ke folder sesi.
    PENTING: disalin (bukan hardlink), karena custom cover ditulis mutagen langsung
    ke file di folder sesi; hardlink akan ikut merusak file cache.
    """
    hits, misses = [], []
    for tid in selected_ids:
        src = cache_path(tid)
        if TRACK_ID_RE.match(tid) and src.is_file():
            meta = track_meta.get(tid) or {}
            artists = ", ".join(meta.get("artists") or [])
            stem = safe_filename(f"{artists} - {meta.get('name')}" if artists else (meta.get("name") or tid))
            dest = unique_path(out_dir, stem, AUDIO_FORMAT, tid)
            try:
                shutil.copy2(src, dest)
                os.utime(src, None)  # tandai baru dipakai (untuk pruning LRU)
                hits.append(tid)
                continue
            except OSError as exc:
                log.warning("Cache %s rusak/tidak terbaca: %s", tid, exc)
                src.unlink(missing_ok=True)
        misses.append(tid)
    return hits, misses


def prune_cache() -> None:
    """Hapus file cache paling lama tidak dipakai sampai ukuran di bawah batas."""
    if CACHE_MAX_BYTES <= 0:
        return
    try:
        files = [(f, f.stat()) for f in CACHE_DIR.iterdir() if f.is_file()]
    except OSError:
        return
    total = sum(st.st_size for _, st in files)
    if total <= CACHE_MAX_BYTES:
        return
    for f, st in sorted(files, key=lambda x: x[1].st_mtime):
        f.unlink(missing_ok=True)
        total -= st.st_size
        if total <= CACHE_MAX_BYTES * 0.9:
            break


# --------------------------------------------------------------------------- #
# SSE helper
# --------------------------------------------------------------------------- #
async def emit(
    session_id: str,
    message: str,
    *,
    status: str | None = None,
    level: str = "info",
) -> None:
    """Tambahkan event log ke sesi dan bangunkan semua listener SSE."""
    session = SESSIONS.get(session_id)
    if session is None:
        return
    async with session.cond:
        if status:
            session.status = status
        session.events.append(
            {
                "ts": time.time(),
                "level": level,
                "status": session.status,
                "message": message,
            }
        )
        session.cond.notify_all()


def _fmt_duration(seconds: int) -> str:
    return f"{seconds // 3600} jam" if seconds >= 3600 else f"{seconds // 60} menit"


# --------------------------------------------------------------------------- #
# Helper: rate limit, URL, kepemilikan sesi
# --------------------------------------------------------------------------- #
def enforce_rate_limit(request: Request, user_id: str | None = None) -> None:
    checks = [(IP_LIMITER, f"ip:{client_ip(request)}")]
    if user_id:
        checks.append((USER_LIMITER, f"user:{user_id}"))
    for limiter, key in checks:
        wait = limiter.hit(key)
        if wait:
            raise HTTPException(
                status_code=429,
                detail=f"Terlalu banyak permintaan. Coba lagi dalam {wait} detik.",
                headers={"Retry-After": str(wait)},
            )


def parse_spotify_url(raw: str) -> tuple[str, str, str]:
    match = SPOTIFY_URL_RE.match(raw.strip())
    if not match:
        raise HTTPException(
            status_code=400,
            detail="URL tidak valid. Gunakan link Spotify, contoh: "
            "https://open.spotify.com/album/<id>",
        )
    kind, sid = match.group("kind"), match.group("id")
    return kind, sid, f"https://open.spotify.com/{kind}/{sid}"  # bersihkan ?si=...


def get_owned_session(session_id: str, request: Request) -> Session:
    """Sesi hanya boleh diakses oleh user yang membuatnya."""
    session = SESSIONS.get(session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="Sesi tidak ditemukan atau sudah kedaluwarsa.")
    user = request.session.get("user")
    if not user or user["id"] != session.user_id:
        raise HTTPException(status_code=403, detail="Sesi ini bukan milik akun kamu.")
    return session


def quota_exceeded_message(st: dict, album_total: int) -> str:
    if st["tier"] == "free":
        return (
            f"Kuota gratis kamu sudah habis ({st['limit']} lagu). "
            "Upgrade ke Premium untuk lanjut mengunduh."
        )
    if st["tier"] == "pass":
        return f"Batas Unlimited Pass sudah tercapai ({st['limit']} lagu)."
    return (
        f"Kuota mingguan kamu habis ({st['limit']} lagu). Reset setiap Senin pukul 09.00 WIB. "
        f"Atau beli Unlimited Pass Rp{db.PASS_PRICE:,}".replace(",", ".") + f" ({db.PASS_DAYS} hari)."
    )


# --------------------------------------------------------------------------- #
# Modul 1: Download (cache -> yt-dlp paralel)
# --------------------------------------------------------------------------- #
async def download_spotify_album(
    spotify_url: str,
    session_id: str,
    selected_ids: list[str],
    track_meta: dict[str, dict],
    album_meta: dict,
) -> tuple[Path, list[str]]:
    """
    Siapkan lagu di `selected_ids` (hasil jatah kuota) ke ./downloads/{session_id}/.

    1. Lagu yang sudah ada di cache_audio/ langsung disalin.
    2. Sisanya dicari di YouTube ("ytsearch1:<artis> - <judul>") dan diunduh dengan
       yt-dlp, `DOWNLOAD_THREADS` lagu sekaligus, lalu dikonversi ke AUDIO_FORMAT.

    Mengembalikan (folder hasil, daftar track_id yang BERHASIL).
    """
    out_dir = DOWNLOADS_DIR / session_id
    out_dir.mkdir(parents=True, exist_ok=True)
    loop = asyncio.get_running_loop()

    # ---- Tahap 1: cek cache ------------------------------------------------
    hit_ids, miss_ids = await asyncio.to_thread(restore_from_cache, selected_ids, track_meta, out_dir)
    if hit_ids:
        await emit(session_id, f"{len(hit_ids)} lagu ditemukan di cache, langsung dipakai.")
    if not miss_ids:
        return out_dir, hit_ids

    # ---- Tahap 2: unduh sisanya via yt-dlp (paralel) -----------------------
    work_dir = DOWNLOADS_DIR / "_tmp" / session_id
    work_dir.mkdir(parents=True, exist_ok=True)
    cover_art = await asyncio.to_thread(_fetch_cover, album_meta.get("cover_url")) if AUDIO_FORMAT == "mp3" else None

    total = len(miss_ids)
    done = 0
    await emit(session_id, f"Mengunduh {total} lagu baru ({DOWNLOAD_THREADS} lagu sekaligus)...")

    async def one(tid: str) -> str | None:
        nonlocal done
        meta = track_meta.get(tid) or {}
        label = _search_query(meta) or tid
        try:
            await asyncio.wait_for(
                loop.run_in_executor(
                    DL_POOL, _process_track, tid, meta, album_meta, cover_art, work_dir, out_dir
                ),
                timeout=TRACK_TIMEOUT + 90,  # pengaman terakhir kalau thread macet
            )
        except Exception as exc:  # satu lagu gagal tidak menghentikan yang lain
            done += 1
            log.warning("Gagal mengunduh %s: %r", label, exc)
            await emit(session_id, f"[{done}/{total}] Gagal: {label}", level="warning")
            return None
        done += 1
        await emit(session_id, f"[{done}/{total}] Selesai: {label}")
        return tid

    try:
        results = await asyncio.gather(*(one(t) for t in miss_ids))
    finally:
        await asyncio.to_thread(shutil.rmtree, work_dir, True)
    new_ids = [t for t in results if t]
    await emit(session_id, f"Unduhan selesai: {len(new_ids)}/{total} lagu baru berhasil.")
    await asyncio.to_thread(prune_cache)

    success_ids = hit_ids + new_ids
    if not success_ids:
        raise DownloadError("Semua lagu gagal diunduh. Periksa koneksi atau coba lagi.")
    return out_dir, success_ids


# --------------------------------------------------------------------------- #
# Modul 2: Zip & Cleanup
# --------------------------------------------------------------------------- #
async def create_zip(session_id: str) -> Path:
    """Bungkus ./downloads/{id}/ -> ./zips/{id}.zip (ZIP_STORED, tanpa kompresi)."""
    src_dir = DOWNLOADS_DIR / session_id
    zip_path = ZIPS_DIR / f"{session_id}.zip"

    def _zip() -> Path:
        ZIPS_DIR.mkdir(parents=True, exist_ok=True)
        part = ZIPS_DIR / f"{session_id}.zip.part"
        # Audio sudah terkompresi, jadi ZIP_STORED = instan & ukuran hampir sama
        with zipfile.ZipFile(part, "w", compression=zipfile.ZIP_STORED, allowZip64=True) as zf:
            for f in sorted(src_dir.iterdir()):
                if f.is_file():
                    zf.write(f, arcname=f.name)
        part.replace(zip_path)
        shutil.rmtree(src_dir, ignore_errors=True)  # hanya setelah zip sukses
        return zip_path

    return await asyncio.to_thread(_zip)


async def schedule_cleanup(session_id: str, delay: int) -> None:
    """Hapus file zip (dan state sesi) setelah `delay` detik."""
    await asyncio.sleep(delay)
    (ZIPS_DIR / f"{session_id}.zip").unlink(missing_ok=True)
    shutil.rmtree(DOWNLOADS_DIR / session_id, ignore_errors=True)
    SESSIONS.pop(session_id, None)
    log.info("Sesi %s dibersihkan", session_id)


def _spawn(coro) -> None:
    task = asyncio.create_task(coro)
    _BG_TASKS.add(task)
    task.add_done_callback(_BG_TASKS.discard)


async def process_session(
    session_id: str,
    spotify_url: str,
    user_id: str,
    plan: dict,
    cover_bytes: bytes | None = None,
    track_meta: dict[str, dict] | None = None,
    album_meta: dict | None = None,
) -> None:
    """Pipeline: cache/download -> (embed cover) -> zip -> catat kuota -> jadwalkan penghapusan."""
    session = SESSIONS.get(session_id)
    if session is None:
        await asyncio.to_thread(quota.release, session_id)
        return

    committed = False
    try:
        await emit(session_id, "Memulai proses...", status="downloading")
        _, success_ids = await download_spotify_album(
            spotify_url, session_id, list(plan["selected_ids"]), track_meta or {}, album_meta or {}
        )

        # --- sematkan custom cover (Premium) via mutagen, hanya pada salinan di folder sesi ---
        print(
            f"[PIPELINE] {session_id[:8]} | cover_bytes={'ada, ' + str(len(cover_bytes)) + ' byte' if cover_bytes else 'None'} "
            f"| AUDIO_FORMAT={AUDIO_FORMAT}",
            flush=True,
        )
        if cover_bytes:
            await emit(session_id, "Menyematkan custom cover...")
            ok, failed = await asyncio.to_thread(
                cover.embed_cover_in_folder, DOWNLOADS_DIR / session_id, cover_bytes
            )
            if ok == 0:
                await emit(
                    session_id,
                    "Custom cover tidak dapat diterapkan (hanya didukung untuk format MP3).",
                    level="warning",
                )
            elif failed:
                await emit(
                    session_id,
                    f"Cover disematkan ke {ok} lagu, {failed} lagu gagal.",
                    level="warning",
                )
            else:
                await emit(session_id, f"Custom cover disematkan ke {ok} lagu.")
        # ---------------------------------------------

        await emit(session_id, "Membungkus file menjadi ZIP...", status="zipping")
        await create_zip(session_id)

        # Kuota baru dipotong setelah ZIP benar-benar siap; hanya lagu yang berhasil
        await asyncio.to_thread(quota.finalize, session_id, user_id, plan, success_ids)
        committed = True

        await emit(
            session_id,
            f"ZIP siap diunduh ({len(success_ids)} lagu). File akan dihapus otomatis "
            f"dalam {_fmt_duration(session.ttl)}.",
            status="done",
        )
    except DownloadError as exc:
        await emit(session_id, str(exc), status="error", level="error")
    except Exception:
        log.exception("Error tak terduga pada sesi %s", session_id)
        await emit(
            session_id,
            "Terjadi kesalahan tak terduga di server.",
            status="error",
            level="error",
        )
    finally:
        if not committed:
            # Gagal: kuota yang dipegang dikembalikan, tidak ada yang terpotong
            await asyncio.to_thread(quota.release, session_id)
        if session.status == "error":
            shutil.rmtree(DOWNLOADS_DIR / session_id, ignore_errors=True)
            (ZIPS_DIR / f"{session_id}.zip").unlink(missing_ok=True)
        _spawn(schedule_cleanup(session_id, session.ttl))


async def job_worker() -> None:
    """Ambil job dari antrean prioritas satu per satu."""
    assert JOB_QUEUE is not None
    while True:
        _, _, job = await JOB_QUEUE.get()
        try:
            await process_session(**job)
        except Exception:
            log.exception("Worker gagal memproses job")
        finally:
            JOB_QUEUE.task_done()


def purge_stale_files() -> None:
    """Saat startup: sesi in-memory hilang, jadi semua file sisa tidak bisa diakses lagi -> hapus.
    (cache_audio/ sengaja TIDAK dihapus.)"""
    for zip_file in ZIPS_DIR.glob("*.zip*"):
        zip_file.unlink(missing_ok=True)
    for folder in DOWNLOADS_DIR.iterdir():
        if folder.is_dir():
            shutil.rmtree(folder, ignore_errors=True)
    for part in CACHE_DIR.glob(".*.part"):  # sisa penulisan cache yang terputus
        part.unlink(missing_ok=True)


# --------------------------------------------------------------------------- #
# App & Endpoints
# --------------------------------------------------------------------------- #
@asynccontextmanager
async def lifespan(_: FastAPI):
    global JOB_QUEUE
    DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
    ZIPS_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    purge_stale_files()
    prune_cache()
    init_db()
    quota.clear_reservations()
    (DOWNLOADS_DIR / "_tmp").mkdir(parents=True, exist_ok=True)

    if not shutil.which("ffmpeg") and not FFMPEG_LOCATION:
        log.error("ffmpeg TIDAK ditemukan. Pasang ffmpeg (dan pastikan ada di PATH), "
                  "kalau tidak konversi ke %s akan gagal.", AUDIO_FORMAT)

    JOB_QUEUE = asyncio.PriorityQueue()
    _WORKERS.extend(asyncio.create_task(job_worker()) for _ in range(MAX_CONCURRENT_JOBS))
    try:
        yield
    finally:
        for w in _WORKERS:
            w.cancel()
        _WORKERS.clear()


app = FastAPI(title="Spotify Album Downloader & Zipper", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # ganti dengan origin frontend-mu di production
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(
    SessionMiddleware,
    secret_key=SESSION_SECRET,
    max_age=60 * 60 * 24 * 7,  # login bertahan 7 hari
    same_site="lax",
    https_only=False,  # ubah ke True kalau sudah pakai HTTPS
)

app.include_router(payments_router)


@app.post("/api/download")
async def start_download(
    request: Request,
    url: str = Form(...),
    cover_image: UploadFile | None = File(None),
):
    # 1. Wajib login (frontend menangkap code "login_required" -> popup Google,
    #    simpan URL, lalu panggil ulang endpoint ini setelah login)
    user = request.session.get("user")
    if not user:
        return JSONResponse(
            status_code=401,
            content={"detail": "Silakan login dengan Google untuk mengunduh.", "code": "login_required"},
        )
    enforce_rate_limit(request, user["id"])

    print(
        f"[DOWNLOAD] user={user['id']} | cover_image diterima: {cover_image is not None} | "
        f"filename={getattr(cover_image, 'filename', None)!r} | "
        f"content_type={getattr(cover_image, 'content_type', None)!r}",
        flush=True,
    )

    # 1b. Custom cover (khusus Premium/Pass): validasi SEBELUM kuota dipesan
    cover_bytes: bytes | None = None
    if cover_image is not None and cover_image.filename:
        try:
            st = await asyncio.to_thread(quota.get_status, user["id"])
            if st["tier"] == "free":
                print(f"[DOWNLOAD] Cover DITOLAK: tier={st['tier']}", flush=True)
                return JSONResponse(
                    status_code=403,
                    content={
                        "detail": "Custom cover hanya untuk pengguna Premium.",
                        "code": "premium_required",
                    },
                )
            try:
                cover_bytes = (await cover.validate_cover_upload(cover_image)).data
                print(f"[DOWNLOAD] tier={st['tier']} | cover tervalidasi: {len(cover_bytes)} byte", flush=True)
            except cover.CoverError as exc:
                raise HTTPException(status_code=400, detail=str(exc))
        finally:
            await cover_image.close()

    # 2. Ambil daftar lagu (dari cache metadata / scraping)
    _, _, url = parse_spotify_url(url)
    info = await fetch_album_info(url)
    track_meta = {t["id"]: t for t in info["tracks"] if t.get("id")}
    track_ids = list(track_meta)
    if not track_ids:
        raise HTTPException(status_code=404, detail="Tidak ada lagu yang ditemukan pada link tersebut.")

    # 3. Cek & reservasi kuota (atomik)
    session_id = uuid.uuid4().hex
    try:
        plan = await asyncio.to_thread(quota.reserve, user["id"], session_id, track_ids)
    except quota.QuotaExceeded as exc:
        return JSONResponse(
            status_code=402,
            content={
                "detail": quota_exceeded_message(exc.status, len(track_ids)),
                "code": "quota_exceeded",
                "quota": exc.status,
            },
        )

    # 4. Buat sesi & masukkan ke antrean prioritas
    try:
        tier = plan["tier"]
        ttl = ZIP_TTL_FREE if tier == "free" else ZIP_TTL_PREMIUM
        SESSIONS[session_id] = Session(user_id=user["id"], ttl=ttl)
        await emit(session_id, "Permintaan diterima, menunggu antrian...")

        allowed = len(plan["selected_ids"])
        message = None
        if plan["partial"]:
            message = (
                f"Sisa kuota {plan['remaining_before']} lagu, album ini {plan['total']} lagu. "
                f"{allowed} lagu pertama akan diunduh."
            )
            await emit(session_id, message, level="warning")

        priority = 1 if tier == "free" else 0
        await JOB_QUEUE.put((
            priority,
            next(_JOB_SEQ),
            {
                "session_id": session_id,
                "spotify_url": url,
                "user_id": user["id"],
                "plan": plan,
                "cover_bytes": cover_bytes,
                "track_meta": track_meta,
                "album_meta": {
                    "kind": info.get("kind"),
                    "title": info.get("title"),
                    "artist": info.get("artist"),
                    "year": info.get("year"),
                    "cover_url": info.get("cover_url"),
                },
            },
        ))
    except Exception:
        SESSIONS.pop(session_id, None)
        await asyncio.to_thread(quota.release, session_id)
        raise

    return {
        "session_id": session_id,
        "tier": tier,
        "total": plan["total"],
        "allowed": allowed,
        "partial": plan["partial"],
        "message": message,
    }


@app.get("/api/quota")
async def quota_status(request: Request):
    user = request.session.get("user")
    if not user:
        raise HTTPException(status_code=401, detail="Belum login.")
    status = await asyncio.to_thread(quota.get_status, user["id"])
    return {
        **status,
        "prices": {"premium": db.PREMIUM_PRICE, "pass": db.PASS_PRICE, "pass_days": db.PASS_DAYS},
    }


@app.get("/api/progress/{session_id}")
async def progress(session_id: str, request: Request):
    session = get_owned_session(session_id, request)

    async def event_stream():
        idx = 0
        while True:
            keepalive = False
            async with session.cond:
                if idx >= len(session.events) and session.status not in TERMINAL_STATUSES:
                    try:
                        await asyncio.wait_for(session.cond.wait(), timeout=15)
                    except asyncio.TimeoutError:
                        keepalive = True
                batch = session.events[idx:]
                idx += len(batch)
                terminal = session.status in TERMINAL_STATUSES

            if keepalive and not batch:
                yield ": keep-alive\n\n"  # cegah koneksi diputus proxy
                continue

            for ev in batch:
                yield f"event: log\ndata: {json.dumps(ev, ensure_ascii=False)}\n\n"

            if terminal:
                # Event penutup: "done" atau "error"
                yield (
                    f"event: {session.status}\n"
                    f"data: {json.dumps({'status': session.status})}\n\n"
                )
                return

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # matikan buffering di nginx
        },
    )


@app.get("/api/get-zip/{session_id}")
async def get_zip(session_id: str, request: Request):
    if not SESSION_ID_RE.match(session_id):  # cegah path traversal
        raise HTTPException(status_code=400, detail="session_id tidak valid.")

    session = get_owned_session(session_id, request)
    if session.status == "error":
        raise HTTPException(status_code=410, detail="Proses gagal, tidak ada file untuk diunduh.")
    if session.status != "done":
        raise HTTPException(status_code=409, detail="File masih diproses, tunggu hingga selesai.")

    zip_path = ZIPS_DIR / f"{session_id}.zip"
    if not zip_path.is_file():
        raise HTTPException(status_code=404, detail="File tidak ditemukan atau sudah kedaluwarsa.")
    return FileResponse(
        zip_path,
        media_type="application/zip",
        filename=f"spotify-album-{session_id[:8]}.zip",
    )


# --------------------------------------------------------------------------- #
# Auth: Login dengan Google
# --------------------------------------------------------------------------- #
class GoogleLoginRequest(BaseModel):
    credential: str


@app.get("/api/auth/config")
async def auth_config():
    return {"google_client_id": GOOGLE_CLIENT_ID}


@app.post("/api/auth/google")
async def auth_google(body: GoogleLoginRequest, request: Request):
    wait = AUTH_LIMITER.hit(f"ip:{client_ip(request)}")
    if wait:
        raise HTTPException(
            status_code=429,
            detail=f"Terlalu banyak percobaan login. Coba lagi dalam {wait} detik.",
            headers={"Retry-After": str(wait)},
        )
    if not GOOGLE_CLIENT_ID:
        raise HTTPException(status_code=500, detail="GOOGLE_CLIENT_ID belum diatur di server.")
    try:
        info = await asyncio.to_thread(
            google_id_token.verify_oauth2_token,
            body.credential,
            google_requests.Request(),
            GOOGLE_CLIENT_ID,
            clock_skew_in_seconds=10,
        )
    except TransportError:
        raise HTTPException(status_code=503, detail="Gagal menghubungi Google. Coba lagi nanti.")
    except ValueError:
        raise HTTPException(status_code=401, detail="Token Google tidak valid.")

    if not info.get("email_verified"):
        raise HTTPException(status_code=401, detail="Email Google belum terverifikasi.")

    user = {
        "id": info["sub"],
        "email": info.get("email"),
        "name": info.get("name"),
        "picture": info.get("picture"),
    }
    request.session["user"] = user
    upsert_user(user)
    return {**user, "is_premium": is_premium(user["id"])}


@app.get("/api/auth/me")
async def auth_me(request: Request):
    user = request.session.get("user")
    if not user:
        raise HTTPException(status_code=401, detail="Belum login.")
    return {**user, "is_premium": is_premium(user["id"])}


@app.post("/api/auth/logout")
async def auth_logout(request: Request):
    request.session.clear()
    return {"ok": True}


@app.get("/login", include_in_schema=False)
async def login_page():
    page = STATIC_DIR / "login.html"
    if not page.is_file():
        raise HTTPException(status_code=404, detail="static/login.html tidak ditemukan.")
    return FileResponse(page)


@app.get("/premium", include_in_schema=False)
async def premium_page(request: Request):
    if not request.session.get("user"):
        return RedirectResponse("/")
    page = STATIC_DIR / "premium.html"
    if not page.is_file():
        raise HTTPException(status_code=404, detail="static/premium.html tidak ditemukan.")
    return FileResponse(page)


@app.get("/premium/transfer-bank", include_in_schema=False)
async def transfer_bank_page(request: Request):
    if not request.session.get("user"):
        return RedirectResponse("/")
    page = STATIC_DIR / "transfer-bank.html"
    if not page.is_file():
        raise HTTPException(status_code=404, detail="static/transfer-bank.html tidak ditemukan.")
    return FileResponse(page)


@app.get("/premium/qris", include_in_schema=False)
async def qris_page(request: Request):
    if not request.session.get("user"):
        return RedirectResponse("/")
    page = STATIC_DIR / "qris.html"
    if not page.is_file():
        raise HTTPException(status_code=404, detail="static/qris.html tidak ditemukan.")
    return FileResponse(page)


@app.get("/premium/kartu", include_in_schema=False)
async def kartu_page(request: Request):
    if not request.session.get("user"):
        return RedirectResponse("/")
    page = STATIC_DIR / "kartu.html"
    if not page.is_file():
        raise HTTPException(status_code=404, detail="static/kartu.html tidak ditemukan.")
    return FileResponse(page)


@app.get("/pembayaran/proses", include_in_schema=False)
async def pembayaran_proses(request: Request):
    if not request.session.get("user"):
        return RedirectResponse("/")
    page = STATIC_DIR / "proses.html"
    if not page.is_file():
        raise HTTPException(status_code=404, detail="static/proses.html tidak ditemukan.")
    return FileResponse(page)


@app.get("/pembayaran/sukses", include_in_schema=False)
async def pembayaran_sukses(request: Request, order_id: str = ""):
    user = request.session.get("user")
    if not user:
        return RedirectResponse("/")

    # Hanya tampil untuk order milik user ini yang benar-benar sudah lunas
    order = db.get_order(order_id) if order_id else None
    if not order or order["user_id"] != user["id"] or order["status"] != "paid":
        return RedirectResponse("/premium")

    nama = "Premium" if order["product"] == "premium" else "Unlimited Pass"
    return HTMLResponse(f"""<!DOCTYPE html>
<html lang="id"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Pembayaran berhasil</title>
<style>
  body{{margin:0;min-height:100dvh;display:grid;place-items:center;padding:24px;
       font-family:Inter,system-ui,sans-serif;background:#e9eaee;color:#0f0f12}}
  .card{{max-width:420px;width:100%;padding:32px 24px;text-align:center;background:#fff;
        border-radius:18px;box-shadow:0 8px 30px rgba(20,20,40,.12)}}
  h1{{font-size:24px;margin:16px 0 8px}}
  p{{color:#3f4047;line-height:1.5;margin:0 0 24px}}
  a{{display:block;padding:14px;border-radius:14px;background:#684ca0;color:#fff;
    font-weight:700;text-decoration:none}}
</style></head><body><main class="card">
<svg width="56" height="56" viewBox="0 0 24 24" fill="none" stroke="#684ca0" stroke-width="2"
     stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><path d="M8 12.5l3 3 5-6"/></svg>
<h1>Pembayaran berhasil</h1>
<p>Paket {nama} sudah aktif di akun kamu.</p>
<a href="/">Kembali ke beranda</a></main></body></html>""")


# --------------------------------------------------------------------------- #
# Load / info album (publik, tanpa login) - scraping halaman Spotify
# --------------------------------------------------------------------------- #
async def _load_from_spotify(kind: str, sid: str, url: str) -> dict:
    loop = asyncio.get_running_loop()
    t0 = time.perf_counter()
    try:
        result = await loop.run_in_executor(META_EXECUTOR, _scrape_sync, kind, sid)
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else None
        log.warning("Scraping HTTP %s untuk %s", status, url)
        if status in (400, 404):
            raise HTTPException(status_code=404, detail="Link Spotify tidak ditemukan atau tidak dapat diakses.")
        if status == 429:
            raise HTTPException(status_code=503, detail="Spotify sedang membatasi permintaan. Coba lagi sebentar lagi.")
        raise HTTPException(status_code=502, detail="Gagal mengambil data dari Spotify. Coba lagi nanti.")
    except (RequestException, ValueError) as exc:
        log.warning("Scraping gagal untuk %s: %r", url, exc)
        raise HTTPException(status_code=502, detail="Gagal terhubung ke Spotify. Coba lagi nanti.")

    if not result["tracks"]:
        raise HTTPException(
            status_code=404,
            detail="Tidak ada lagu yang ditemukan pada link tersebut (atau format halaman Spotify berubah).",
        )
    log.info("load %s via %s: %d lagu dalam %.2f detik", kind, result.get("source"), len(result["tracks"]), time.perf_counter() - t0)
    return result


async def fetch_album_info(url: str) -> dict:
    """Ambil info album/playlist/track (cache + scraping). `url` harus sudah dibersihkan."""
    match = SPOTIFY_URL_RE.match(url)
    if not match:
        raise HTTPException(status_code=400, detail="URL tidak valid. Gunakan link Spotify.")

    cached = ALBUM_CACHE.get(url)
    if cached and time.time() - cached[0] < ALBUM_CACHE_TTL:
        return cached[1]

    # Request identik yang sedang berjalan digabung jadi satu panggilan ke Spotify
    task = _INFLIGHT.get(url)
    if task is None:
        task = asyncio.create_task(_load_from_spotify(match.group("kind"), match.group("id"), url))
        _INFLIGHT[url] = task
        task.add_done_callback(lambda _t, u=url: _INFLIGHT.pop(u, None))
    result = await asyncio.shield(task)

    if len(ALBUM_CACHE) >= ALBUM_CACHE_MAX:  # buang entri kedaluwarsa / terlama
        now = time.time()
        for k in [k for k, (ts, _) in ALBUM_CACHE.items() if now - ts >= ALBUM_CACHE_TTL]:
            ALBUM_CACHE.pop(k, None)
        while len(ALBUM_CACHE) >= ALBUM_CACHE_MAX:
            ALBUM_CACHE.pop(min(ALBUM_CACHE, key=lambda k: ALBUM_CACHE[k][0]))
    ALBUM_CACHE[url] = (time.time(), result)
    return result


@app.get("/api/load")
async def load_link(url: str):
    _, _, clean = parse_spotify_url(url)
    return await fetch_album_info(clean)


@app.get("/api/album-info")  # alias lama, tetap didukung frontend yang ada
async def album_info(url: str):
    _, _, clean = parse_spotify_url(url)
    return await fetch_album_info(clean)


# --------------------------------------------------------------------------- #
# Frontend: sajikan halaman web dari FastAPI
# --------------------------------------------------------------------------- #
@app.get("/album", include_in_schema=False)
async def album_page():
    page = STATIC_DIR / "album.html"
    if not page.is_file():
        raise HTTPException(status_code=404, detail="static/album.html tidak ditemukan.")
    return FileResponse(page)


@app.get("/", include_in_schema=False)
async def home():
    index = STATIC_DIR / "index.html"
    if not index.is_file():
        raise HTTPException(status_code=404, detail="static/index.html tidak ditemukan.")
    return FileResponse(index)


# Aset tambahan (gambar, css, js) bisa ditaruh di folder static/ dan diakses lewat /static/...
app.mount("/static", StaticFiles(directory=STATIC_DIR, check_dir=False), name="static")