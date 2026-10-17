"""
Spotify Album Downloader & Zipper - FastAPI backend (dengan fitur Premium).

Jalankan:
    uvicorn main:app --host 0.0.0.0 --port 8000

Variabel lingkungan (opsional):
    SPOTIFY_CLIENT_ID / SPOTIFY_CLIENT_SECRET  -> kredensial Spotify milikmu sendiri
    AUDIO_FORMAT                               -> mp3 (default), flac, ogg, opus, m4a, wav
    MAX_CONCURRENT_JOBS                        -> jumlah job paralel (default 2)
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
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from google.auth.exceptions import TransportError
from google.auth.transport import requests as google_requests
from google.oauth2 import id_token as google_id_token
from pydantic import BaseModel
from requests.exceptions import RequestException
from spotdl import Spotdl
from starlette.middleware.sessions import SessionMiddleware

from dotenv import load_dotenv
load_dotenv()

import db
import quota
import cover  # <- custom cover (Premium)
from db import init_db, is_premium, upsert_user
from ratelimit import RateLimiter, client_ip
from payments import router as payments_router, pages_router


# --------------------------------------------------------------------------- #
# Konfigurasi
# --------------------------------------------------------------------------- #
BASE_DIR = Path(__file__).resolve().parent
DOWNLOADS_DIR = BASE_DIR / "downloads"
ZIPS_DIR = BASE_DIR / "zips"
STATIC_DIR = BASE_DIR / "static"  # berisi index.html (frontend)

ZIP_TTL_FREE = int(os.getenv("ZIP_TTL_FREE", "600"))        # 10 menit
ZIP_TTL_PREMIUM = int(os.getenv("ZIP_TTL_PREMIUM", "3600"))  # 1 jam
MAX_CONCURRENT_JOBS = int(os.getenv("MAX_CONCURRENT_JOBS", "2"))
AUDIO_FORMAT = os.getenv("AUDIO_FORMAT", "mp3")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
SESSION_SECRET = os.getenv("SESSION_SECRET", "dev-only-ganti-di-production")

# Kredensial default bawaan spotdl (dipakai bersama banyak orang, mudah kena
# rate limit). Sebaiknya ganti dengan kredensial dari developer.spotify.com.
SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID", "5f573c9620494bae87890c0f08a60293")
SPOTIFY_CLIENT_SECRET = os.getenv(
    "SPOTIFY_CLIENT_SECRET", "212476d9b0f3472eaa762d90b19b0ba8"
)

# Tipe link yang diizinkan. Hapus "playlist"/"track" jika hanya ingin album.
SPOTIFY_URL_RE = re.compile(
    r"^https?://open\.spotify\.com/(?:intl-[a-z]{2}/)?"
    r"(?P<kind>album|playlist|track)/(?P<id>[A-Za-z0-9]{22})(?:\?.*)?$"
)
SESSION_ID_RE = re.compile(r"^[0-9a-f]{32}$")

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


# Satu thread khusus untuk semua pekerjaan spotdl (instance spotdl hanya boleh satu)
SPOTDL_EXECUTOR = ThreadPoolExecutor(max_workers=1, thread_name_prefix="spotdl")
_spotdl_client: Spotdl | None = None


def get_spotdl() -> Spotdl:
    """Dibuat sekali, dan hanya boleh dipanggil dari thread SPOTDL_EXECUTOR."""
    global _spotdl_client
    if _spotdl_client is None:
        _spotdl_client = Spotdl(
            client_id=SPOTIFY_CLIENT_ID,
            client_secret=SPOTIFY_CLIENT_SECRET,
            downloader_settings={
                "output": str(DOWNLOADS_DIR / "_tmp" / "{artists} - {title}.{output-ext}"),
                "format": AUDIO_FORMAT,
                "threads": 1,
            },
        )
    return _spotdl_client


ALBUM_CACHE: dict[str, tuple[float, dict]] = {}
ALBUM_CACHE_TTL = 3600  # 1 jam


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
# Modul 1: Download
# --------------------------------------------------------------------------- #
async def download_spotify_album(
    spotify_url: str, session_id: str, selected_ids: set[str]
) -> tuple[Path, list[str]]:
    """
    Unduh lagu yang ada di `selected_ids` (hasil jatah kuota) ke ./downloads/{session_id}/.

    spotdl bersifat sinkron/blocking, jadi dijalankan di thread terpisah dan
    progress per lagu dikirim balik ke event loop lewat `emit`.
    Mengembalikan (folder hasil unduhan, daftar track_id yang BERHASIL).
    """
    out_dir = DOWNLOADS_DIR / session_id
    out_dir.mkdir(parents=True, exist_ok=True)
    loop = asyncio.get_running_loop()

    def push(message: str, level: str = "info") -> None:
        asyncio.run_coroutine_threadsafe(
            emit(session_id, message, level=level), loop
        )

    def worker() -> list[str]:
        client = get_spotdl()

        push("Mengambil metadata dari Spotify...")
        try:
            songs = client.search([spotify_url])
        except RequestException as exc:
            raise DownloadError(
                "Gagal terhubung ke Spotify (kesalahan jaringan). Coba lagi nanti."
            ) from exc
        except Exception as exc:  # spotdl melempar beberapa jenis error
            log.warning("Search gagal untuk %s: %s", spotify_url, exc)
            raise DownloadError(
                "Link Spotify tidak ditemukan atau tidak dapat diakses."
            ) from exc

        if not songs:
            raise DownloadError("Tidak ada lagu yang ditemukan pada link tersebut.")

        found = len(songs)
        songs = [s for s in songs if s.song_id in selected_ids]  # sesuai jatah kuota
        if not songs:
            raise DownloadError("Tidak ada lagu yang cocok dengan jatah unduhanmu.")

        total = len(songs)
        if total < found:
            push(f"Ditemukan {found} lagu. Mengunduh {total} lagu sesuai sisa kuota...")
        else:
            push(f"Ditemukan {total} lagu. Mulai mengunduh...")

        ok_ids: list[str] = []
        for index, song in enumerate(songs, start=1):
            label = f"{song.artist} - {song.name}"
            push(f"[{index}/{total}] Mengunduh: {label}")
            try:
                _, path = client.download(song)
            except RequestException:
                push(f"[{index}/{total}] Gagal (jaringan): {label}", "warning")
                continue
            except Exception as exc:
                log.warning("Gagal mengunduh %s: %s", label, exc)
                push(f"[{index}/{total}] Gagal: {label}", "warning")
                continue

            if path is None:
                push(f"[{index}/{total}] Dilewati (tidak ditemukan): {label}", "warning")
            else:
                shutil.move(str(path), str(out_dir / Path(path).name))  # pindah ke folder sesi
                ok_ids.append(song.song_id)
                push(f"[{index}/{total}] Selesai: {label}")

        push(f"Unduhan selesai: {len(ok_ids)}/{total} lagu berhasil.")
        return ok_ids

    success_ids = await loop.run_in_executor(SPOTDL_EXECUTOR, worker)
    if not success_ids:
        raise DownloadError("Semua lagu gagal diunduh. Periksa koneksi atau coba lagi.")
    return out_dir, success_ids


# --------------------------------------------------------------------------- #
# Modul 2: Zip & Cleanup
# --------------------------------------------------------------------------- #
async def create_zip(session_id: str) -> Path:
    """Kompres ./downloads/{id}/ -> ./zips/{id}.zip lalu hapus folder mentah."""
    src_dir = DOWNLOADS_DIR / session_id
    zip_path = ZIPS_DIR / f"{session_id}.zip"

    def _zip() -> Path:
        ZIPS_DIR.mkdir(parents=True, exist_ok=True)
        shutil.make_archive(str(ZIPS_DIR / session_id), "zip", root_dir=src_dir)
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
    cover_bytes: bytes | None = None,  # <- baru
) -> None:
    """Pipeline: download -> (embed cover) -> zip -> catat kuota -> jadwalkan penghapusan."""
    session = SESSIONS.get(session_id)
    if session is None:
        await asyncio.to_thread(quota.release, session_id)
        return

    committed = False
    try:
        await emit(session_id, "Memulai proses...", status="downloading")
        _, success_ids = await download_spotify_album(
            spotify_url, session_id, set(plan["selected_ids"])
        )

        # --- baru: sematkan custom cover (Premium) ---
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

        await emit(session_id, "Mengompresi file menjadi ZIP...", status="zipping")
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
    """Saat startup: sesi in-memory hilang, jadi semua file sisa tidak bisa diakses lagi -> hapus."""
    for zip_file in ZIPS_DIR.glob("*.zip"):
        zip_file.unlink(missing_ok=True)
    for folder in DOWNLOADS_DIR.iterdir():
        if folder.is_dir():
            shutil.rmtree(folder, ignore_errors=True)


# --------------------------------------------------------------------------- #
# App & Endpoints
# --------------------------------------------------------------------------- #
@asynccontextmanager
async def lifespan(_: FastAPI):
    global JOB_QUEUE
    DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
    ZIPS_DIR.mkdir(parents=True, exist_ok=True)
    purge_stale_files()
    init_db()
    quota.clear_reservations()
    (DOWNLOADS_DIR / "_tmp").mkdir(parents=True, exist_ok=True)

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
app.include_router(pages_router)


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

    # 2. Ambil daftar lagu (dari cache album-info kalau ada)
    _, _, url = parse_spotify_url(url)
    info = await fetch_album_info(url)
    track_ids = [t["id"] for t in info["tracks"] if t.get("id")]
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
                "cover_bytes": cover_bytes,  # <- baru
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
# Info album (publik, tanpa login)
# --------------------------------------------------------------------------- #
async def fetch_album_info(url: str) -> dict:
    """Ambil info album/playlist/track (dengan cache). `url` harus sudah dibersihkan."""
    match = SPOTIFY_URL_RE.match(url)
    if not match:
        raise HTTPException(status_code=400, detail="URL tidak valid. Gunakan link Spotify.")

    kind = match.group("kind")
    cached = ALBUM_CACHE.get(url)
    if cached and time.time() - cached[0] < ALBUM_CACHE_TTL:
        return cached[1]

    loop = asyncio.get_running_loop()
    t0 = time.perf_counter()
    result = None

    # Jalur cepat untuk album: satu panggilan ke Spotify
    if kind == "album":
        def _fast():
            from spotdl.utils.spotify import SpotifyClient
            get_spotdl()  # pastikan koneksi Spotify sudah dibuat
            sp = SpotifyClient()
            data = sp.album(match.group("id"))
            tracks_obj = data.get("tracks") or {}

            items = list(tracks_obj.get("items", []))
            page = tracks_obj
            while page.get("next"):          # ikuti halaman berikutnya kalau ada
                page = sp.next(page)
                items.extend(page.get("items", []))
            if not items:
                raise ValueError("daftar lagu kosong")
            total = data.get("total_tracks")
            if total and len(items) < total:
                raise ValueError(f"daftar lagu tidak lengkap ({len(items)}/{total})")

            return {
                "title": data.get("name"),
                "artist": ", ".join(a["name"] for a in data.get("artists", []) if a.get("name")),
                "year": int(data["release_date"][:4]) if data.get("release_date") else None,
                "cover_url": data["images"][0]["url"] if data.get("images") else None,
                "tracks": [
                    {"id": t.get("id"), "name": t.get("name"), "duration": (t.get("duration_ms") or 0) // 1000}
                    for t in items
                ],
            }

        try:
            result = await loop.run_in_executor(SPOTDL_EXECUTOR, _fast)
            log.info("album-info (jalur cepat): %.1f detik", time.perf_counter() - t0)
        except Exception as exc:
            log.warning("Jalur cepat gagal (%r), memakai spotdl.", exc)

    # Jalur lama lewat spotdl (playlist/track, atau kalau jalur cepat gagal)
    if result is None:
        def _fetch():
            return get_spotdl().search([url])

        try:
            songs = await loop.run_in_executor(SPOTDL_EXECUTOR, _fetch)
        except RequestException:
            raise HTTPException(status_code=502, detail="Gagal terhubung ke Spotify. Coba lagi nanti.")
        except Exception as exc:
            log.warning("album-info gagal untuk %s: %r", url, exc, exc_info=True)
            raise HTTPException(status_code=404, detail="Link Spotify tidak ditemukan atau tidak dapat diakses.")
        if not songs:
            raise HTTPException(status_code=404, detail="Tidak ada lagu yang ditemukan pada link tersebut.")

        first = songs[0]
        title = {"album": first.album_name, "playlist": first.list_name, "track": first.name}[kind] or first.album_name
        result = {
            "title": title,
            "artist": first.album_artist or first.artist,
            "year": first.year,
            "cover_url": first.cover_url,
            "tracks": [{"id": s.song_id, "name": s.name, "duration": s.duration} for s in songs],
        }
        log.info("album-info (spotdl): %d lagu dalam %.1f detik", len(songs), time.perf_counter() - t0)

    ALBUM_CACHE[url] = (time.time(), result)
    return result


@app.get("/api/album-info")
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