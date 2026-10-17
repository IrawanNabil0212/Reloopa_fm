"""
Logika kuota unduhan.

Aturan:
  free    : 30 lagu SEUMUR HIDUP (tidak pernah reset)
  premium : 60 lagu / minggu, reset Senin 09.00 WIB
  pass    : Unlimited Pass (hanya Premium), aktif 7 hari sejak dibeli,
            60 lagu, reset tiap Senin 09.00 WIB selama Pass aktif
  - Yang dihitung hanya lagu yang BERHASIL diunduh.
  - Lagu yang sama diunduh ulang dalam 24 jam tidak memotong kuota.
  - Album lebih besar dari sisa kuota: hanya sebagian (urut dari lagu pertama) yang diunduh.

Alur: reserve() sebelum unduh -> finalize() kalau berhasil / release() kalau gagal.
"""
from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone

import db

WIB = timezone(timedelta(hours=7))

FREE_LIMIT = int(os.getenv("FREE_LIMIT", "30"))
PREMIUM_WEEKLY_LIMIT = int(os.getenv("PREMIUM_WEEKLY_LIMIT", "60"))
PASS_LIMIT = int(os.getenv("PASS_LIMIT", "60"))
REDOWNLOAD_WINDOW = 24 * 3600
RESERVATION_TTL = 2 * 3600  # pengaman kalau server mati di tengah unduhan


class QuotaExceeded(Exception):
    def __init__(self, status: dict):
        super().__init__("Kuota habis")
        self.status = status


# --------------------------------------------------------------------------- #
# Waktu reset mingguan
# --------------------------------------------------------------------------- #
def week_start(now: float | None = None) -> int:
    """Epoch detik dari Senin 09.00 WIB terakhir (awal jendela kuota minggu ini)."""
    dt = datetime.fromtimestamp(now if now is not None else time.time(), WIB)
    monday = (dt - timedelta(days=dt.weekday())).replace(hour=9, minute=0, second=0, microsecond=0)
    if dt < monday:  # Senin sebelum jam 09.00 masih masuk minggu lalu
        monday -= timedelta(days=7)
    return int(monday.timestamp())


def next_reset(now: float | None = None) -> int:
    return week_start(now) + 7 * 86400


# --------------------------------------------------------------------------- #
# Status kuota
# --------------------------------------------------------------------------- #
def _status(conn, user_id: str, now: int) -> dict:
    row = conn.execute("SELECT premium_until FROM users WHERE id=?", (user_id,)).fetchone()
    premium = bool(row and row["premium_until"] > now)

    pass_row = None
    if premium:
        pass_row = conn.execute(
            "SELECT starts_at, ends_at FROM unlimited_pass "
            "WHERE user_id=? AND starts_at<=? AND ends_at>? ORDER BY ends_at DESC LIMIT 1",
            (user_id, now, now),
        ).fetchone()

    if pass_row:
        since = max(pass_row["starts_at"], week_start(now))
        tier, limit = "pass", PASS_LIMIT
        resets_at = min(next_reset(now), pass_row["ends_at"])
    elif premium:
        since = week_start(now)
        tier, limit, resets_at = "premium", PREMIUM_WEEKLY_LIMIT, since + 7 * 86400
    else:
        tier, limit, since, resets_at = "free", FREE_LIMIT, 0, None

    used = conn.execute(
        "SELECT COUNT(*) FROM download_log WHERE user_id=? AND tier=? AND counted=1 AND created_at>=?",
        (user_id, tier, since),
    ).fetchone()[0]
    reserved = conn.execute(
        "SELECT COALESCE(SUM(count), 0) FROM quota_reservations WHERE user_id=? AND tier=? AND expires_at>?",
        (user_id, tier, now),
    ).fetchone()[0]

    return {
        "tier": tier,
        "is_premium": premium,
        "limit": limit,
        "used": used,
        "remaining": max(0, limit - used - reserved),
        "resets_at": resets_at,  # free: None (tidak reset)
        "pass_active": pass_row is not None,
        "pass_ends_at": pass_row["ends_at"] if pass_row else None,
        "timezone": "WIB",
    }


def get_status(user_id: str) -> dict:
    with db.get_db() as conn:
        return _status(conn, user_id, int(time.time()))


# --------------------------------------------------------------------------- #
# Reservasi -> finalisasi / lepas
# --------------------------------------------------------------------------- #
def _recent_downloads(conn, user_id: str, track_ids: list[str], now: int) -> set[str]:
    """Lagu yang sudah dihitung dalam 24 jam terakhir (unduh ulang gratis)."""
    recent: set[str] = set()
    unique = list(dict.fromkeys(track_ids))
    for i in range(0, len(unique), 500):
        chunk = unique[i:i + 500]
        marks = ",".join("?" * len(chunk))
        rows = conn.execute(
            f"SELECT DISTINCT track_id FROM download_log "
            f"WHERE user_id=? AND counted=1 AND created_at>=? AND track_id IN ({marks})",
            (user_id, now - REDOWNLOAD_WINDOW, *chunk),
        )
        recent.update(r["track_id"] for r in rows)
    return recent


def reserve(user_id: str, session_id: str, track_ids: list[str]) -> dict:
    """
    Tentukan lagu mana yang boleh diunduh dan "pegang" kuotanya.
    Semua dilakukan dalam satu transaksi terkunci, jadi dua tab tidak bisa
    sama-sama lolos dengan sisa kuota yang sama.

    Raise QuotaExceeded kalau tidak ada satu lagu pun yang boleh diunduh.
    """
    now = int(time.time())
    with db.get_db(immediate=True) as conn:
        st = _status(conn, user_id, now)
        recent = _recent_downloads(conn, user_id, track_ids, now)

        budget = st["remaining"]
        selected: list[str] = []
        free_ids: set[str] = set()
        billable = 0
        for tid in track_ids:  # urutan album dipertahankan
            if tid in recent:
                selected.append(tid)
                free_ids.add(tid)
            elif budget > 0:
                selected.append(tid)
                budget -= 1
                billable += 1

        if not selected:
            raise QuotaExceeded(st)

        if billable:
            conn.execute(
                "INSERT OR REPLACE INTO quota_reservations "
                "(session_id, user_id, tier, count, created_at, expires_at) VALUES (?,?,?,?,?,?)",
                (session_id, user_id, st["tier"], billable, now, now + RESERVATION_TTL),
            )

    return {
        "tier": st["tier"],
        "selected_ids": selected,
        "free_ids": sorted(free_ids),
        "billable": billable,
        "total": len(track_ids),
        "partial": len(selected) < len(track_ids),
        "remaining_before": st["remaining"],
    }


def finalize(session_id: str, user_id: str, plan: dict, success_ids: list[str]) -> int:
    """Catat lagu yang berhasil, lepas reservasi. Mengembalikan jumlah yang memotong kuota."""
    now = int(time.time())
    free_ids = set(plan["free_ids"])
    counted = 0
    with db.get_db(immediate=True) as conn:
        for tid in dict.fromkeys(success_ids):
            is_free = tid in free_ids
            conn.execute(
                "INSERT INTO download_log (user_id, track_id, tier, counted, created_at) VALUES (?,?,?,?,?)",
                (user_id, tid, plan["tier"], 0 if is_free else 1, now),
            )
            counted += 0 if is_free else 1
        conn.execute("DELETE FROM quota_reservations WHERE session_id=?", (session_id,))
    return counted


def release(session_id: str) -> None:
    """Unduhan gagal total: kembalikan kuota yang dipegang."""
    with db.get_db() as conn:
        conn.execute("DELETE FROM quota_reservations WHERE session_id=?", (session_id,))


def clear_reservations() -> None:
    """Saat startup: semua job lama hilang, jadi reservasi lama dibuang."""
    with db.get_db() as conn:
        conn.execute("DELETE FROM quota_reservations")