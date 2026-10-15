"""SQLite untuk user, order pembayaran, Premium, Unlimited Pass, dan log kuota."""
import os
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "app.db"

# Harga ditentukan SERVER (bukan dari frontend)
PREMIUM_PRICE = int(os.getenv("PREMIUM_PRICE", "89000"))  # sekali bayar, selamanya
PASS_PRICE = int(os.getenv("PASS_PRICE", "20000"))        # Unlimited Pass
PASS_DAYS = int(os.getenv("PASS_DAYS", "7"))              # masa berlaku Pass sejak dibeli

# Premium "selamanya" = premium_until diisi 1 Jan 2100 (UTC), jadi is_premium() tetap sama
FOREVER = 4102444800


@contextmanager
def get_db(immediate: bool = False):
    """
    immediate=True -> BEGIN IMMEDIATE: database dikunci untuk penulisan sejak awal,
    dipakai untuk pola 'baca lalu tulis' (cek kuota lalu reservasi) agar tidak balapan.
    """
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        if immediate:
            conn.execute("BEGIN IMMEDIATE")
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db() -> None:
    with get_db() as db:
        db.execute("PRAGMA journal_mode=WAL")
        db.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                id TEXT PRIMARY KEY,                 -- 'sub' dari Google
                email TEXT, name TEXT, picture TEXT,
                premium_until INTEGER NOT NULL DEFAULT 0,   -- epoch detik
                created_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS orders (
                order_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL REFERENCES users(id),
                amount INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',     -- pending | paid | expired | failed
                qr_url TEXT,
                expires_at INTEGER,
                created_at INTEGER NOT NULL,
                paid_at INTEGER
            );

            CREATE TABLE IF NOT EXISTS unlimited_pass (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                starts_at INTEGER NOT NULL,
                ends_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_pass_user ON unlimited_pass(user_id, ends_at);

            -- Satu baris per lagu yang BERHASIL diunduh.
            -- tier   : free | premium | pass (status user saat mengunduh)
            -- counted: 1 = memotong kuota, 0 = unduh ulang gratis (<24 jam)
            CREATE TABLE IF NOT EXISTS download_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                track_id TEXT NOT NULL,
                tier TEXT NOT NULL,
                counted INTEGER NOT NULL DEFAULT 1,
                created_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_log_quota ON download_log(user_id, tier, created_at);
            CREATE INDEX IF NOT EXISTS idx_log_track ON download_log(user_id, track_id, created_at);

            -- Kuota yang "dipegang" selama unduhan berjalan (anti balapan antar tab)
            CREATE TABLE IF NOT EXISTS quota_reservations (
                session_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                tier TEXT NOT NULL,
                count INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_resv_user ON quota_reservations(user_id, tier);
            """
        )
        # Migrasi: kolom 'product' untuk membedakan order Premium vs Pass
        cols = {r["name"] for r in db.execute("PRAGMA table_info(orders)")}
        if "product" not in cols:
            db.execute("ALTER TABLE orders ADD COLUMN product TEXT NOT NULL DEFAULT 'premium'")


def upsert_user(user: dict) -> None:
    with get_db() as db:
        db.execute(
            """INSERT INTO users (id, email, name, picture, created_at)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET
                 email=excluded.email, name=excluded.name, picture=excluded.picture""",
            (user["id"], user.get("email"), user.get("name"), user.get("picture"), int(time.time())),
        )


def is_premium(user_id: str) -> bool:
    with get_db() as db:
        row = db.execute("SELECT premium_until FROM users WHERE id=?", (user_id,)).fetchone()
    return bool(row and row["premium_until"] > time.time())


def has_active_pass(user_id: str) -> bool:
    now = int(time.time())
    with get_db() as db:
        row = db.execute(
            "SELECT 1 FROM unlimited_pass WHERE user_id=? AND starts_at<=? AND ends_at>? LIMIT 1",
            (user_id, now, now),
        ).fetchone()
    return row is not None


def create_order(order_id: str, user_id: str, amount: int, qr_url: str,
                 expires_at: int, product: str = "premium") -> None:
    with get_db() as db:
        db.execute(
            "INSERT INTO orders (order_id, user_id, amount, qr_url, expires_at, created_at, product) "
            "VALUES (?,?,?,?,?,?,?)",
            (order_id, user_id, amount, qr_url, expires_at, int(time.time()), product),
        )


def get_order(order_id: str):
    with get_db() as db:
        return db.execute("SELECT * FROM orders WHERE order_id=?", (order_id,)).fetchone()


def _grant_pass(conn, user_id: str, now: int) -> None:
    """Beri Pass 7 hari. Kalau masih ada Pass aktif, perpanjang 7 hari (uang tidak hangus)."""
    active = conn.execute(
        "SELECT id FROM unlimited_pass WHERE user_id=? AND starts_at<=? AND ends_at>? "
        "ORDER BY ends_at DESC LIMIT 1",
        (user_id, now, now),
    ).fetchone()
    if active:
        conn.execute(
            "UPDATE unlimited_pass SET ends_at = ends_at + ? WHERE id=?",
            (PASS_DAYS * 86400, active["id"]),
        )
    else:
        conn.execute(
            "INSERT INTO unlimited_pass (user_id, starts_at, ends_at) VALUES (?,?,?)",
            (user_id, now, now + PASS_DAYS * 86400),
        )


def update_order_status(order_id: str, status: str) -> None:
    """
    Idempotent: order yang sudah 'paid' tidak diproses lagi.
    BEGIN IMMEDIATE mencegah polling + webhook memberi hak dua kali secara bersamaan.
    """
    with get_db(immediate=True) as db:
        order = db.execute("SELECT * FROM orders WHERE order_id=?", (order_id,)).fetchone()
        if not order or order["status"] == "paid" or order["status"] == status:
            return
        if status == "paid":
            now = int(time.time())
            db.execute("UPDATE orders SET status='paid', paid_at=? WHERE order_id=?", (now, order_id))
            if order["product"] == "pass":
                _grant_pass(db, order["user_id"], now)
            else:
                db.execute("UPDATE users SET premium_until=? WHERE id=?", (FOREVER, order["user_id"]))
        else:
            db.execute("UPDATE orders SET status=? WHERE order_id=?", (status, order_id))