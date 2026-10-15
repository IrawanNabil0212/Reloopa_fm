"""Pembayaran QRIS lewat Midtrans Core API (mode sandbox secara default).

Produk:
  premium : Rp89.000, sekali bayar, selamanya
  pass    : Unlimited Pass Rp20.000, 7 hari, HANYA untuk user Premium

Env: MIDTRANS_SERVER_KEY   (wajib, dari dashboard Midtrans > Settings > Access Keys)
     MIDTRANS_PRODUCTION   (opsional, isi "true" hanya saat sudah live)
"""
import asyncio
import base64
import hashlib
import hmac
import logging
import os
import time
import uuid

import requests
from fastapi import APIRouter, HTTPException, Request

import db

log = logging.getLogger("spotify-zipper")
router = APIRouter(prefix="/api/payments")

SERVER_KEY = os.getenv("MIDTRANS_SERVER_KEY", "")
PRODUCTION = os.getenv("MIDTRANS_PRODUCTION", "").lower() == "true"
BASE_URL = "https://api.midtrans.com" if PRODUCTION else "https://api.sandbox.midtrans.com"
QR_LIFETIME = 15 * 60  # detik, perkiraan masa berlaku QR; status asli tetap dari Midtrans

PRODUCTS = {
    "premium": {"prefix": "PRM", "price": db.PREMIUM_PRICE},
    "pass": {"prefix": "PAS", "price": db.PASS_PRICE},
}

# status Midtrans -> status order kita
STATUS_MAP = {"settlement": "paid", "capture": "paid", "pending": "pending",
              "expire": "expired", "cancel": "failed", "deny": "failed", "failure": "failed"}


def _auth_header() -> dict:
    token = base64.b64encode(f"{SERVER_KEY}:".encode()).decode()
    return {"Authorization": f"Basic {token}", "Accept": "application/json", "Content-Type": "application/json"}


def _call(method: str, path: str, **kwargs) -> dict:
    r = requests.request(method, BASE_URL + path, headers=_auth_header(), timeout=20, **kwargs)
    return r.json()


def _user(request: Request) -> dict:
    user = request.session.get("user")
    if not user:
        raise HTTPException(status_code=401, detail="Belum login.")
    return user


@router.get("/products")
async def products():
    """Info harga untuk ditampilkan di halaman premium (satu sumber kebenaran: server)."""
    return {
        "premium": {"price": db.PREMIUM_PRICE, "lifetime": True},
        "pass": {"price": db.PASS_PRICE, "days": db.PASS_DAYS, "requires_premium": True},
    }


@router.post("/qris")
async def create_qris(request: Request, product: str = "premium"):
    """POST /api/payments/qris             -> beli Premium (kompatibel dengan frontend lama)
       POST /api/payments/qris?product=pass -> beli Unlimited Pass"""
    user = _user(request)
    if product not in PRODUCTS:
        raise HTTPException(status_code=400, detail="Produk tidak dikenal.")
    if not SERVER_KEY:
        raise HTTPException(status_code=500, detail="MIDTRANS_SERVER_KEY belum diatur di server.")

    db.upsert_user({**user})
    premium = db.is_premium(user["id"])
    if product == "premium" and premium:
        raise HTTPException(status_code=409, detail="Akun kamu sudah Premium.")
    if product == "pass":
        if not premium:
            raise HTTPException(status_code=403, detail="Unlimited Pass hanya untuk user Premium.")
        if db.has_active_pass(user["id"]):
            raise HTTPException(status_code=409, detail="Unlimited Pass kamu masih aktif.")

    info = PRODUCTS[product]
    amount = info["price"]
    order_id = f"{info['prefix']}-{uuid.uuid4().hex[:20]}"
    payload = {
        "payment_type": "qris",
        "qris": {"acquirer": "gopay"},
        "transaction_details": {"order_id": order_id, "gross_amount": amount},
        "customer_details": {"email": user.get("email") or ""},
    }
    try:
        data = await asyncio.to_thread(_call, "POST", "/v2/charge", json=payload)
    except requests.RequestException:
        raise HTTPException(status_code=502, detail="Gagal menghubungi Midtrans. Coba lagi nanti.")

    qr_url = next((a["url"] for a in data.get("actions", []) if a.get("name") == "generate-qr-code"), None)
    if not qr_url:
        log.warning("Charge QRIS gagal: %s", data)
        raise HTTPException(status_code=502, detail=f"Midtrans menolak permintaan: {data.get('status_message', 'tidak diketahui')}")

    expires_at = int(time.time()) + QR_LIFETIME
    db.create_order(order_id, user["id"], amount, qr_url, expires_at, product)
    return {"order_id": order_id, "product": product, "amount": amount, "qr_url": qr_url,
            "expires_at": expires_at, "sandbox": not PRODUCTION}


@router.get("/status/{order_id}")
async def order_status(order_id: str, request: Request):
    user = _user(request)
    order = db.get_order(order_id)
    if not order or order["user_id"] != user["id"]:
        raise HTTPException(status_code=404, detail="Order tidak ditemukan.")

    # Tanya Midtrans langsung, jadi tetap jalan di localhost walau webhook belum bisa masuk
    if order["status"] == "pending":
        try:
            data = await asyncio.to_thread(_call, "GET", f"/v2/{order_id}/status")
            new = STATUS_MAP.get(data.get("transaction_status", ""))
            if new and str(data.get("gross_amount", "")).split(".")[0] == str(order["amount"]):
                db.update_order_status(order_id, new)
        except requests.RequestException:
            pass
    fresh = db.get_order(order_id)
    return {"order_id": order_id, "product": fresh["product"], "status": fresh["status"]}


@router.post("/notification")
async def notification(request: Request):
    """Webhook Midtrans (daftarkan URL ini di dashboard: Settings > Payment > Notification URL)."""
    body = await request.json()
    order_id = str(body.get("order_id", ""))
    expected = hashlib.sha512(
        f"{order_id}{body.get('status_code', '')}{body.get('gross_amount', '')}{SERVER_KEY}".encode()
    ).hexdigest()
    if not SERVER_KEY or not hmac.compare_digest(expected, str(body.get("signature_key", ""))):
        raise HTTPException(status_code=403, detail="Signature tidak valid.")

    order = db.get_order(order_id)
    if not order:
        return {"ok": True}
    new = STATUS_MAP.get(body.get("transaction_status", ""))
    if new and str(body.get("gross_amount", "")).split(".")[0] == str(order["amount"]):
        db.update_order_status(order_id, new)
    return {"ok": True}