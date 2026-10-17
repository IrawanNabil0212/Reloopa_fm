import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import time
import uuid
from datetime import datetime, timedelta, timezone

import requests
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

import db
import re
from fastapi.responses import HTMLResponse

log = logging.getLogger("spotify-zipper")
router = APIRouter(prefix="/api/payments")

SERVER_KEY = os.getenv("MIDTRANS_SERVER_KEY", "")
CLIENT_KEY = os.getenv("MIDTRANS_CLIENT_KEY", "")
PRODUCTION = os.getenv("MIDTRANS_PRODUCTION", "").lower() == "true"
BASE_URL = "https://api.midtrans.com" if PRODUCTION else "https://api.sandbox.midtrans.com"
QR_LIFETIME = 15 * 60
CARD_LIFETIME = 15 * 60

# Contoh .env: ENABLED_METHODS=card,paypal,stripe   (kosong = semua mati)
ENABLED_METHODS = {m.strip() for m in os.getenv("ENABLED_METHODS", "").split(",") if m.strip()}


def _require_enabled(name: str) -> None:
    """Penjaga di server: toggle di JS saja bisa dilewati lewat DevTools/curl."""
    if name not in ENABLED_METHODS:
        raise HTTPException(status_code=503, detail="Metode pembayaran ini belum tersedia.")


class CardIn(BaseModel):
    token_id: str


PRODUCTS = {
    "premium": {"prefix": "PRM", "price": db.PREMIUM_PRICE},
    "pass": {"prefix": "PAS", "price": db.PASS_PRICE},
}
# status Midtrans -> status order kita
STATUS_MAP = {"settlement": "paid", "capture": "paid", "pending": "pending",
              "expire": "expired", "cancel": "failed", "deny": "failed", "failure": "failed"}

# Penjaga order ganda (in-memory; cukup untuk satu proses uvicorn)
_create_locks: dict = {}
_recent_orders: dict = {}   # key: (user_id, product, method) -> hasil order terakhir

# Transfer bank: BCA/BNI/BRI = Virtual Account (bank_transfer), Mandiri = Bill Payment (echannel)
BANKS = {"bca": "bank_transfer", "bni": "bank_transfer", "bri": "bank_transfer", "mandiri": "echannel"}
VA_LIFETIME = 24 * 60 * 60  # cadangan kalau Midtrans tidak mengirim expiry_time
WIB = timezone(timedelta(hours=7))  # Midtrans memakai waktu Asia/Jakarta
PRODUCT_LABEL = {"premium": "Premium", "pass": "Unl. Pass"}


def _dbg(*args):
    """print() agar langsung terlihat di terminal uvicorn (flush=True)."""
    print("[MIDTRANS]", *args, flush=True)


def _auth_header() -> dict:
    token = base64.b64encode(f"{SERVER_KEY}:".encode()).decode()
    return {"Authorization": f"Basic {token}", "Accept": "application/json", "Content-Type": "application/json"}


def _call(method: str, path: str, **kwargs) -> dict:
    r = requests.request(method, BASE_URL + path, headers=_auth_header(), timeout=20, **kwargs)
    try:
        data = r.json()
    except ValueError:
        data = {"status_code": str(r.status_code), "status_message": r.text[:300]}
    # Midtrans sering membalas HTTP 200 dengan "status_code" 404/4xx di dalam body, jadi catat keduanya
    _dbg(f"{method} {path} -> HTTP {r.status_code} | body status_code={data.get('status_code')}")
    return data


def _to_int(value) -> int | None:
    """'89000.00' / '89000' / 89000 / 89000.0 -> 89000. None kalau tidak bisa diparse."""
    try:
        return int(float(str(value)))
    except (TypeError, ValueError):
        return None


def _user(request: Request) -> dict:
    user = request.session.get("user")
    if not user:
        raise HTTPException(status_code=401, detail="Belum login.")
    return user


def _apply_status(order_id: str, order: dict, data: dict, source: str) -> None:
    """Satu-satunya tempat status order diubah (dipakai polling DAN webhook)."""
    midtrans_status = data.get("transaction_status", "")
    if midtrans_status == "capture" and data.get("fraud_status") == "challenge":
        midtrans_status = "pending"   # belum lunas sampai di-approve di dashboard
    new = STATUS_MAP.get(midtrans_status)
    if not new:
        _dbg(f"[{source}] {order_id}: transaction_status={midtrans_status!r} tidak dikenal "
             f"(status_code={data.get('status_code')}, msg={data.get('status_message')!r}) -> DIABAIKAN")
        return

    paid_amount = _to_int(data.get("gross_amount"))
    our_amount = _to_int(order["amount"])
    if paid_amount != our_amount:
        _dbg(f"[{source}] {order_id}: AMOUNT TIDAK COCOK midtrans={data.get('gross_amount')!r} "
             f"db={order['amount']!r} -> DIABAIKAN")
        return

    if order["status"] == "paid":
        return  # jangan pernah menurunkan order yang sudah dibayar
    if order["status"] == new:
        return

    db.update_order_status(order_id, new)
    _dbg(f"[{source}] {order_id}: {order['status']} -> {new} (midtrans={midtrans_status})")

    if new == "paid":
        uid = order["user_id"]
        # Cek apakah upgrade benar-benar terjadi. Kalau keduanya False untuk produk yang baru dibayar,
        # berarti db.update_order_status() belum memberi hak akses -> lihat db.py.
        _dbg(f"[{source}] {order_id}: produk={order['product']} | setelah paid: "
             f"is_premium={db.is_premium(uid)} has_active_pass={db.has_active_pass(uid)}")


def _expiry_epoch(data: dict, fallback_seconds: int) -> int:
    """Ubah expiry_time Midtrans ('2026-10-16 16:08:58', WIB) menjadi epoch; cadangan: sekarang + fallback."""
    raw = data.get("expiry_time")
    if raw:
        try:
            return int(datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=WIB).timestamp())
        except ValueError:
            pass
    return int(time.time()) + fallback_seconds


def _payment_info(data: dict) -> dict:
    """Ambil info yang perlu ditampilkan ke user dari respons charge/status Midtrans."""
    info: dict = {}
    va = (data.get("va_numbers") or [None])[0]
    if va:  # BCA / BNI / BRI
        info.update(bank=va.get("bank"), va_number=va.get("va_number"))
    if data.get("bill_key"):  # Mandiri Bill Payment
        info.update(bank="mandiri", biller_code=data.get("biller_code"), bill_key=data.get("bill_key"))
    return info


def _check_can_buy(user: dict, product: str) -> None:
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


async def _cancel_other_pending(user_id, product: str, keep_order_id: str) -> None:
    """Batalkan order pending lain (metode lain) untuk produk yang sama supaya tidak bisa terbayar ganda."""
    for key, cached in list(_recent_orders.items()):
        uid, prod, _method = key
        if uid != user_id or prod != product or cached["order_id"] == keep_order_id:
            continue
        old_id = cached["order_id"]
        old = db.get_order(old_id)
        if old and old["status"] == "pending":
            try:
                res = await asyncio.to_thread(_call, "POST", f"/v2/{old_id}/cancel")
            except requests.RequestException as e:
                _dbg(f"CANCEL {old_id} gagal koneksi:", repr(e))
                continue
            if str(res.get("status_code")) == "200":
                db.update_order_status(old_id, "failed")
                _dbg(f"CANCEL {old_id}: order lama dibatalkan karena user memilih metode lain")
            else:
                _dbg(f"CANCEL {old_id}: ditolak Midtrans ({res.get('status_message')}), dibiarkan")
                continue
        _recent_orders.pop(key, None)


async def _create_payment(request: Request, product: str, method: str) -> dict:
    """method = 'qris' atau kode bank ('bca', 'bni', 'bri', 'mandiri')."""
    user = _user(request)
    _check_can_buy(user, product)

    # Cegah order ganda: pakai ulang order yang sama (metode sama) selama masih pending dan belum hampir habis.
    key = (user["id"], product, method)
    async with _create_locks.setdefault(key, asyncio.Lock()):
        cached = _recent_orders.get(key)
        if cached and cached["expires_at"] - time.time() > 60:
            existing = db.get_order(cached["order_id"])
            if existing and existing["status"] == "pending":
                _dbg(f"REUSE order {cached['order_id']} (request ganda dicegah)")
                return {**cached, "sandbox": not PRODUCTION}

        info = PRODUCTS[product]
        amount = int(info["price"])  # harga SELALU dari server, tidak pernah dari browser
        order_id = f"{info['prefix']}-{uuid.uuid4().hex[:20]}"
        details = {"order_id": order_id, "gross_amount": amount}

        if method == "qris":
            payload = {"payment_type": "qris", "qris": {"acquirer": "gopay"}, "transaction_details": details}
        elif BANKS[method] == "echannel":
            payload = {"payment_type": "echannel", "transaction_details": details,
                       "echannel": {"bill_info1": "Payment:", "bill_info2": PRODUCT_LABEL[product]}}
        else:
            payload = {"payment_type": "bank_transfer", "transaction_details": details,
                       "bank_transfer": {"bank": method}}
        if user.get("email"):  # email kosong ("") bisa ditolak Midtrans
            payload["customer_details"] = {"email": user["email"]}

        _dbg(f"CHARGE {method} order_id={order_id} amount={amount} env={'PRODUCTION' if PRODUCTION else 'SANDBOX'}")
        try:
            data = await asyncio.to_thread(_call, "POST", "/v2/charge", json=payload)
        except requests.RequestException as e:
            _dbg("CHARGE gagal koneksi:", repr(e))
            raise HTTPException(status_code=502, detail="Gagal menghubungi Midtrans. Coba lagi nanti.")
        _dbg("CHARGE response:", json.dumps(data, ensure_ascii=False))

        ok = str(data.get("status_code")) in ("200", "201")
        extra: dict = {}
        qr_url = ""
        if ok and method == "qris":
            actions = {a.get("name"): a.get("url") for a in data.get("actions", [])}
            # Simulator sandbox menerima URL "generate-qr-code-v2" (terbukti saat uji)
            qr_url = actions.get("generate-qr-code-v2") or actions.get("generate-qr-code") or ""
            ok = bool(qr_url)
            extra = {"qr_url": qr_url}
        elif ok:
            extra = _payment_info(data)
            ok = bool(extra.get("va_number") or extra.get("bill_key"))
        if not ok:
            log.warning("Charge %s gagal: %s", method, data)
            raise HTTPException(status_code=502, detail=f"Midtrans menolak permintaan: {data.get('status_message', 'tidak diketahui')}")

        fallback = QR_LIFETIME if method == "qris" else VA_LIFETIME
        expires_at = _expiry_epoch(data, fallback)
        db.create_order(order_id, user["id"], amount, qr_url, expires_at, product)
        result = {"order_id": order_id, "product": product, "amount": amount,
                  "method": method, "expires_at": expires_at, **extra}
        _recent_orders[key] = result

    await _cancel_other_pending(user["id"], product, keep_order_id=order_id)
    return {**result, "sandbox": not PRODUCTION}


@router.post("/card")
async def create_card(request: Request, body: CardIn, product: str = "premium"):
    """Kartu kredit/debit via Midtrans Core API.
    token_id didapat dari Midtrans.js di browser (dengan 3DS aktif).
    Hasil: 'paid' langsung (capture), atau 'pending' + redirect_url untuk 3DS."""
    _require_enabled("card")
    user = _user(request)
    _check_can_buy(user, product)

    info = PRODUCTS[product]
    amount = int(info["price"])          # harga selalu dari server
    order_id = f"{info['prefix']}-{uuid.uuid4().hex[:20]}"
    payload = {
        "payment_type": "credit_card",
        "transaction_details": {"order_id": order_id, "gross_amount": amount},
        "credit_card": {"token_id": body.token_id, "authentication": True},
    }
    if user.get("email"):
        payload["customer_details"] = {"email": user["email"]}

    _dbg(f"CHARGE card order_id={order_id} amount={amount}")
    try:
        data = await asyncio.to_thread(_call, "POST", "/v2/charge", json=payload)
    except requests.RequestException as e:
        _dbg("CHARGE card gagal koneksi:", repr(e))
        raise HTTPException(status_code=502, detail="Gagal menghubungi Midtrans. Coba lagi nanti.")

    if str(data.get("status_code")) not in ("200", "201"):
        log.warning("Charge card gagal: %s", data)
        raise HTTPException(status_code=402, detail=f"Kartu ditolak: {data.get('status_message', 'tidak diketahui')}")

    expires_at = _expiry_epoch(data, CARD_LIFETIME)
    redirect_url = data.get("redirect_url", "")      # ada kalau butuh 3DS
    db.create_order(order_id, user["id"], amount, redirect_url, expires_at, product)
    _apply_status(order_id, db.get_order(order_id), data, source="card")   # kalau capture langsung lunas

    # 1) Batalkan dulu order pending lain (QRIS/VA/kartu lama) untuk produk yang sama
    await _cancel_other_pending(user["id"], product, keep_order_id=order_id)

    # 2) Baru catat order kartu ini kalau masih pending (menunggu 3DS)
    fresh = db.get_order(order_id)
    if fresh["status"] == "pending":
        _recent_orders[(user["id"], product, "card")] = {
            "order_id": order_id,
            "expires_at": expires_at,
        }

    return {"order_id": order_id, "product": product, "amount": amount, "method": "card",
            "status": fresh["status"], "redirect_url": redirect_url, "sandbox": not PRODUCTION}


# ====================== SIMULASI PayPal / Stripe ======================
SIM_PROVIDERS = {"paypal": "PayPal", "stripe": "Stripe"}
SIM_LIFETIME = 15 * 60
_SIM_ID = re.compile(r"^[A-Z]{3}-SIM[0-9a-f]{16}$")


def _sim_guard(provider: str) -> None:
    if provider not in SIM_PROVIDERS:
        raise HTTPException(status_code=404, detail="Metode tidak dikenal.")
    _require_enabled(provider)
    if PRODUCTION:  # simulasi TIDAK BOLEH hidup di production
        raise HTTPException(status_code=501, detail=f"{SIM_PROVIDERS[provider]} belum terintegrasi.")


async def _create_sim_payment(request: Request, provider: str, product: str) -> dict:
    _sim_guard(provider)
    user = _user(request)
    _check_can_buy(user, product)

    info = PRODUCTS[product]
    amount = int(info["price"])
    order_id = f"{info['prefix']}-SIM{uuid.uuid4().hex[:16]}"
    expires_at = int(time.time()) + SIM_LIFETIME
    redirect_url = f"/payments/{provider}/checkout?order_id={order_id}"

    db.create_order(order_id, user["id"], amount, redirect_url, expires_at, product)
    await _cancel_other_pending(user["id"], product, keep_order_id=order_id)
    _dbg(f"SIM {provider} order_id={order_id} amount={amount}")
    return {"order_id": order_id, "product": product, "amount": amount, "method": provider,
            "redirect_url": redirect_url, "expires_at": expires_at, "dummy": True, "sandbox": True}


@router.post("/paypal")
async def create_paypal(request: Request, product: str = "premium"):
    # TODO produksi: PayPal Orders v2 (intent=CAPTURE) + webhook PAYMENT.CAPTURE.COMPLETED.
    # PayPal tidak mendukung IDR -> konversi ke USD.
    return await _create_sim_payment(request, "paypal", product)


@router.post("/stripe")
async def create_stripe(request: Request, product: str = "premium"):
    # TODO produksi: stripe.checkout.Session.create(currency="idr", metadata={"order_id": ...})
    # + webhook checkout.session.completed (verifikasi Stripe-Signature).
    return await _create_sim_payment(request, "stripe", product)


def _load_sim_order(request: Request, provider: str, order_id: str) -> dict:
    _sim_guard(provider)
    user = _user(request)
    if not _SIM_ID.match(order_id):          # hanya order simulasi, bukan order Midtrans asli
        raise HTTPException(status_code=400, detail="Order bukan order simulasi.")
    order = db.get_order(order_id)
    if not order or order["user_id"] != user["id"]:
        raise HTTPException(status_code=404, detail="Order tidak ditemukan.")
    return order


pages_router = APIRouter()   # tanpa prefix /api

@pages_router.get("/payments/{provider}/checkout", response_class=HTMLResponse)
async def checkout_page(provider: str, order_id: str, request: Request):
    """Halaman checkout palsu. Status HANYA berubah lewat POST /api/payments/{provider}/success."""
    order = _load_sim_order(request, provider, order_id)
    name = SIM_PROVIDERS[provider]
    product = PRODUCT_LABEL.get(order["product"], order["product"])
    amt = f"{int(order['amount']):,}".replace(",", ".")
    cfg = json.dumps({"provider": provider, "order_id": order_id})
    color = "#0070ba" if provider == "paypal" else "#635bff"
    return f"""<!DOCTYPE html><html lang="id"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Checkout {name} (Simulasi)</title>
<style>
body{{font-family:system-ui,sans-serif;background:#f3f4f7;margin:0;display:grid;place-items:center;min-height:100dvh}}
.card{{background:#fff;border-radius:18px;box-shadow:0 8px 30px rgba(20,20,40,.12);padding:28px;width:min(420px,92vw);text-align:center}}
h2{{margin:0 0 4px;color:{color}}} .badge{{display:inline-block;background:#fff3cd;color:#8a6d00;font-size:12px;font-weight:700;padding:3px 10px;border-radius:999px}}
.row{{display:flex;justify-content:space-between;padding:12px 0;border-bottom:1px solid #eee;font-size:15px}}
.total{{font-weight:800;font-size:20px;border:0}}
button{{font:inherit;font-weight:700;width:100%;padding:13px;margin-top:12px;border:0;border-radius:12px;cursor:pointer}}
.ok{{background:{color};color:#fff}} .no{{background:#e0e2e6}} button:disabled{{opacity:.6;cursor:wait}}
#msg{{color:#d92d20;font-size:13px;min-height:18px;margin-top:10px}}
</style></head><body><div class="card">
<h2>{name}</h2><span class="badge">SIMULASI – tidak ada uang dipindahkan</span>
<div style="margin-top:18px">
<div class="row"><span>Produk</span><strong>{product}</strong></div>
<div class="row"><span>Order</span><span style="font-size:12px">{order_id}</span></div>
<div class="row total"><span>Total tagihan</span><span>Rp {amt}</span></div></div>
<button class="ok" id="pay">Simulasikan Bayar Berhasil</button>
<button class="no" id="cancel">Batalkan</button>
<div id="msg"></div></div>
<script>
const c={cfg}, msg=document.getElementById('msg');
async function go(path, done) {{
  document.querySelectorAll('button').forEach(b=>b.disabled=true);
  try {{
    const r=await fetch('/api/payments/'+c.provider+'/'+path+'?order_id='+encodeURIComponent(c.order_id),{{method:'POST'}});
    if(r.ok) {{ location.href=done; return; }}
    const d=await r.json().catch(()=>({{}})); msg.textContent=d.detail||'Gagal.';
  }} catch {{ msg.textContent='Koneksi bermasalah.'; }}
  document.querySelectorAll('button').forEach(b=>b.disabled=false);
}}
document.getElementById('pay').onclick=()=>go('success','/?paid=1&order_id='+encodeURIComponent(c.order_id));
document.getElementById('cancel').onclick=()=>go('cancel','/premium');
</script></body></html>"""

@router.post("/{provider}/success")
async def sim_success(provider: str, order_id: str, request: Request):
    """Callback simulasi: menandai order lunas -> memberi Premium / Pass."""
    order = _load_sim_order(request, provider, order_id)
    if order["status"] == "paid":
        return {"order_id": order_id, "product": order["product"], "status": "paid"}
    if order["status"] != "pending":
        raise HTTPException(status_code=409, detail="Order sudah tidak aktif.")

    exp = dict(order).get("expires_at")
    if exp and time.time() > float(exp):
        db.update_order_status(order_id, "expired")
        raise HTTPException(status_code=410, detail="Order kedaluwarsa.")

    uid = order["user_id"]
    # Cegah bayar ganda kalau user sudah lunas lewat metode lain
    if (order["product"] == "premium" and db.is_premium(uid)) or \
       (order["product"] == "pass" and db.has_active_pass(uid)):
        db.update_order_status(order_id, "failed")
        raise HTTPException(status_code=409, detail="Paket ini sudah aktif di akun kamu.")

    db.update_order_status(order_id, "paid")
    _dbg(f"[sim-{provider}] {order_id}: pending -> paid | produk={order['product']} | "
         f"is_premium={db.is_premium(uid)} has_active_pass={db.has_active_pass(uid)}")
    return {"order_id": order_id, "product": order["product"], "status": "paid"}


@router.post("/{provider}/cancel")
async def sim_cancel(provider: str, order_id: str, request: Request):
    order = _load_sim_order(request, provider, order_id)
    if order["status"] == "pending":
        db.update_order_status(order_id, "failed")
    return {"order_id": order_id, "status": "failed"}

@router.get("/products")
async def products():
    """Info harga untuk ditampilkan di halaman premium (satu sumber kebenaran: server)."""
    return {
        "premium": {"price": db.PREMIUM_PRICE, "lifetime": True},
        "pass": {"price": db.PASS_PRICE, "days": db.PASS_DAYS, "requires_premium": True},
    }

@router.get("/config")
async def payment_config(request: Request):
    _user(request)
    _require_enabled("card")
    if not CLIENT_KEY:
        raise HTTPException(status_code=500, detail="MIDTRANS_CLIENT_KEY belum diatur di server.")
    return {"client_key": CLIENT_KEY, "sandbox": not PRODUCTION}


@router.post("/qris")
async def create_qris(request: Request, product: str = "premium"):
    """POST /api/payments/qris             -> beli Premium (kompatibel dengan frontend lama)
       POST /api/payments/qris?product=pass -> beli Unlimited Pass"""
    return await _create_payment(request, product, "qris")


@router.post("/bank-transfer")
async def create_bank_transfer(request: Request, bank: str, product: str = "premium"):
    """POST /api/payments/bank-transfer?bank=bca&product=premium
       bank: bca | bni | bri (Virtual Account) | mandiri (Bill Payment: biller_code + bill_key)"""
    if bank not in BANKS:
        raise HTTPException(status_code=400, detail="Bank tidak dikenal.")
    return await _create_payment(request, product, bank)


@router.get("/instructions/{order_id}")
async def order_instructions(order_id: str, request: Request):
    """Info bayar (nomor VA / kode Mandiri) untuk order yang sudah dibuat, supaya halaman bisa dimuat ulang."""
    user = _user(request)
    order = db.get_order(order_id)
    if not order or order["user_id"] != user["id"]:
        raise HTTPException(status_code=404, detail="Order tidak ditemukan.")
    try:
        data = await asyncio.to_thread(_call, "GET", f"/v2/{order_id}/status")
    except requests.RequestException:
        raise HTTPException(status_code=502, detail="Gagal menghubungi Midtrans. Coba lagi nanti.")
    return {"order_id": order_id, "product": order["product"], "amount": order["amount"],
            "status": order["status"], "sandbox": not PRODUCTION,
            "expires_at": _expiry_epoch(data, 0) if data.get("expiry_time") else None,
            **_payment_info(data)}


@router.get("/status/{order_id}")
async def order_status(order_id: str, request: Request):
    user = _user(request)
    order = db.get_order(order_id)
    if not order or order["user_id"] != user["id"]:
        raise HTTPException(status_code=404, detail="Order tidak ditemukan.")

    _dbg(f"POLL {order_id} | status lokal={order['status']!r} amount_db={order['amount']!r}")

    # Tanya Midtrans langsung, jadi tetap jalan di localhost walau webhook belum bisa masuk
    if order["status"] == "pending":
        try:
            data = await asyncio.to_thread(_call, "GET", f"/v2/{order_id}/status")
            _dbg(f"POLL {order_id} -> midtrans={data.get('transaction_status')} ({data.get('status_message')})")
            _apply_status(order_id, order, data, source="poll")
        except requests.RequestException as e:
            _dbg(f"POLL {order_id} gagal koneksi ke Midtrans:", repr(e))
    else:
        _dbg(f"POLL {order_id}: status lokal bukan 'pending' -> Midtrans TIDAK ditanya")

    fresh = db.get_order(order_id)
    return {"order_id": order_id, "product": fresh["product"], "status": fresh["status"]}


@router.post("/notification")
async def notification(request: Request):
    """Webhook Midtrans (daftarkan URL ini di dashboard: Settings > Payment > Notification URL)."""
    body = await request.json()
    _dbg("WEBHOOK RAW:", json.dumps(body, ensure_ascii=False))
    order_id = str(body.get("order_id", ""))
    expected = hashlib.sha512(
        f"{order_id}{body.get('status_code', '')}{body.get('gross_amount', '')}{SERVER_KEY}".encode()
    ).hexdigest()
    if not SERVER_KEY or not hmac.compare_digest(expected, str(body.get("signature_key", ""))):
        _dbg(f"WEBHOOK {order_id}: signature tidak valid")
        raise HTTPException(status_code=403, detail="Signature tidak valid.")

    order = db.get_order(order_id)
    if not order:
        return {"ok": True}
    _apply_status(order_id, order, body, source="webhook")
    return {"ok": True}