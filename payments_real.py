# =====================================================================
# STRIPE & PAYPAL (NYATA) - tempel menggantikan seluruh bagian
# "SIMULASI PayPal / Stripe" di payments.py, yaitu dari baris
#   # ====================== SIMULASI PayPal / Stripe ======================
# sampai akhir fungsi sim_cancel(). Hapus juga pages_router, _SIM_ID,
# SIM_PROVIDERS, SIM_LIFETIME, _sim_guard, _create_sim_payment, checkout_page,
# sim_success, sim_cancel. Lalu ganti fungsi order_status() dengan yang di bawah.
#
# Env baru:
#   STRIPE_SECRET_KEY, STRIPE_WEBHOOK_SECRET, STRIPE_CURRENCY (idr | usd, default idr)
#   PAYPAL_CLIENT_ID, PAYPAL_SECRET, PAYPAL_PRODUCTION (true = live)
#   USD_RATE (default 16000; dipakai PayPal, dan Stripe kalau STRIPE_CURRENCY=usd)
#   APP_BASE_URL (contoh https://domainmu.com; kosong = diambil dari request)
#   ENABLED_METHODS=card,paypal,stripe
# =====================================================================

STRIPE_KEY = os.getenv("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")
STRIPE_CURRENCY = os.getenv("STRIPE_CURRENCY", "idr").lower()   # hanya "idr" atau "usd"
PAYPAL_CLIENT_ID = os.getenv("PAYPAL_CLIENT_ID", "")
PAYPAL_SECRET = os.getenv("PAYPAL_SECRET", "")
PAYPAL_LIVE = os.getenv("PAYPAL_PRODUCTION", "").lower() == "true"
PAYPAL_BASE = "https://api-m.paypal.com" if PAYPAL_LIVE else "https://api-m.sandbox.paypal.com"
USD_RATE = float(os.getenv("USD_RATE", "16000"))
APP_BASE_URL = os.getenv("APP_BASE_URL", "").rstrip("/")

PAYPAL_LIFETIME = 3 * 60 * 60          # order PayPal berlaku sekitar 3 jam
PROVIDER_CODE = {"stripe": "STR", "paypal": "PPL"}
ITEM_NAME = {"premium": "Premium (sekali bayar)", "pass": "Unlimited Pass 7 hari"}
_EXT_ID = re.compile(r"^[A-Z]{3}-(STR|PPL)[0-9a-f]{20}$")
_STRIPE_SESSION = re.compile(r"^cs_[A-Za-z0-9_]+$")
_PAYPAL_TOKEN = re.compile(r"^[A-Za-z0-9]{10,40}$")


def _base_url(request: Request) -> str:
    return APP_BASE_URL or str(request.base_url).rstrip("/")


def _new_ext_order_id(product: str, provider: str) -> str:
    return f"{PRODUCTS[product]['prefix']}-{PROVIDER_CODE[provider]}{uuid.uuid4().hex[:20]}"


def _provider_of(order_id: str) -> str | None:
    m = _EXT_ID.match(order_id or "")
    return {"STR": "stripe", "PPL": "paypal"}[m.group(1)] if m else None


def _usd(amount_idr: int) -> str:
    return f"{amount_idr / USD_RATE:.2f}"


def _stripe_minor(amount_idr: int) -> int:
    """Stripe memakai satuan terkecil. IDR dihitung x100, USD = sen."""
    if STRIPE_CURRENCY == "idr":
        return amount_idr * 100
    return round(amount_idr / USD_RATE * 100)


def _settle_external(order_id: str, order: dict, source: str) -> None:
    """Satu-satunya tempat order Stripe/PayPal ditandai lunas (setelah provider mengonfirmasi)."""
    if order["status"] == "paid":
        return
    db.update_order_status(order_id, "paid")
    uid = order["user_id"]
    _dbg(f"[{source}] {order_id}: {order['status']} -> paid | produk={order['product']} | "
         f"is_premium={db.is_premium(uid)} has_active_pass={db.has_active_pass(uid)}")


# ------------------------------- STRIPE -------------------------------
def _stripe_headers() -> dict:
    return {"Authorization": f"Bearer {STRIPE_KEY}"}


def _stripe_create_session(form: dict) -> requests.Response:
    return requests.post("https://api.stripe.com/v1/checkout/sessions",
                         data=form, headers=_stripe_headers(), timeout=20)


def _stripe_get_session(session_id: str) -> requests.Response:
    return requests.get(f"https://api.stripe.com/v1/checkout/sessions/{session_id}",
                        headers=_stripe_headers(), timeout=20)


def _stripe_confirm(order_id: str, order: dict, session: dict) -> bool:
    """Lunas hanya jika Stripe bilang paid DAN order, mata uang, jumlah cocok dengan data server."""
    if session.get("client_reference_id") != order_id:
        return False
    if session.get("payment_status") != "paid":
        return False
    expected = _stripe_minor(_to_int(order["amount"]) or 0)
    if session.get("currency") != STRIPE_CURRENCY or _to_int(session.get("amount_total")) != expected:
        _dbg(f"[stripe] {order_id}: JUMLAH/MATA UANG TIDAK COCOK "
             f"stripe={session.get('amount_total')} {session.get('currency')} server={expected} {STRIPE_CURRENCY}")
        return False
    _settle_external(order_id, order, "stripe")
    return True


@router.post("/stripe")
async def create_stripe(request: Request, product: str = "premium"):
    _require_enabled("stripe")
    if not STRIPE_KEY:
        raise HTTPException(status_code=500, detail="STRIPE_SECRET_KEY belum diatur di server.")
    user = _user(request)
    _check_can_buy(user, product)

    amount = int(PRODUCTS[product]["price"])           # harga selalu dari server
    order_id = _new_ext_order_id(product, "stripe")
    base = _base_url(request)
    form = {
        "mode": "payment",
        "success_url": f"{base}/pembayaran/proses?order_id={order_id}&provider=stripe&session_id={{CHECKOUT_SESSION_ID}}",
        "cancel_url": f"{base}/premium",
        "client_reference_id": order_id,
        "metadata[order_id]": order_id,
        "line_items[0][quantity]": "1",
        "line_items[0][price_data][currency]": STRIPE_CURRENCY,
        "line_items[0][price_data][unit_amount]": str(_stripe_minor(amount)),
        "line_items[0][price_data][product_data][name]": ITEM_NAME[product],
    }
    if user.get("email"):
        form["customer_email"] = user["email"]

    _dbg(f"STRIPE create session order_id={order_id} amount={amount} {STRIPE_CURRENCY}")
    try:
        resp = await asyncio.to_thread(_stripe_create_session, form)
    except requests.RequestException as e:
        _dbg("STRIPE gagal koneksi:", repr(e))
        raise HTTPException(status_code=502, detail="Gagal menghubungi Stripe. Coba lagi nanti.")
    data = resp.json() if resp.content else {}
    if resp.status_code != 200 or not data.get("url"):
        msg = (data.get("error") or {}).get("message", "tidak diketahui")
        log.warning("Stripe menolak: %s", data)
        raise HTTPException(status_code=502, detail=f"Stripe menolak permintaan: {msg}")

    expires_at = _to_int(data.get("expires_at")) or int(time.time()) + 1800
    db.create_order(order_id, user["id"], amount, data["url"], expires_at, product)
    await _cancel_other_pending(user["id"], product, keep_order_id=order_id)
    return {"order_id": order_id, "product": product, "amount": amount, "method": "stripe",
            "redirect_url": data["url"], "expires_at": expires_at, "sandbox": STRIPE_KEY.startswith("sk_test")}


def _stripe_sig_ok(payload: bytes, header: str) -> bool:
    if not STRIPE_WEBHOOK_SECRET:
        return False
    items = [p.split("=", 1) for p in header.split(",") if "=" in p]
    ts = next((v for k, v in items if k == "t"), None)
    sigs = [v for k, v in items if k == "v1"]
    if not ts or not sigs:
        return False
    try:
        if abs(time.time() - int(ts)) > 300:      # tolak webhook basi (anti replay)
            return False
    except ValueError:
        return False
    expected = hmac.new(STRIPE_WEBHOOK_SECRET.encode(), f"{ts}.".encode() + payload, hashlib.sha256).hexdigest()
    return any(hmac.compare_digest(expected, s) for s in sigs)


@router.post("/stripe/webhook")
async def stripe_webhook(request: Request):
    """Daftarkan di Stripe Dashboard > Developers > Webhooks:
    https://domainmu.com/api/payments/stripe/webhook
    event: checkout.session.completed, checkout.session.async_payment_succeeded"""
    payload = await request.body()
    if not _stripe_sig_ok(payload, request.headers.get("stripe-signature", "")):
        _dbg("STRIPE WEBHOOK: signature tidak valid")
        raise HTTPException(status_code=400, detail="Signature tidak valid.")
    try:
        event = json.loads(payload)
    except ValueError:
        raise HTTPException(status_code=400, detail="Payload tidak valid.")
    if event.get("type") in ("checkout.session.completed", "checkout.session.async_payment_succeeded"):
        session = (event.get("data") or {}).get("object") or {}
        oid = str(session.get("client_reference_id", ""))
        order = db.get_order(oid) if _provider_of(oid) == "stripe" else None
        if order:
            _stripe_confirm(oid, order, session)
    return {"ok": True}


# ------------------------------- PAYPAL -------------------------------
def _paypal_token() -> str:
    r = requests.post(f"{PAYPAL_BASE}/v1/oauth2/token", data={"grant_type": "client_credentials"},
                      auth=(PAYPAL_CLIENT_ID, PAYPAL_SECRET), headers={"Accept": "application/json"}, timeout=20)
    r.raise_for_status()
    return r.json()["access_token"]


def _paypal_call(method: str, path: str, token: str, request_id: str | None = None, **kw) -> requests.Response:
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    if request_id:
        headers["PayPal-Request-Id"] = request_id      # idempotensi: request ulang tidak membuat order ganda
    return requests.request(method, PAYPAL_BASE + path, headers=headers, timeout=20, **kw)


def _paypal_create(body: dict, order_id: str) -> tuple[int, dict]:
    t = _paypal_token()
    r = _paypal_call("POST", "/v2/checkout/orders", t, request_id=order_id, json=body)
    return r.status_code, (r.json() if r.content else {})


def _paypal_capture_flow(order_id: str, pp_id: str, expected_usd: str) -> str:
    """Kembalikan 'paid' | 'pending' | 'failed'. Uang baru ditarik saat capture dari server ini."""
    t = _paypal_token()
    g = _paypal_call("GET", f"/v2/checkout/orders/{pp_id}", t)
    if g.status_code != 200:
        return "failed"
    o = g.json()
    unit = (o.get("purchase_units") or [{}])[0]
    amt = unit.get("amount") or {}
    if unit.get("custom_id") != order_id or amt.get("currency_code") != "USD" or amt.get("value") != expected_usd:
        _dbg(f"[paypal] {order_id}: order PayPal tidak cocok dengan order kita -> DITOLAK")
        return "failed"

    status = o.get("status")
    if status == "APPROVED":
        c = _paypal_call("POST", f"/v2/checkout/orders/{pp_id}/capture", t,
                         request_id=f"capture-{order_id}", json={})
        if c.status_code not in (200, 201):
            return "failed"
        o = c.json()
        unit = (o.get("purchase_units") or [{}])[0]
    elif status != "COMPLETED":
        return "pending"       # user belum menyetujui pembayaran di PayPal

    caps = (unit.get("payments") or {}).get("captures") or []
    if any(cap.get("status") == "PENDING" for cap in caps):
        return "pending"
    ok = o.get("status") == "COMPLETED" and any(
        cap.get("status") == "COMPLETED"
        and (cap.get("amount") or {}).get("currency_code") == "USD"
        and (cap.get("amount") or {}).get("value") == expected_usd
        for cap in caps)
    return "paid" if ok else "failed"


@router.post("/paypal")
async def create_paypal(request: Request, product: str = "premium"):
    _require_enabled("paypal")
    if not (PAYPAL_CLIENT_ID and PAYPAL_SECRET):
        raise HTTPException(status_code=500, detail="PAYPAL_CLIENT_ID / PAYPAL_SECRET belum diatur di server.")
    user = _user(request)
    _check_can_buy(user, product)

    amount = int(PRODUCTS[product]["price"])
    order_id = _new_ext_order_id(product, "paypal")
    base = _base_url(request)
    body = {
        "intent": "CAPTURE",
        "purchase_units": [{
            "reference_id": order_id,
            "custom_id": order_id,
            "description": ITEM_NAME[product],
            "amount": {"currency_code": "USD", "value": _usd(amount)},
        }],
        "payment_source": {"paypal": {"experience_context": {
            "return_url": f"{base}/pembayaran/proses?order_id={order_id}&provider=paypal",
            "cancel_url": f"{base}/premium",
            "user_action": "PAY_NOW",
            "shipping_preference": "NO_SHIPPING",
        }}},
    }
    _dbg(f"PAYPAL create order order_id={order_id} amount={amount} IDR = {_usd(amount)} USD")
    try:
        status, data = await asyncio.to_thread(_paypal_create, body, order_id)
    except requests.RequestException as e:
        _dbg("PAYPAL gagal koneksi:", repr(e))
        raise HTTPException(status_code=502, detail="Gagal menghubungi PayPal. Coba lagi nanti.")
    link = next((l.get("href") for l in data.get("links", []) if l.get("rel") in ("payer-action", "approve")), None)
    if status not in (200, 201) or not link:
        log.warning("PayPal menolak: %s", data)
        raise HTTPException(status_code=502, detail="PayPal menolak permintaan. Coba lagi nanti.")

    expires_at = int(time.time()) + PAYPAL_LIFETIME
    db.create_order(order_id, user["id"], amount, link, expires_at, product)
    await _cancel_other_pending(user["id"], product, keep_order_id=order_id)
    return {"order_id": order_id, "product": product, "amount": amount, "method": "paypal",
            "redirect_url": link, "expires_at": expires_at, "sandbox": not PAYPAL_LIVE}


@router.post("/paypal/capture")
async def paypal_capture(order_id: str, token: str, request: Request):
    """Dipanggil halaman /pembayaran/proses setelah user kembali dari PayPal (token = ID order PayPal)."""
    user = _user(request)
    if _provider_of(order_id) != "paypal" or not _PAYPAL_TOKEN.match(token):
        raise HTTPException(status_code=400, detail="Parameter tidak valid.")
    order = db.get_order(order_id)
    if not order or order["user_id"] != user["id"]:
        raise HTTPException(status_code=404, detail="Order tidak ditemukan.")
    if order["status"] == "paid":
        return {"order_id": order_id, "product": order["product"], "status": "paid"}

    expected_usd = _usd(_to_int(order["amount"]) or 0)
    try:
        result = await asyncio.to_thread(_paypal_capture_flow, order_id, token, expected_usd)
    except requests.RequestException as e:
        _dbg("PAYPAL capture gagal koneksi:", repr(e))
        raise HTTPException(status_code=502, detail="Gagal menghubungi PayPal. Coba lagi nanti.")

    if result == "paid":
        _settle_external(order_id, order, "paypal")
    elif result == "failed" and order["status"] == "pending":
        db.update_order_status(order_id, "failed")
    return {"order_id": order_id, "product": order["product"], "status": db.get_order(order_id)["status"]}


# ----------- GANTI fungsi order_status() lama dengan versi ini -----------
@router.get("/status/{order_id}")
async def order_status(order_id: str, request: Request, session_id: str = ""):
    user = _user(request)
    order = db.get_order(order_id)
    if not order or order["user_id"] != user["id"]:
        raise HTTPException(status_code=404, detail="Order tidak ditemukan.")

    provider = _provider_of(order_id)
    _dbg(f"POLL {order_id} | provider={provider or 'midtrans'} | status lokal={order['status']!r}")

    if order["status"] != "paid":
        try:
            if provider == "stripe":
                if _STRIPE_SESSION.match(session_id):
                    resp = await asyncio.to_thread(_stripe_get_session, session_id)
                    if resp.status_code == 200:
                        _stripe_confirm(order_id, order, resp.json())
            elif provider == "paypal":
                pass        # PayPal dikonfirmasi lewat POST /paypal/capture
            elif order["status"] == "pending":
                data = await asyncio.to_thread(_call, "GET", f"/v2/{order_id}/status")
                _dbg(f"POLL {order_id} -> midtrans={data.get('transaction_status')} ({data.get('status_message')})")
                _apply_status(order_id, order, data, source="poll")
        except requests.RequestException as e:
            _dbg(f"POLL {order_id} gagal koneksi:", repr(e))

    fresh = db.get_order(order_id)
    return {"order_id": order_id, "product": fresh["product"], "status": fresh["status"]}