"""Rate limiter sliding-window in-memory (cukup untuk satu proses server)."""
import time
from collections import defaultdict, deque

from fastapi import Request


class RateLimiter:
    def __init__(self, limit: int, window: int):
        self.limit = limit
        self.window = window
        self._hits: dict[str, deque] = defaultdict(deque)

    def hit(self, key: str) -> int:
        """Catat satu permintaan. Mengembalikan 0 kalau boleh, atau detik tunggu kalau kena limit."""
        now = time.monotonic()
        q = self._hits[key]
        while q and now - q[0] > self.window:
            q.popleft()
        if len(q) >= self.limit:
            return int(self.window - (now - q[0])) + 1
        q.append(now)
        return 0


def client_ip(request: Request) -> str:
    # Jalan langsung tanpa reverse proxy. Kalau nanti di belakang proxy (nginx),
    # ambil dari header X-Forwarded-For yang dipercaya.
    return request.client.host if request.client else "unknown"