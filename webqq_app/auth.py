from .common import *

class BanTracker:
    def __init__(self, max_failures=5, window_seconds=300, ban_seconds=1800):
        self.max_failures = max(1, int(max_failures or 5))
        self.window_seconds = max(1, int(window_seconds or 300))
        self.ban_seconds = max(1, int(ban_seconds or 1800))
        self._failures = defaultdict(deque)
        self._banned_until = {}

    def is_banned(self, ip, now=None):
        if not ip:
            return False
        now = now or time.time()
        banned_until = self._banned_until.get(ip, 0)
        if banned_until > now:
            return True
        self._banned_until.pop(ip, None)
        return False

    def record_failure(self, ip, now=None):
        if not ip:
            return False
        now = now or time.time()
        failures = self._failures[ip]
        cutoff = now - self.window_seconds
        while failures and failures[0] < cutoff:
            failures.popleft()
        failures.append(now)
        if len(failures) >= self.max_failures:
            self._banned_until[ip] = now + self.ban_seconds
            failures.clear()
            print(f"[security] banned {ip} for {self.ban_seconds}s after failed auth attempts")
            return True
        return False

    def clear(self, ip):
        if not ip:
            return
        self._failures.pop(ip, None)
        self._banned_until.pop(ip, None)


def client_ip(request):
    for header in ("CF-Connecting-IP", "X-Real-IP"):
        value = request.headers.get(header, "").strip()
        if value:
            return value
    forwarded = request.headers.get("X-Forwarded-For", "")
    if forwarded:
        return forwarded.split(",", 1)[0].strip()
    return request.remote or ""


def record_auth_failure(request):
    tracker = request.app.get("ban_tracker")
    if tracker:
        tracker.record_failure(client_ip(request))


@web.middleware
async def ban_middleware(request, handler):
    tracker = request.app.get("ban_tracker")
    if tracker and tracker.is_banned(client_ip(request)):
        return web.json_response({"error": "too many failed auth attempts"}, status=429)
    return await handler(request)


def check_auth(request):
    cfg = request.app["config"]
    auth_token = cfg.get("web_token", "")
    if not auth_token:
        return True
    req_token = request.query.get("token") or request.cookies.get("token") or ""
    ok = hmac.compare_digest(req_token, auth_token)
    if not ok and req_token:
        record_auth_failure(request)
    return ok


async def read_json_body(request):
    try:
        body = await request.json()
    except Exception:
        raise web.HTTPBadRequest(text='{"error":"invalid JSON"}', content_type="application/json")
    if not isinstance(body, dict):
        raise web.HTTPBadRequest(text='{"error":"JSON body must be an object"}', content_type="application/json")
    return body
