import base64
import binascii
import json
from pathlib import Path
from urllib.parse import unquote, urlparse

import aiohttp

from .common import MAX_FILE_UPLOAD, PublicAddressResolver, file_url_allowed


QZONE_UPLOAD_URL = "https://up.qzone.qq.com/cgi-bin/upload/cgi_upload_image"
QZONE_PUBLISH_URL = "https://user.qzone.qq.com/proxy/domain/taotao.qzone.qq.com/cgi-bin/emotion_cgi_publish_v6"
QZONE_DELETE_URL = "https://user.qzone.qq.com/proxy/domain/taotao.qzone.qq.com/cgi-bin/emotion_cgi_delete_v6"


def qzone_gtk(skey):
    value = 5381
    for char in str(skey):
        value = value + (value << 5) + ord(char)
    return str(value & 0x7FFFFFFF)


def parse_cookie_string(raw):
    cookies = {}
    for part in str(raw or "").split(";"):
        key, separator, value = part.strip().partition("=")
        if separator and key:
            cookies[key] = value
    return cookies


def qzone_auth_from_cookies(raw):
    cookies = parse_cookie_string(raw)
    raw_uin = cookies.get("uin") or cookies.get("p_uin") or ""
    uin = raw_uin[1:] if raw_uin.startswith("o") else raw_uin
    skey = cookies.get("skey") or ""
    pskey = cookies.get("p_skey") or ""
    if not uin or not uin.isdigit() or not skey or not pskey:
        raise RuntimeError("Qzone credentials are unavailable from NapCat")
    cookie = "p_uin=o{}; p_skey={}; skey={}; uin=o{}".format(uin, pskey, skey, uin)
    return {"uin": uin, "skey": skey, "pskey": pskey, "g_tk": qzone_gtk(skey), "cookie": cookie}


def qzone_upload_form(auth, image_base64):
    uin = auth["uin"]
    g_tk = auth["g_tk"]
    back_urls = (
        "http://upbak.photo.qzone.qq.com/cgi-bin/upload/cgi_upload_image,"
        "http://119.147.64.75/cgi-bin/upload/cgi_upload_image"
        "&url=https://up.qzone.qq.com/cgi-bin/upload/cgi_upload_image?g_tk={}".format(g_tk)
    )
    return {
        "filename": "filename", "uin": uin, "skey": auth["skey"], "zzpaneluin": uin,
        "p_uin": uin, "p_skey": auth["pskey"], "uploadtype": "1", "albumtype": "7",
        "exttype": "0", "refer": "shuoshuo", "output_type": "jsonhtml", "charset": "utf-8",
        "output_charset": "utf-8", "upload_hd": "1", "hd_width": "2048", "hd_height": "10000",
        "hd_quality": "96", "backUrls": back_urls, "base64": "1", "jsonhtml_callback": "callback",
        "picfile": image_base64, "qzreferrer": "https://user.qzone.qq.com/{}/main".format(uin),
    }


def parse_qzone_upload(raw):
    start_tag = "frameElement.callback"
    end_tag = "</script>"
    start = str(raw).find(start_tag)
    end = str(raw).find(end_tag, start + len(start_tag))
    if start < 0 or end < 0:
        raise RuntimeError("Qzone image upload returned an invalid response")
    wrapped = str(raw)[start + len(start_tag):end]
    json_start = wrapped.find("(")
    json_end = wrapped.rfind(")")
    if json_start < 0 or json_end <= json_start:
        raise RuntimeError("Qzone image upload response could not be parsed")
    try:
        result = json.loads(wrapped[json_start + 1:json_end])
    except (TypeError, ValueError):
        raise RuntimeError("Qzone image upload returned invalid JSON")
    if not isinstance(result, dict):
        raise RuntimeError("Qzone image upload returned invalid data")
    code = result.get("code")
    if code is not None and code != 0:
        raise RuntimeError(str(result.get("msg") or "Qzone image upload failed (code={})".format(code)))
    data = result.get("data")
    if not isinstance(data, dict):
        raise RuntimeError("Qzone image upload response is missing data")
    required = [str(data.get(key) or "") for key in ("albumid", "lloc", "type", "height", "width")]
    if not all(required):
        raise RuntimeError("Qzone image upload response is incomplete")
    album_id, lloc, image_type, height, width = required
    return ",{0},{1},{1},{2},{3},{4},,{3},{4}".format(album_id, lloc, image_type, height, width)


def qzone_publish_form(auth, content, richvals, ugc_right, target_uins):
    form = {
        "syn_tweet_verson": "1", "paramstr": "1", "con": str(content), "feedversion": "1",
        "ver": "1", "ugc_right": str(ugc_right), "to_sign": "0", "hostuin": auth["uin"],
        "code_version": "1", "format": "json",
        "qzreferrer": "https://user.qzone.qq.com/{}/main".format(auth["uin"]),
    }
    if richvals:
        form.update({"richtype": "1", "richval": "\t".join(richvals)})
    if target_uins:
        form.update({"allow_uins": "|".join(str(uid) for uid in target_uins), "who": "1"})
    return form


def qzone_delete_form(auth, tid):
    return {
        "hostuin": auth["uin"], "tid": str(tid), "t1_source": "1", "code_version": "1",
        "format": "json", "qzreferrer": "https://user.qzone.qq.com/{}".format(auth["uin"]),
    }


async def _qzone_auth(request_action):
    response = await request_action("get_cookies", {"domain": "qzone.qq.com"}, timeout=30)
    if not response or response.get("status") != "ok":
        detail = ""
        if isinstance(response, dict):
            detail = response.get("wording") or response.get("message") or ""
        raise RuntimeError(str(detail or "NapCat could not provide Qzone credentials"))
    data = response.get("data")
    if not isinstance(data, dict):
        raise RuntimeError("NapCat returned invalid Qzone credentials")
    return qzone_auth_from_cookies(data.get("cookies"))


def _qzone_session():
    resolver = PublicAddressResolver()
    connector = aiohttp.TCPConnector(resolver=resolver)
    timeout = aiohttp.ClientTimeout(total=180, connect=15, sock_read=120)
    return aiohttp.ClientSession(connector=connector, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})


async def _response_text(session, url, form, auth):
    headers = {"Cookie": auth["cookie"], "Content-Type": "application/x-www-form-urlencoded"}
    async with session.post(url + "?g_tk=" + auth["g_tk"], data=form, headers=headers) as response:
        text = await response.text()
        if response.status >= 400:
            raise RuntimeError("Qzone request failed with HTTP {}".format(response.status))
        return text


async def _response_json(session, url, form, auth):
    text = await _response_text(session, url, form, auth)
    try:
        result = json.loads(text.lstrip("\ufeff").strip())
    except (TypeError, ValueError):
        raise RuntimeError("Qzone returned invalid JSON")
    if not isinstance(result, dict):
        raise RuntimeError("Qzone returned invalid data")
    return result


async def _read_image_source(session, source, remaining):
    value = str(source or "").strip()
    if remaining <= 0:
        raise ValueError("Qzone images exceed the cumulative 100 MB limit")
    if value.startswith("base64://"):
        encoded = value[len("base64://"):]
        if len(encoded) > ((remaining + 2) // 3) * 4 + 4:
            raise ValueError("Qzone images exceed the cumulative 100 MB limit")
        try:
            data = base64.b64decode(encoded, validate=True)
        except (ValueError, binascii.Error):
            raise ValueError("invalid base64 image")
    elif value.startswith("file://"):
        parsed = urlparse(value)
        if parsed.netloc not in ("", "localhost"):
            raise ValueError("invalid file image URI")
        path = Path(unquote(parsed.path))
        try:
            if path.stat().st_size > remaining:
                raise ValueError("Qzone images exceed the cumulative 100 MB limit")
            data = path.read_bytes()
        except OSError as error:
            raise ValueError("Qzone image file is unavailable: {}".format(error))
    elif value.startswith(("http://", "https://")):
        if not file_url_allowed(value):
            raise ValueError("Qzone image URL must use a public host")
        chunks = []
        size = 0
        async with session.get(value, allow_redirects=True) as response:
            if response.status != 200:
                raise RuntimeError("Qzone image download failed with HTTP {}".format(response.status))
            async for chunk in response.content.iter_chunked(1024 * 1024):
                size += len(chunk)
                if size > remaining:
                    raise ValueError("Qzone images exceed the cumulative 100 MB limit")
                chunks.append(chunk)
        data = b"".join(chunks)
    else:
        raise ValueError("Qzone images must use file://, http(s)://, or base64://")
    if not data:
        raise ValueError("Qzone image is empty")
    if len(data) > remaining:
        raise ValueError("Qzone images exceed the cumulative 100 MB limit")
    return data


def _qzone_api_error(result, action):
    code = result.get("subcode", result.get("code"))
    if code is not None and code != 0:
        raise RuntimeError(str(result.get("message") or result.get("msg") or "Qzone {} failed (subcode={})".format(action, code)))


async def publish_qzone_post(request_action, content, images, ugc_right, target_uins):
    auth = await _qzone_auth(request_action)
    richvals = []
    total = 0
    async with _qzone_session() as session:
        for source in images or []:
            image = await _read_image_source(session, source, MAX_FILE_UPLOAD - total)
            total += len(image)
            encoded = base64.b64encode(image).decode("ascii")
            upload = await _response_text(session, QZONE_UPLOAD_URL, qzone_upload_form(auth, encoded), auth)
            richvals.append(parse_qzone_upload(upload))
        result = await _response_json(
            session, QZONE_PUBLISH_URL,
            qzone_publish_form(auth, content, richvals, ugc_right, target_uins), auth,
        )
    _qzone_api_error(result, "publish")
    tid = result.get("t1_tid") or result.get("tid")
    if not tid:
        raise RuntimeError("Qzone publish response did not include tid")
    return {"tid": str(tid)}


async def delete_qzone_post(request_action, tid):
    auth = await _qzone_auth(request_action)
    async with _qzone_session() as session:
        result = await _response_json(session, QZONE_DELETE_URL, qzone_delete_form(auth, tid), auth)
    _qzone_api_error(result, "deletion")
