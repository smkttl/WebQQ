import argparse
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Optional, Sequence
from urllib.parse import urlsplit, urlunsplit


DEFAULT_SERVER_URL = "http://localhost:8080"


def normalize_server_url(value: str) -> str:
    value = (value or "").strip()
    if not value:
        value = DEFAULT_SERVER_URL
    if "://" not in value:
        value = "http://" + value
    parsed = urlsplit(value)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ValueError("server URL must use http:// or https://")
    if parsed.query or parsed.fragment:
        raise ValueError("server URL must not include a query or fragment")
    path = parsed.path.rstrip("/")
    return urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))


@dataclass(frozen=True)
class TuiConfig:
    server_url: str = DEFAULT_SERVER_URL
    token: Optional[str] = None
    download_dir: Path = Path.cwd()

    @classmethod
    def from_args(
        cls,
        argv: Optional[Sequence[str]] = None,
        environ: Optional[Mapping[str, str]] = None,
    ) -> "TuiConfig":
        parser = argparse.ArgumentParser(description="Connect to a WebQQ server in the terminal")
        parser.add_argument("--url", help="WebQQ server URL (env: WEBQQ_URL)")
        parser.add_argument("--token", help="Web login token (env: WEBQQ_TOKEN)")
        parser.add_argument(
            "--download-dir",
            type=Path,
            help="Directory for downloaded attachments (default: current directory)",
        )
        args = parser.parse_args(argv)
        env = os.environ if environ is None else environ
        server_url = normalize_server_url(args.url or env.get("WEBQQ_URL", DEFAULT_SERVER_URL))
        token = args.token if args.token is not None else env.get("WEBQQ_TOKEN")
        download_dir = (args.download_dir or Path.cwd()).expanduser().resolve()
        return cls(server_url=server_url, token=token, download_dir=download_dir)
