"""Small shared HTTP helper so country scripts don't each hand-roll requests calls."""
import os
import time
import requests

DEFAULT_TIMEOUT = 30
DEFAULT_HEADERS = {
    "User-Agent": "DiliguardDataPipeline/1.0 (+https://github.com/diliguard/diliguard-data-pipeline)"
}


def get_with_retry(url: str, *, timeout: int = DEFAULT_TIMEOUT, max_attempts: int = 5,
                    backoff_seconds: float = 2.0, stream: bool = False, **kwargs) -> requests.Response:
    """GET a URL, retrying on transient 5xx/network errors with exponential backoff. Raises on final failure."""
    last_exc = None
    custom_headers = kwargs.pop("headers", None) or {}
    for attempt in range(1, max_attempts + 1):
        try:
            headers = {**DEFAULT_HEADERS, **custom_headers}
            response = requests.get(url, timeout=timeout, stream=stream, headers=headers, **kwargs)
            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                sleep_time = backoff_seconds * (2 ** (attempt - 1))
                print(f"HTTP {response.status_code} for {url} — retrying in {sleep_time:.1f}s (attempt {attempt}/{max_attempts})...", flush=True)
                time.sleep(sleep_time)
                continue
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            last_exc = exc
            if attempt < max_attempts:
                sleep_time = backoff_seconds * (2 ** (attempt - 1))
                print(f"Request error ({exc}) for {url} — retrying in {sleep_time:.1f}s (attempt {attempt}/{max_attempts})...", flush=True)
                time.sleep(sleep_time)
    raise last_exc


def download_file(url: str, local_path: str, *, timeout: int = 180, max_attempts: int = 5,
                   progress_every_bytes: int = 25 * 1024 * 1024) -> None:
    """Stream a URL to disk, retrying full download on stream/network drop.

    Logs progress periodically so a large download doesn't look 'stuck'.
    """
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            response = get_with_retry(url, timeout=timeout, max_attempts=1, stream=True)
            total = response.headers.get("Content-Length")
            total_mb = f"{int(total) / 1024 / 1024:.1f}MB" if total else "unknown size"
            print(f"Starting download ({total_mb})...", flush=True)

            downloaded = 0
            next_report_at = progress_every_bytes
            with open(local_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192 * 1024):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if downloaded >= next_report_at:
                            print(f"...downloaded {downloaded / 1024 / 1024:.1f}MB / {total_mb}", flush=True)
                            next_report_at += progress_every_bytes
            print(f"Download complete: {downloaded / 1024 / 1024:.1f}MB", flush=True)
            return
        except Exception as exc:
            last_exc = exc
            if os.path.exists(local_path):
                try:
                    os.remove(local_path)
                except OSError:
                    pass
            if attempt < max_attempts:
                sleep_time = 3.0 * (2 ** (attempt - 1))
                print(f"Download stream error ({exc}) — retrying in {sleep_time:.1f}s (attempt {attempt}/{max_attempts})...", flush=True)
                time.sleep(sleep_time)
    raise last_exc
