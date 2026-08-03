"""Small shared HTTP helper so country scripts don't each hand-roll requests calls."""
import time
import requests

DEFAULT_TIMEOUT = 30
DEFAULT_HEADERS = {
    "User-Agent": "DiliguardDataPipeline/1.0 (+https://github.com/diliguard/diliguard-data-pipeline)"
}


def get_with_retry(url: str, *, timeout: int = DEFAULT_TIMEOUT, max_attempts: int = 3,
                    backoff_seconds: float = 2.0, stream: bool = False, **kwargs) -> requests.Response:
    """GET a URL, retrying on transient 5xx/network errors. Raises on final failure."""
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            headers = {**DEFAULT_HEADERS, **kwargs.pop("headers", {})}
            response = requests.get(url, timeout=timeout, stream=stream, headers=headers, **kwargs)
            if response.status_code >= 500 and attempt < max_attempts:
                time.sleep(backoff_seconds * attempt)
                continue
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            last_exc = exc
            if attempt < max_attempts:
                time.sleep(backoff_seconds * attempt)
    raise last_exc


def download_file(url: str, local_path: str, *, timeout: int = 120, max_attempts: int = 3) -> None:
    """Stream a URL to disk, retrying on transient failures."""
    response = get_with_retry(url, timeout=timeout, max_attempts=max_attempts, stream=True)
    with open(local_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192 * 1024):
            if chunk:
                f.write(chunk)
