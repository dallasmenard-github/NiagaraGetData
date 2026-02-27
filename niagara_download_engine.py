"""
================================================================================
NIAGARA DOWNLOAD ENGINE v2.0
================================================================================
High-performance download engine with parallel requests, connection pooling,
and persistent state tracking for download resume.

Features:
    - Parallel downloads with configurable workers
    - Connection pooling via requests.Session
    - Adaptive rate limiting
    - Progress tracking
    - Retry logic with exponential backoff
    - JSON state file for download resume

USAGE:
    from niagara_download_engine import DownloadEngine

    engine = DownloadEngine(cookies, max_workers=10)
    stats = engine.download_batch(url_list, output_folder)
================================================================================
"""

import json
import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Callable

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3

from utils import standardize_filename
from logging_config import get_logger

logger = get_logger("engine")

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ============================================================================
# DATA CLASSES
# ============================================================================
@dataclass
class DownloadStats:
    """Statistics for download batch."""
    total: int = 0
    success: int = 0
    failed: int = 0
    empty: int = 0
    skipped: int = 0
    bytes_downloaded: int = 0
    start_time: float = field(default_factory=time.time)
    end_time: float = 0
    errors: List[Tuple[str, str]] = field(default_factory=list)

    @property
    def elapsed(self) -> float:
        end = self.end_time if self.end_time else time.time()
        return end - self.start_time

    @property
    def rate(self) -> float:
        """Downloads per second."""
        if self.elapsed > 0:
            return (self.success + self.failed + self.empty) / self.elapsed
        return 0

    def summary(self) -> str:
        return (
            f"Total: {self.total} | Success: {self.success} | "
            f"Failed: {self.failed} | Empty: {self.empty} | "
            f"Skipped: {self.skipped} | "
            f"Time: {self.elapsed:.1f}s | Rate: {self.rate:.1f}/s"
        )


@dataclass
class DownloadState:
    """Persistent state for download resume."""
    district: str = ""
    date_started: str = ""
    total_points: int = 0
    completed: List[str] = field(default_factory=list)
    failed: List[Dict[str, str]] = field(default_factory=list)
    empty: List[str] = field(default_factory=list)

    def save(self, path: Path) -> None:
        """Save state to JSON file."""
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'w') as f:
            json.dump(asdict(self), f, indent=2)

    @classmethod
    def load(cls, path: Path) -> Optional['DownloadState']:
        """Load state from JSON file."""
        if not path.exists():
            return None
        try:
            with open(path) as f:
                data = json.load(f)
            return cls(**data)
        except (json.JSONDecodeError, TypeError, KeyError):
            logger.warning("Corrupt state file: %s", path)
            return None

    @property
    def completed_set(self) -> set:
        return set(self.completed)


# ============================================================================
# SESSION FACTORY
# ============================================================================
def create_session(
    pool_connections: int = 20,
    pool_maxsize: int = 20,
    max_retries: int = 3,
    backoff_factor: float = 0.3
) -> requests.Session:
    """Create a requests Session with connection pooling and retry logic."""
    session = requests.Session()

    retry_strategy = Retry(
        total=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )

    adapter = HTTPAdapter(
        pool_connections=pool_connections,
        pool_maxsize=pool_maxsize,
        max_retries=retry_strategy
    )

    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.verify = False

    return session


# ============================================================================
# DOWNLOAD ENGINE
# ============================================================================
class DownloadEngine:
    """High-performance download engine for Niagara BAS data."""

    def __init__(
        self,
        cookies: Dict[str, str],
        max_workers: int = 10,
        timeout: int = 30,
        min_content_size: int = 50,
        throttle_delay: float = 0.0,
        progress_callback: Optional[Callable] = None
    ) -> None:
        self.cookies = cookies
        self.max_workers = max_workers
        self.timeout = timeout
        self.min_content_size = min_content_size
        self.throttle_delay = throttle_delay
        self.progress_callback = progress_callback

        self.session = create_session(
            pool_connections=max_workers,
            pool_maxsize=max_workers
        )
        self.session.cookies.update(cookies)

        self._lock = threading.Lock()
        self._consecutive_failures = 0
        self._throttle_multiplier = 1.0

    def _download_single(
        self,
        point_path: str,
        url: str,
        save_folder: str
    ) -> Tuple[str, str, int, Optional[str]]:
        """Download a single point's data."""
        try:
            if self.throttle_delay > 0:
                time.sleep(self.throttle_delay * self._throttle_multiplier)

            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()

            content = response.content
            filename = standardize_filename(point_path) + '.csv'
            filepath = os.path.join(save_folder, filename)

            with open(filepath, 'wb') as f:
                f.write(content)

            with self._lock:
                self._consecutive_failures = 0
                self._throttle_multiplier = max(1.0, self._throttle_multiplier * 0.9)

            if len(content) < self.min_content_size:
                return (point_path, 'empty', len(content), None)

            return (point_path, 'success', len(content), None)

        except requests.exceptions.Timeout:
            self._handle_failure()
            return (point_path, 'failed', 0, 'Timeout')

        except requests.exceptions.HTTPError as e:
            self._handle_failure()
            status = e.response.status_code if e.response is not None else 'unknown'
            return (point_path, 'failed', 0, f'HTTP {status}')

        except requests.exceptions.RequestException as e:
            self._handle_failure()
            return (point_path, 'failed', 0, str(e)[:50])

        except Exception as e:
            self._handle_failure()
            return (point_path, 'failed', 0, str(e)[:50])

    def _handle_failure(self) -> None:
        """Handle download failure with adaptive throttling."""
        with self._lock:
            self._consecutive_failures += 1
            if self._consecutive_failures > 5:
                self._throttle_multiplier = min(5.0, self._throttle_multiplier * 1.5)

    def download_batch(
        self,
        url_list: List[Tuple[str, str]],
        output_folder: str,
        date_subfolder: bool = True
    ) -> DownloadStats:
        """
        Download a batch of URLs in parallel.

        Args:
            url_list: List of (point_path, url) tuples
            output_folder: Base output folder
            date_subfolder: Create YYYY-MM-DD subfolder

        Returns:
            DownloadStats with results
        """
        stats = DownloadStats(total=len(url_list))

        if not url_list:
            return stats

        if date_subfolder:
            save_folder = os.path.join(
                output_folder,
                datetime.now().strftime('%Y-%m-%d')
            )
        else:
            save_folder = output_folder

        os.makedirs(save_folder, exist_ok=True)

        completed = 0
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(
                    self._download_single,
                    point_path,
                    url,
                    save_folder
                ): point_path
                for point_path, url in url_list
            }

            for future in as_completed(futures):
                point_path, status, size, error = future.result()
                completed += 1

                if status == 'success':
                    stats.success += 1
                    stats.bytes_downloaded += size
                elif status == 'empty':
                    stats.empty += 1
                    stats.bytes_downloaded += size
                else:
                    stats.failed += 1
                    if error:
                        stats.errors.append((point_path, error))

                if self.progress_callback:
                    self.progress_callback(completed, stats.total, point_path, status)

        stats.end_time = time.time()
        return stats

    def download_batch_with_resume(
        self,
        url_list: List[Tuple[str, str]],
        output_folder: str,
        district: str = "",
        date_subfolder: bool = True
    ) -> DownloadStats:
        """
        Download with persistent state tracking for resume.

        On interruption, state is saved so subsequent runs skip completed points.

        Args:
            url_list: List of (point_path, url) tuples
            output_folder: Base output folder
            district: District name for state tracking
            date_subfolder: Create YYYY-MM-DD subfolder

        Returns:
            DownloadStats with results
        """
        if date_subfolder:
            save_folder = Path(output_folder) / datetime.now().strftime('%Y-%m-%d')
        else:
            save_folder = Path(output_folder)

        save_folder.mkdir(parents=True, exist_ok=True)
        state_path = save_folder / '.download_state.json'

        # Load or create state
        state = DownloadState.load(state_path)
        if state is None:
            state = DownloadState(
                district=district,
                date_started=datetime.now().isoformat(),
                total_points=len(url_list)
            )

        # Filter already-completed points
        already_done = state.completed_set
        remaining = [(p, u) for p, u in url_list if p not in already_done]
        skipped_by_state = len(url_list) - len(remaining)

        if skipped_by_state > 0:
            logger.info("Resuming: %d already completed, %d remaining", skipped_by_state, len(remaining))

        stats = DownloadStats(total=len(remaining), skipped=skipped_by_state)

        if not remaining:
            return stats

        save_folder.mkdir(parents=True, exist_ok=True)

        completed = 0
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(
                    self._download_single,
                    point_path,
                    url,
                    str(save_folder)
                ): point_path
                for point_path, url in remaining
            }

            for future in as_completed(futures):
                point_path, status, size, error = future.result()
                completed += 1

                if status == 'success':
                    stats.success += 1
                    stats.bytes_downloaded += size
                    state.completed.append(point_path)
                elif status == 'empty':
                    stats.empty += 1
                    stats.bytes_downloaded += size
                    state.empty.append(point_path)
                    state.completed.append(point_path)
                else:
                    stats.failed += 1
                    if error:
                        stats.errors.append((point_path, error))
                    state.failed.append({
                        'point': point_path,
                        'error': error or 'unknown',
                        'time': datetime.now().isoformat()
                    })

                # Save state periodically (every 50 downloads)
                if completed % 50 == 0:
                    state.save(state_path)

                if self.progress_callback:
                    self.progress_callback(completed, stats.total, point_path, status)

        # Final state save
        state.save(state_path)
        stats.end_time = time.time()
        return stats

    def close(self) -> None:
        """Close the session and release resources."""
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# ============================================================================
# PROGRESS PRINTER
# ============================================================================
class ProgressPrinter:
    """In-place console progress bar with live stats."""

    def __init__(self, show_every: int = 1, bar_width: int = 25) -> None:
        self.show_every = show_every
        self.bar_width = bar_width
        self._count = 0
        self._ok = 0
        self._fail = 0
        self._empty = 0
        self._lock = threading.Lock()
        self._start = time.time()

    def __call__(self, current: int, total: int, point_path: str, status: str) -> None:
        import sys
        with self._lock:
            self._count += 1
            if status == 'success':
                self._ok += 1
            elif status == 'empty':
                self._empty += 1
            else:
                self._fail += 1

            if self._count % self.show_every == 0 or current == total:
                pct = current / total if total > 0 else 0
                filled = int(self.bar_width * pct)
                bar = "\u2588" * filled + "\u2591" * (self.bar_width - filled)
                elapsed = time.time() - self._start
                rate = current / elapsed if elapsed > 0 else 0
                fail_str = f" FAIL:{self._fail}" if self._fail else ""
                line = (
                    f"  [{bar}] {current:>{len(str(total))}}/{total}"
                    f"  {pct*100:5.1f}%"
                    f"  OK:{self._ok} EMPTY:{self._empty}{fail_str}"
                    f"  {rate:.1f}/s"
                )
                sys.stdout.write(f"\r{line}")
                sys.stdout.flush()
                if current == total:
                    sys.stdout.write("\n")
                    sys.stdout.flush()


# ============================================================================
# FILTER EXISTING FILES
# ============================================================================
def filter_existing_files(
    url_list: List[Tuple[str, str]],
    output_folder: str,
    force: bool = False
) -> Tuple[List[Tuple[str, str]], int]:
    """
    Filter out points that already have downloaded files.

    Args:
        url_list: List of (point_path, url) tuples
        output_folder: Output folder to check
        force: If True, return all URLs (no filtering)

    Returns:
        Tuple of (filtered_list, skipped_count)
    """
    if force:
        return url_list, 0

    today = datetime.now().strftime('%Y-%m-%d')
    today_folder = os.path.join(output_folder, today)

    if not os.path.exists(today_folder):
        return url_list, 0

    existing = {f for f in os.listdir(today_folder) if f.endswith('.csv')}

    filtered: List[Tuple[str, str]] = []
    skipped = 0

    for point_path, url in url_list:
        expected = standardize_filename(point_path) + '.csv'
        if expected in existing:
            skipped += 1
        else:
            filtered.append((point_path, url))

    return filtered, skipped


# ============================================================================
# CLI / TESTING
# ============================================================================
if __name__ == '__main__':
    print("Niagara Download Engine v2.0")
    print("=" * 40)
    print("This module is designed to be imported.")
    print("See download_niagara_fast.py for CLI usage.")
