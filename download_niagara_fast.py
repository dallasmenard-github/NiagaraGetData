"""
================================================================================
NIAGARA FAST DOWNLOAD v2.0
================================================================================
High-performance trend data downloader with parallel requests.

USAGE:
    python download_niagara_fast.py
    python download_niagara_fast.py --district WINDHAMSCHOOLSNH --days 30
    python download_niagara_fast.py --all-districts
================================================================================
"""

import argparse
import os
import sys
import time
from datetime import datetime
from typing import List, Optional, Tuple

from utils import safe_print, print_header, setup_console_encoding, APP_VERSION
from logging_config import get_logger

setup_console_encoding()

logger = get_logger("download_fast")

from config_district_details import district_config
from niagara_download_engine import (
    DownloadEngine, DownloadStats, ProgressPrinter, filter_existing_files
)
from niagara_url_generator import URLGenerator, get_available_districts
from niagara_auth import NiagaraAuth

try:
    from fetch_pointlist import fetch_pointlist_selenium
    FETCH_POINTLIST_AVAILABLE: bool = True
except ImportError:
    FETCH_POINTLIST_AVAILABLE = False

DEFAULT_DAYS: int = 90
DEFAULT_WORKERS: int = 10
DEFAULT_THROTTLE: float = 0.0
DEFAULT_TOGGLE_INTERVAL: int = 100


def list_districts() -> None:
    """Display all available districts with credential and point list status."""
    print_header("AVAILABLE DISTRICTS")
    districts: List[str] = get_available_districts()
    for i, district in enumerate(districts, 1):
        config = district_config.get(district, {})
        base_ip: str = config.get('BASE_IP', 'N/A')
        from niagara_auth import get_credentials
        user, passwd = get_credentials(district)
        cred_status: str = "Y" if (user and passwd) else "N"
        from niagara_url_generator import get_point_list_path
        path, source = get_point_list_path(district)
        pl_status: str = "Y" if path else "N"
        safe_print(f"{i:2d}. [{cred_status}][{pl_status}] {district:25s} | {base_ip[:40]}")
    safe_print(f"{'='*70}")
    safe_print(f"Total: {len(districts)} districts")
    safe_print(f"[Creds][PointList] Y=yes N=no")
    logger.info("Listed %d districts", len(districts))


def select_district_interactive() -> Optional[List[str]]:
    """Prompt the user to select a district interactively.

    Returns:
        List of selected district names, or None if cancelled.
    """
    list_districts()
    districts: List[str] = get_available_districts()
    while True:
        try:
            choice: str = input("\nSelect district number (or 'q' to quit): ").strip()
            if choice.lower() == 'q':
                return None
            if choice.lower() == 'a':
                return districts
            choice_num: int = int(choice)
            if 1 <= choice_num <= len(districts):
                return [districts[choice_num - 1]]
            safe_print(f"Invalid. Enter 1-{len(districts)}")
        except ValueError:
            choice_upper: str = choice.upper()
            if choice_upper in districts:
                return [choice_upper]
            safe_print(f"District '{choice}' not found")
        except KeyboardInterrupt:
            safe_print("\nCancelled.")
            return None


def process_district(
    district_name: str,
    days: int = DEFAULT_DAYS,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    workers: int = DEFAULT_WORKERS,
    throttle: float = DEFAULT_THROTTLE,
    output_dir: Optional[str] = None,
    force: bool = False,
    cookie: Optional[str] = None,
    headless: bool = False,
    toggle_interval: int = DEFAULT_TOGGLE_INTERVAL,
    auto_fetch: bool = False
) -> Optional[DownloadStats]:
    """Process a single district: authenticate, generate URLs, and download.

    Args:
        district_name: Name of the district to process.
        days: Number of days of history to download.
        start_date: Optional start date (YYYY-MM-DD).
        end_date: Optional end date (YYYY-MM-DD).
        workers: Number of parallel download workers.
        throttle: Delay in seconds between requests.
        output_dir: Override output directory.
        force: If True, re-download existing files.
        cookie: Optional pre-existing session cookie.
        headless: Run browser authentication in headless mode.
        toggle_interval: Interval for session toggle refresh.
        auto_fetch: Automatically fetch point list if missing.

    Returns:
        DownloadStats on success, or None on failure.
    """
    print_header(f"PROCESSING: {district_name}")
    logger.info("Starting processing for district: %s", district_name)

    try:
        url_gen: URLGenerator = URLGenerator(district_name)
    except ValueError as e:
        safe_print(f"ERROR: {e}")
        logger.error("Failed to create URLGenerator for %s: %s", district_name, e)
        return None

    info: dict = url_gen.info()
    safe_print(f"Base IP:     {info['base_ip']}")
    safe_print(f"Point List:  {info['point_list_path']} ({info['point_list_source']})")
    safe_print(f"Points:      {info['point_count']}")

    if not url_gen.has_point_list:
        if auto_fetch and FETCH_POINTLIST_AVAILABLE:
            safe_print("\nAttempting to auto-fetch point list...")
            logger.info("Auto-fetching point list for %s", district_name)
            success: bool = fetch_pointlist_selenium(district_name, headless=False)
            if success:
                safe_print("[OK] Point list fetched!")
                logger.info("Point list fetched successfully for %s", district_name)
                try:
                    url_gen = URLGenerator(district_name)
                    info = url_gen.info()
                    safe_print(f"Points:      {info['point_count']}")
                except ValueError as e:
                    safe_print(f"ERROR: {e}")
                    logger.error("Failed to reload URLGenerator after fetch for %s: %s", district_name, e)
                    return None
            else:
                safe_print("ERROR: Auto-fetch failed")
                logger.error("Auto-fetch of point list failed for %s", district_name)
                return None
        else:
            safe_print(f"\nERROR: No point list found for {district_name}")
            logger.error("No point list found for %s", district_name)
            if FETCH_POINTLIST_AVAILABLE:
                safe_print(f"Use --auto-fetch flag to fetch automatically")
            else:
                safe_print(f"Run: python fetch_pointlist.py --district {district_name}")
            return None

    if output_dir:
        output_folder: str = os.path.join(output_dir, district_name)
    else:
        output_folder = info['output_folder']
    safe_print(f"Output:      {output_folder}")

    safe_print("\nGenerating URLs...")
    try:
        if start_date and end_date:
            url_list: List[str] = url_gen.generate(start_date=start_date, end_date=end_date)
            safe_print(f"Date range:  {start_date} to {end_date}")
        else:
            url_list = url_gen.generate(days=days)
            safe_print(f"Date range:  Last {days} days")
    except ValueError as e:
        safe_print(f"ERROR: {e}")
        logger.error("URL generation failed for %s: %s", district_name, e)
        return None

    safe_print(f"URLs:        {len(url_list)}")
    logger.info("Generated %d URLs for %s", len(url_list), district_name)

    filtered_list: List[str]
    skipped: int
    filtered_list, skipped = filter_existing_files(url_list, output_folder, force)
    if skipped > 0:
        safe_print(f"Skipping:    {skipped} already downloaded")
        safe_print(f"Remaining:   {len(filtered_list)}")

    if not filtered_list:
        safe_print("\nAll files already downloaded!")
        logger.info("All files already downloaded for %s", district_name)
        stats: DownloadStats = DownloadStats(total=0, skipped=skipped)
        return stats

    safe_print("\nAuthenticating...")
    auth: NiagaraAuth = NiagaraAuth(district_name)
    if cookie:
        cookies = auth.login_with_cookie(cookie)
        safe_print(f"Using provided cookie")
        logger.info("Authenticated with provided cookie for %s", district_name)
    else:
        cookies = auth.login(headless=headless, keep_driver=(toggle_interval > 0))
        if not cookies:
            safe_print("ERROR: Authentication failed")
            logger.error("Authentication failed for %s", district_name)
            return None
        logger.info("Authenticated successfully for %s", district_name)

    safe_print(f"\nStarting parallel download ({workers} workers)...")
    safe_print(f"Throttle: {throttle}s between requests" if throttle > 0 else "Max speed (no throttle)")
    safe_print("-" * 70)

    progress: ProgressPrinter = ProgressPrinter(show_every=max(1, len(filtered_list) // 100))
    start_time: float = time.time()

    with DownloadEngine(
        cookies=cookies,
        max_workers=workers,
        throttle_delay=throttle,
        progress_callback=progress
    ) as engine:
        stats = engine.download_batch_with_resume(filtered_list, output_folder, district=district_name)

    auth.close()
    stats.skipped = skipped
    elapsed: float = time.time() - start_time

    safe_print("-" * 70)
    safe_print(f"\nCOMPLETED: {district_name}")
    safe_print(f"  {stats.summary()}")
    safe_print(f"  Throughput: {stats.bytes_downloaded / 1024 / 1024:.1f} MB")
    logger.info(
        "Completed %s: %s (%.1f MB in %.1fs)",
        district_name, stats.summary(),
        stats.bytes_downloaded / 1024 / 1024, elapsed
    )

    if stats.errors and len(stats.errors) <= 10:
        safe_print("\nErrors:")
        for point, err in stats.errors[:10]:
            safe_print(f"  {point[:40]}: {err}")
            logger.error("Download error for %s: %s", point, err)
    elif stats.errors:
        safe_print(f"\n{len(stats.errors)} errors (see log for details)")
        logger.error("%d download errors for %s", len(stats.errors), district_name)

    return stats


def main() -> int:
    """Entry point: parse arguments and orchestrate district downloads.

    Returns:
        Exit code (0 for success, 1 for failure).
    """
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description='Fast Niagara BAS trend data downloader',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --list-districts
  %(prog)s --district WINDHAMSCHOOLSNH
  %(prog)s --district WINDHAMSCHOOLSNH --days 30 --workers 20
  %(prog)s --all-districts
        """
    )

    district_group = parser.add_mutually_exclusive_group()
    district_group.add_argument('--district', nargs='+', help='District name(s)')
    district_group.add_argument('--all-districts', action='store_true')
    district_group.add_argument('--list-districts', action='store_true')

    parser.add_argument('--days', type=int, default=DEFAULT_DAYS)
    parser.add_argument('--start', type=str, help='Start date YYYY-MM-DD')
    parser.add_argument('--end', type=str, help='End date YYYY-MM-DD')
    parser.add_argument('--workers', type=int, default=DEFAULT_WORKERS)
    parser.add_argument('--throttle', type=float, default=DEFAULT_THROTTLE)
    parser.add_argument('--output', type=str, help='Override output directory')
    parser.add_argument('--force', action='store_true')
    parser.add_argument('--cookie', type=str)
    parser.add_argument('--headless', action='store_true')
    parser.add_argument('--toggle-interval', type=int, default=DEFAULT_TOGGLE_INTERVAL)
    parser.add_argument('--auto-fetch', action='store_true')

    args: argparse.Namespace = parser.parse_args()

    if args.list_districts:
        list_districts()
        return 0

    districts: List[str]

    if args.all_districts:
        districts = get_available_districts()
        safe_print(f"\nProcessing ALL {len(districts)} districts")
        logger.info("Processing all %d districts", len(districts))
    elif args.district:
        districts = [d.upper() for d in args.district]
        valid = set(get_available_districts())
        invalid: List[str] = [d for d in districts if d not in valid]
        if invalid:
            safe_print(f"ERROR: Invalid districts: {', '.join(invalid)}")
            logger.error("Invalid districts: %s", ', '.join(invalid))
            return 1
    else:
        result: Optional[List[str]] = select_district_interactive()
        if not result:
            safe_print("No district selected.")
            return 0
        districts = result

    print_header(f"NIAGARA FAST DOWNLOAD v{APP_VERSION}")
    safe_print(f"Districts:   {len(districts)}")
    safe_print(f"Workers:     {args.workers}")
    safe_print(f"Days:        {args.days}")
    logger.info(
        "Starting download run: %d districts, %d workers, %d days",
        len(districts), args.workers, args.days
    )

    all_stats: List[Tuple[str, DownloadStats]] = []
    for i, district in enumerate(districts, 1):
        if len(districts) > 1:
            safe_print(f"\n[{i}/{len(districts)}] ", end='')

        stats: Optional[DownloadStats] = process_district(
            district,
            days=args.days, start_date=args.start, end_date=args.end,
            workers=args.workers, throttle=args.throttle,
            output_dir=args.output, force=args.force,
            cookie=args.cookie, headless=args.headless,
            toggle_interval=args.toggle_interval, auto_fetch=args.auto_fetch
        )
        if stats:
            all_stats.append((district, stats))

    if len(all_stats) > 1:
        print_header("OVERALL SUMMARY")
        total_success: int = sum(s.success for _, s in all_stats)
        total_failed: int = sum(s.failed for _, s in all_stats)
        total_skipped: int = sum(s.skipped for _, s in all_stats)
        total_bytes: int = sum(s.bytes_downloaded for _, s in all_stats)
        for district, stats in all_stats:
            safe_print(f"{district:25s} | OK:{stats.success:4d} | Fail:{stats.failed:3d} | Skip:{stats.skipped:4d}")
        safe_print("-" * 70)
        safe_print(f"{'TOTAL':25s} | OK:{total_success:4d} | Fail:{total_failed:3d} | Skip:{total_skipped:4d}")
        safe_print(f"Total data: {total_bytes / 1024 / 1024:.1f} MB")
        logger.info(
            "Overall: OK=%d, Fail=%d, Skip=%d, Data=%.1f MB",
            total_success, total_failed, total_skipped, total_bytes / 1024 / 1024
        )

    safe_print("\nDone!")
    return 0 if all(s.failed == 0 for _, s in all_stats) else 1


if __name__ == '__main__':
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        safe_print("\n\nCancelled by user.")
        logger.info("Cancelled by user (KeyboardInterrupt)")
        sys.exit(1)
