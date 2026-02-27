"""
================================================================================
NIAGARA BAS - FETCH POINT LIST v2.0
================================================================================
Fetches point lists from Niagara BAS systems.

Uses modular auth (NiagaraAuth), centralized credentials, and shared utilities.

USAGE:
    python fetch_pointlist.py                              # Interactive
    python fetch_pointlist.py --district PINKERTONACADEMY  # Browser fetch
    python fetch_pointlist.py --district PINKERTONACADEMY --auto  # Selenium
    python fetch_pointlist.py --check-all                  # Status
    python fetch_pointlist.py --fetch-missing              # Batch
================================================================================
"""

import os
import sys
import argparse
import platform
import webbrowser
import time
import shutil
import requests
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from utils import safe_print
from logging_config import get_logger
from config_district_details import district_config
from credentials import get_district_credentials
from niagara_auth import NiagaraAuth
from niagara_url_generator import get_point_list_path, load_point_list

logger = get_logger("fetch_pointlist")

# ============================================================================
# CONFIGURATION
# ============================================================================
SCRIPT_DIR = Path(__file__).parent
POINT_LISTS_DIR = SCRIPT_DIR / "point_lists"
POINT_LIST_PREFIX = "pointlist_"
POINT_LIST_URL_SUFFIX = "/ord?history:|bql:select%20id|view:file:ITableToCsv"
CUSTOM_URLS_FILE = SCRIPT_DIR / "get_new_pointlist.txt"
DOWNLOAD_WAIT_TIMEOUT = 120
DOWNLOAD_CHECK_INTERVAL = 2


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================
def ensure_point_lists_folder() -> Path:
    """Ensure point_lists folder exists."""
    if not POINT_LISTS_DIR.exists():
        POINT_LISTS_DIR.mkdir(parents=True, exist_ok=True)
        safe_print(f"Created: {POINT_LISTS_DIR}")
    return POINT_LISTS_DIR


def get_downloads_folder() -> Path:
    """Get user's downloads folder."""
    paths = [
        Path.home() / "Downloads",
        Path.home() / "Download",
    ]
    if os.name == 'nt':
        userprofile = os.environ.get('USERPROFILE', '')
        if userprofile:
            paths.append(Path(userprofile) / "Downloads")

    for p in paths:
        if p and p.exists():
            return p
    return Path.home() / "Downloads"


def get_point_list_url(base_ip: str) -> str:
    """Generate point list URL."""
    return f"{base_ip.rstrip('/')}{POINT_LIST_URL_SUFFIX}"


def load_custom_urls() -> Dict[str, str]:
    """Load custom URLs from get_new_pointlist.txt.

    File format:
        DISTRICTNAME
        http://ip/ord?history:|bql:select%20id|view:file:ITableToCsv

        ANOTHERDISTRICT
        https://ip/ord?...

    Returns:
        Dict mapping district name to custom URL.
    """
    custom_urls: Dict[str, str] = {}

    if not CUSTOM_URLS_FILE.exists():
        return custom_urls

    try:
        with open(CUSTOM_URLS_FILE, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        current_district: Optional[str] = None
        for line in lines:
            line = line.strip()

            # Skip empty lines, comments, and section markers
            if not line or line.startswith('#') or line.startswith('####'):
                current_district = None
                continue

            # Check if this is a URL line
            if line.startswith('http://') or line.startswith('https://'):
                if current_district:
                    custom_urls[current_district] = line
                    current_district = None
                continue

            # Check if this looks like a district name (all caps, no spaces, letters/numbers only)
            if line.isupper() and line.replace('_', '').isalnum() and len(line) > 3:
                current_district = line
                continue

            # Otherwise reset (might be a description line)
            current_district = None

        if custom_urls:
            safe_print(f"Loaded {len(custom_urls)} custom URLs from {CUSTOM_URLS_FILE.name}")

    except Exception as e:
        safe_print(f"Warning: Could not load {CUSTOM_URLS_FILE}: {e}")

    return custom_urls


def get_custom_url(district_name: str) -> Optional[str]:
    """Get custom URL for a district if defined in get_new_pointlist.txt."""
    custom_urls = load_custom_urls()
    return custom_urls.get(district_name.upper())


def get_output_path(district_name: str) -> str:
    """Get output path for district's point list."""
    ensure_point_lists_folder()
    return str(POINT_LISTS_DIR / f"{POINT_LIST_PREFIX}{district_name.upper()}.txt")


def open_folder(path: str) -> None:
    """Open folder in file explorer."""
    if platform.system() == 'Windows':
        os.startfile(path)
    elif platform.system() == 'Darwin':
        os.system(f'open "{path}"')
    else:
        os.system(f'xdg-open "{path}"')


def check_point_list_exists(district_name: str) -> Tuple[bool, str, str]:
    """Check if point list exists.

    Returns:
        Tuple of (exists, path, source).
    """
    path, source = get_point_list_path(district_name)
    if path is not None:
        return True, path, source
    return False, get_output_path(district_name), 'none'


def count_points_in_file(filepath: str) -> int:
    """Count points in file."""
    try:
        with open(filepath, 'r', encoding='utf-8-sig') as f:
            lines = [l.strip() for l in f if l.strip() and not l.startswith('#')]
        return len(lines)
    except Exception:
        return 0


# ============================================================================
# DOWNLOAD MONITORING
# ============================================================================
def find_recent_csv_download(downloads_folder: Path, start_time: float) -> Optional[Path]:
    """Find CSV files downloaded after start_time."""
    patterns = ['*.csv', '*.CSV', 'ITableToCsv*', '*.txt']
    recent: List[Tuple[Path, float]] = []

    for pattern in patterns:
        for fp in downloads_folder.glob(pattern):
            if fp.is_file() and fp.stat().st_mtime > start_time:
                recent.append((fp, fp.stat().st_mtime))

    if recent:
        recent.sort(key=lambda x: x[1], reverse=True)
        return recent[0][0]
    return None


def wait_for_download(downloads_folder: Path, start_time: float, timeout: int = DOWNLOAD_WAIT_TIMEOUT) -> Optional[Path]:
    """Wait for download to appear."""
    safe_print(f"\nWaiting for download (up to {timeout}s)...")

    elapsed = 0
    while elapsed < timeout:
        csv_file = find_recent_csv_download(downloads_folder, start_time)
        if csv_file:
            time.sleep(1)
            size1 = csv_file.stat().st_size
            time.sleep(1)
            size2 = csv_file.stat().st_size
            if size1 == size2 and size1 > 0:
                safe_print(f"[OK] Download detected: {csv_file.name}")
                return csv_file

        if elapsed > 0 and elapsed % 10 == 0:
            safe_print(f"  Still waiting... ({elapsed}s)")

        time.sleep(DOWNLOAD_CHECK_INTERVAL)
        elapsed += DOWNLOAD_CHECK_INTERVAL

    safe_print("[X] Timeout waiting for download")
    return None


def save_content_to_pointlist(content: bytes | str, district_name: str, backup: bool = True) -> str:
    """Save content to point list file."""
    dest_path = get_output_path(district_name)

    if backup and os.path.exists(dest_path):
        backup_path = dest_path.replace('.txt', f'_backup_{datetime.now():%Y%m%d_%H%M%S}.txt')
        shutil.move(dest_path, backup_path)
        safe_print(f"  Backed up: {os.path.basename(backup_path)}")

    mode = 'wb' if isinstance(content, bytes) else 'w'
    with open(dest_path, mode) as f:
        f.write(content)

    safe_print(f"[OK] Saved: {dest_path}")
    safe_print(f"  Points: {count_points_in_file(dest_path)}")

    return dest_path


def move_download_to_pointlist(source_path: Path, district_name: str, backup: bool = True) -> str:
    """Move downloaded file to point_lists folder."""
    dest_path = get_output_path(district_name)

    if backup and os.path.exists(dest_path):
        backup_path = dest_path.replace('.txt', f'_backup_{datetime.now():%Y%m%d_%H%M%S}.txt')
        shutil.move(dest_path, backup_path)
        safe_print(f"  Backed up: {os.path.basename(backup_path)}")

    shutil.move(str(source_path), dest_path)
    safe_print(f"[OK] Saved: {dest_path}")
    safe_print(f"  Points: {count_points_in_file(dest_path)}")

    return dest_path


# ============================================================================
# FETCH METHODS
# ============================================================================
def fetch_pointlist_selenium(district_name: str, headless: bool = False) -> bool:
    """Fetch using Selenium with automated login via NiagaraAuth."""
    district = district_name.upper()
    config = district_config.get(district)

    if not config:
        safe_print(f"ERROR: District '{district}' not found")
        return False

    base_ip = config.get('BASE_IP', '')
    if not base_ip or base_ip.lower() in ('na', 'n/a', ''):
        safe_print(f"ERROR: No BASE_IP for {district}")
        return False

    # Check for custom URL first
    custom_url = get_custom_url(district)
    if custom_url:
        url = custom_url
        safe_print("Using custom URL from get_new_pointlist.txt")
    else:
        url = get_point_list_url(base_ip)

    safe_print(f"\n{'='*60}")
    safe_print(f"AUTOMATED FETCH: {district}")
    safe_print(f"{'='*60}")
    safe_print(f"URL: {url}")

    auth = NiagaraAuth(district)
    if not auth.has_credentials:
        safe_print(f"ERROR: No credentials for {district}")
        return False

    safe_print(f"Username: {auth.username}")
    cookies = auth.login(headless=headless)

    if not cookies:
        safe_print("ERROR: Login failed")
        return False

    # Fetch point list
    safe_print("Fetching point list...")

    try:
        response = requests.get(url, cookies=cookies, timeout=60, verify=False)
        response.raise_for_status()

        content = response.content

        if len(content) < 50:
            safe_print("WARNING: Response too short")
            if b'login' in content.lower():
                safe_print("ERROR: Session invalid")
                return False

        save_content_to_pointlist(content, district)
        safe_print("\n[OK] Point list fetched!")
        return True

    except Exception as e:
        safe_print(f"ERROR: {e}")
        return False


def fetch_pointlist_with_cookie(district_name: str, cookie_value: str) -> bool:
    """Fetch using existing session cookie."""
    district = district_name.upper()
    config = district_config.get(district)

    if not config:
        safe_print(f"ERROR: District '{district}' not found")
        return False

    base_ip = config.get('BASE_IP', '')
    if not base_ip:
        safe_print(f"ERROR: No BASE_IP for {district}")
        return False

    # Check for custom URL first
    custom_url = get_custom_url(district)
    if custom_url:
        url = custom_url
        safe_print("Using custom URL from get_new_pointlist.txt")
    else:
        url = get_point_list_url(base_ip)

    safe_print(f"\n{'='*60}")
    safe_print(f"FETCH WITH COOKIE: {district}")
    safe_print(f"{'='*60}")
    safe_print(f"URL: {url}")

    if '=' in cookie_value:
        name, value = cookie_value.split('=', 1)
        cookies = {name: value}
    else:
        cookies = {'JSESSIONID': cookie_value}

    try:
        response = requests.get(url, cookies=cookies, timeout=60, verify=False)
        response.raise_for_status()

        content = response.content

        if len(content) < 50:
            if b'login' in content.lower():
                safe_print("ERROR: Session invalid")
                return False

        save_content_to_pointlist(content, district)
        safe_print("\n[OK] Point list fetched!")
        return True

    except Exception as e:
        safe_print(f"ERROR: {e}")
        return False


def fetch_pointlist_browser(district_name: str, auto_save: bool = True) -> bool:
    """Open URL in browser for manual download."""
    district = district_name.upper()
    config = district_config.get(district)

    if not config:
        safe_print(f"ERROR: District '{district}' not found")
        return False

    base_ip = config.get('BASE_IP', '')
    if not base_ip:
        safe_print(f"ERROR: No BASE_IP for {district}")
        return False

    # Check for custom URL first
    custom_url = get_custom_url(district)
    if custom_url:
        url = custom_url
        safe_print("Using custom URL from get_new_pointlist.txt")
    else:
        url = get_point_list_url(base_ip)
    output_path = get_output_path(district)

    safe_print(f"\n{'='*60}")
    safe_print(f"BROWSER FETCH: {district}")
    safe_print(f"{'='*60}")
    safe_print(f"URL: {url}")
    safe_print(f"Output: {output_path}")
    safe_print("\nEnsure you are logged into Niagara in your browser.")

    proceed = input("\nOpen URL? [Y/n]: ").strip().lower()
    if proceed == 'n':
        return False

    start_time = time.time()
    downloads_folder = get_downloads_folder()

    safe_print("\nOpening browser...")
    webbrowser.open(url)

    if not auto_save:
        safe_print(f"\nManual: Save to {output_path}")
        return True

    downloaded = wait_for_download(downloads_folder, start_time)

    if downloaded:
        move_download_to_pointlist(downloaded, district)
        return True
    else:
        safe_print(f"\nIf downloaded, move to: {output_path}")
        open_it = input("Open point_lists folder? [Y/n]: ").strip().lower()
        if open_it != 'n':
            open_folder(str(POINT_LISTS_DIR))
        return False


# ============================================================================
# STATUS & BATCH OPERATIONS
# ============================================================================
def list_available_districts() -> List[str]:
    """List all districts."""
    safe_print(f"\n{'='*70}")
    safe_print("AVAILABLE DISTRICTS")
    safe_print(f"{'='*70}")

    districts = sorted(district_config.keys())

    for i, district in enumerate(districts, 1):
        config = district_config[district]
        base_ip = config.get('BASE_IP', '')

        user, passwd = get_district_credentials(district)
        cred = "Y" if (user and passwd) else "N"

        exists, _, source = check_point_list_exists(district)
        pl = "Y" if exists else "N"

        has_ip = "Y" if (base_ip and base_ip.lower() not in ('na', 'n/a', '')) else "N"

        safe_print(f"{i:2d}. [{cred}][{pl}][{has_ip}] {district:25s} | {base_ip[:35]}")

    safe_print(f"{'='*70}")
    safe_print("[Creds][PointList][HasIP] Y=yes N=no")
    return districts


def check_all_districts() -> None:
    """Show status for all districts."""
    safe_print(f"\n{'='*70}")
    safe_print("POINT LIST STATUS")
    safe_print(f"{'='*70}")

    found = missing = 0
    missing_list: List[Tuple[str, bool]] = []

    for district in sorted(district_config.keys()):
        exists, path, source = check_point_list_exists(district)
        config = district_config.get(district, {})
        base_ip = config.get('BASE_IP', '')
        has_ip = bool(base_ip and base_ip.lower() not in ('na', 'n/a', ''))

        if exists:
            found += 1
            count = count_points_in_file(path)
            safe_print(f"  [OK] {district:30s} {count:5d} pts ({source})")
        else:
            missing += 1
            ip_note = "has IP" if has_ip else "NO IP"
            missing_list.append((district, has_ip))
            safe_print(f"  [--] {district:30s} MISSING   ({ip_note})")

    safe_print(f"{'='*70}")
    safe_print(f"Found: {found} | Missing: {missing}")

    if missing_list:
        fetchable = [d for d, has_ip in missing_list if has_ip]
        if fetchable:
            safe_print(f"\nFetchable ({len(fetchable)}): {', '.join(fetchable[:5])}...")


def get_missing_districts() -> List[str]:
    """Get districts missing point lists with valid IPs."""
    missing: List[str] = []
    for district in sorted(district_config.keys()):
        exists, _, _ = check_point_list_exists(district)
        if not exists:
            config = district_config.get(district, {})
            base_ip = config.get('BASE_IP', '')
            if base_ip and base_ip.lower() not in ('na', 'n/a', ''):
                missing.append(district)
    return missing


def fetch_missing_pointlists(use_selenium: bool = False, cookie: Optional[str] = None) -> None:
    """Fetch all missing point lists."""
    missing = get_missing_districts()

    if not missing:
        safe_print("\n[OK] All districts have point lists!")
        return

    safe_print(f"\n{'='*60}")
    safe_print(f"FETCH MISSING: {len(missing)} districts")
    safe_print(f"{'='*60}")

    for i, d in enumerate(missing, 1):
        config = district_config.get(d, {})
        safe_print(f"  {i}. {d}: {config.get('BASE_IP', '')}")

    if cookie:
        safe_print("\nUsing provided cookie...")
        for d in missing:
            safe_print(f"\n--- {d} ---")
            fetch_pointlist_with_cookie(d, cookie)
            time.sleep(2)
    elif use_selenium:
        safe_print("\nUsing Selenium...")
        for d in missing:
            safe_print(f"\n--- {d} ---")
            fetch_pointlist_selenium(d)
            time.sleep(2)
    else:
        safe_print("\nWill open browser for each. Must be logged in.")
        proceed = input("Continue? [Y/n]: ").strip().lower()
        if proceed == 'n':
            return

        for d in missing:
            safe_print(f"\n--- {d} ---")
            fetch_pointlist_browser(d)
            cont = input("\nNext? [Y/n]: ").strip().lower()
            if cont == 'n':
                break


def select_district_interactive() -> Optional[str]:
    """Interactive district selection."""
    districts = list_available_districts()

    while True:
        choice = input("\nSelect number (or 'q'): ").strip()
        if choice.lower() == 'q':
            return None
        try:
            num = int(choice)
            if 1 <= num <= len(districts):
                return districts[num - 1]
        except ValueError:
            if choice.upper() in districts:
                return choice.upper()
        safe_print("Invalid selection")


# ============================================================================
# MAIN
# ============================================================================
def main() -> int:
    parser = argparse.ArgumentParser(
        description='Fetch point lists from Niagara BAS',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                                  # Interactive
  %(prog)s --district PINKERTONACADEMY      # Browser fetch
  %(prog)s --district PINKERTONACADEMY --auto
  %(prog)s --check-all
  %(prog)s --fetch-missing --auto
  %(prog)s --show-custom                    # Show URLs from get_new_pointlist.txt
        """
    )

    parser.add_argument('--district', type=str, help='District name')
    parser.add_argument('--list-districts', action='store_true')
    parser.add_argument('--auto', action='store_true', help='Use Selenium')
    parser.add_argument('--cookie', type=str, help='Session cookie')
    parser.add_argument('--url-only', action='store_true')
    parser.add_argument('--check-all', action='store_true')
    parser.add_argument('--fetch-missing', action='store_true')
    parser.add_argument('--open-folder', action='store_true')
    parser.add_argument('--headless', action='store_true')
    parser.add_argument('--show-custom', action='store_true', help='Show custom URLs from get_new_pointlist.txt')

    args = parser.parse_args()

    if args.show_custom:
        custom_urls = load_custom_urls()
        if custom_urls:
            safe_print(f"\n{'='*70}")
            safe_print(f"CUSTOM URLs FROM {CUSTOM_URLS_FILE.name}")
            safe_print(f"{'='*70}")
            for district, url in sorted(custom_urls.items()):
                safe_print(f"\n{district}:")
                safe_print(f"  {url}")
            safe_print(f"\n{'='*70}")
            safe_print(f"Total: {len(custom_urls)} custom URLs defined")
        else:
            safe_print(f"No custom URLs found in {CUSTOM_URLS_FILE}")
        return 0

    if args.list_districts:
        list_available_districts()
        return 0

    if args.check_all:
        check_all_districts()
        return 0

    if args.open_folder:
        ensure_point_lists_folder()
        open_folder(str(POINT_LISTS_DIR))
        return 0

    if args.fetch_missing:
        fetch_missing_pointlists(use_selenium=args.auto, cookie=args.cookie)
        return 0

    # Single district
    if args.district:
        district = args.district.upper()
        if district not in district_config:
            safe_print(f"ERROR: '{district}' not found")
            return 1
    else:
        district = select_district_interactive()
        if not district:
            return 0

    config = district_config.get(district, {})
    base_ip = config.get('BASE_IP', '')

    if not base_ip or base_ip.lower() in ('na', 'n/a', ''):
        safe_print(f"ERROR: No BASE_IP for {district}")
        return 1

    if args.url_only:
        safe_print(get_point_list_url(base_ip))
        return 0

    if args.cookie:
        success = fetch_pointlist_with_cookie(district, args.cookie)
    elif args.auto:
        success = fetch_pointlist_selenium(district, headless=args.headless)
    else:
        success = fetch_pointlist_browser(district)

    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())
