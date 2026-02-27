"""
================================================================================
NIAGARA BAS CLI - Interactive Command Line Interface v2.0
================================================================================
Interactive CLI for downloading trend data from Niagara BAS systems.
Uses the fast parallel download engine.

Features:
    - VPN connection status check and credential display
    - Interactive district selection
    - Progress tracking with parallel downloads
    - Session management
    - Performance tuning options

USAGE:
    python niagara_cli.py
================================================================================
"""

import os
import sys
import time
import socket
from typing import Optional, List, Dict, Tuple, Any

# ============================================================================
# SHARED UTILITIES (consolidated in utils.py)
# ============================================================================
from utils import safe_print, print_header, print_separator, setup_console_encoding, APP_VERSION
from utils import SYM_CHECK, SYM_EMPTY, SYM_FAIL, SYM_OK, SYM_WARN, SYM_BULLET
from logging_config import get_logger
from config_district_details import district_config
from credentials import get_district_credentials

setup_console_encoding()

logger = get_logger("cli")

# Optional tkinter import
TK_AVAILABLE: bool = False
try:
    import tkinter as tk
    from tkinter import filedialog
    TK_AVAILABLE = True
except ImportError:
    pass

# Import fast modules (mandatory in V2.0)
from niagara_download_engine import DownloadEngine, ProgressPrinter, filter_existing_files
from niagara_url_generator import URLGenerator, get_available_districts, get_point_list_path
from niagara_auth import NiagaraAuth

# Import fetch_pointlist module for creating new point lists
try:
    from fetch_pointlist import (
        fetch_pointlist_selenium,
        fetch_pointlist_browser,
        get_output_path as get_pointlist_output_path,
        ensure_point_lists_folder
    )
    FETCH_POINTLIST_AVAILABLE: bool = True
except ImportError:
    FETCH_POINTLIST_AVAILABLE = False

# ============================================================================
# CONFIGURATION
# ============================================================================
DEFAULT_DAYS: int = 90
DEFAULT_WORKERS: int = 10
DEFAULT_THROTTLE: float = 0.0

SCRIPT_DIR: str = os.path.dirname(os.path.abspath(__file__))


def reload_env() -> bool:
    """Reload .env file."""
    try:
        from dotenv import load_dotenv
        env_path = os.path.join(SCRIPT_DIR, '.env')
        if os.path.exists(env_path):
            load_dotenv(env_path, override=True)
            return True
    except ImportError:
        pass
    return False


# ============================================================================
# OUTPUT DIRECTORY MANAGEMENT
# ============================================================================
def get_default_output_dir() -> str:
    """Get default output directory from .env or config."""
    reload_env()
    output_dir = os.environ.get('OUTPUT_DIR', '')
    if output_dir:
        return output_dir
    return os.path.join(SCRIPT_DIR, 'output')


def save_output_dir_to_env(output_dir: str) -> None:
    """Save output directory to .env file."""
    env_path = os.path.join(SCRIPT_DIR, '.env')

    content = ''
    if os.path.exists(env_path):
        with open(env_path, 'r') as f:
            content = f.read()

    lines = content.split('\n') if content else []
    output_found = False
    new_lines: List[str] = []

    for line in lines:
        if line.startswith('OUTPUT_DIR='):
            new_lines.append(f'OUTPUT_DIR={output_dir}')
            output_found = True
        else:
            new_lines.append(line)

    if not output_found:
        insert_idx = 0
        for i, line in enumerate(new_lines):
            if not line.startswith('#') and line.strip():
                insert_idx = i
                break
        new_lines.insert(insert_idx, f'OUTPUT_DIR={output_dir}')
        new_lines.insert(insert_idx + 1, '')

    with open(env_path, 'w') as f:
        f.write('\n'.join(new_lines))

    os.environ['OUTPUT_DIR'] = output_dir
    reload_env()


def browse_for_folder(title: str = "Select Output Directory",
                      initial_dir: Optional[str] = None) -> Optional[str]:
    """Open folder browser dialog."""
    if not TK_AVAILABLE:
        safe_print(f"  {SYM_WARN} Folder browser not available (tkinter not installed)")
        return None

    try:
        root = tk.Tk()
        root.withdraw()
        root.attributes('-topmost', True)
        folder = filedialog.askdirectory(
            title=title,
            initialdir=initial_dir or os.path.expanduser('~')
        )
        root.destroy()
        return folder if folder else None
    except Exception as e:
        safe_print(f"  {SYM_WARN} Folder browser error: {e}")
        return None


def select_output_directory() -> Optional[str]:
    """Let user select output directory."""
    current_dir = get_default_output_dir()

    safe_print("\n" + "=" * 70)
    safe_print(" OUTPUT DIRECTORY SELECTION")
    safe_print("=" * 70)
    safe_print(f"\n  Current: {current_dir}")

    if current_dir:
        drive = os.path.splitdrive(current_dir)[0]
        if drive and not os.path.exists(drive + '\\'):
            safe_print(f"  {SYM_WARN} Warning: Drive {drive} not accessible!")

    safe_print("\n  Options:")
    safe_print("    1. Use current directory")
    safe_print("    2. Browse for new directory")
    safe_print("    3. Enter path manually")
    safe_print("    q. Cancel")

    choice = get_user_input("\n  Select [1]: ", allow_empty=True)

    if choice is None or choice.lower() == 'q':
        return None

    if choice == '' or choice == '1':
        selected = current_dir
    elif choice == '2':
        safe_print("\n  Opening folder browser...")
        selected = browse_for_folder(
            "Select Output Directory",
            current_dir if os.path.exists(current_dir) else os.path.expanduser('~')
        )
        if not selected:
            return select_output_directory()
    elif choice == '3':
        selected = get_user_input("\n  Enter path: ")
        if not selected:
            return select_output_directory()
    else:
        safe_print("  Invalid option.")
        return select_output_directory()

    # Validate path
    drive = os.path.splitdrive(selected)[0]
    if drive and not os.path.exists(drive + '\\'):
        safe_print(f"\n  {SYM_FAIL} Drive {drive} not accessible!")
        return select_output_directory()

    # Create if needed
    if not os.path.exists(selected):
        try:
            os.makedirs(selected, exist_ok=True)
            safe_print(f"\n  {SYM_OK} Created: {selected}")
        except PermissionError:
            safe_print(f"\n  {SYM_FAIL} Permission denied: {selected}")
            return select_output_directory()
        except Exception as e:
            safe_print(f"\n  {SYM_FAIL} Error: {e}")
            return select_output_directory()

    if confirm_prompt(f"\n  Save as default?"):
        save_output_dir_to_env(selected)
        safe_print(f"  {SYM_OK} Saved to .env")

    return selected


# ============================================================================
# CONSOLE UTILITIES
# ============================================================================
def clear_screen() -> None:
    """Clear terminal screen."""
    os.system('cls' if os.name == 'nt' else 'clear')


def get_user_input(prompt: str, valid_options: Optional[List[str]] = None,
                   allow_empty: bool = False) -> Optional[str]:
    """Get validated user input."""
    while True:
        try:
            user_input = input(prompt).strip()

            if not user_input and not allow_empty:
                safe_print("  Please enter a value.")
                continue

            if valid_options:
                if user_input.upper() in [opt.upper() for opt in valid_options]:
                    return user_input
                safe_print(f"  Invalid. Choose: {', '.join(valid_options)}")
                continue

            return user_input

        except KeyboardInterrupt:
            safe_print("\n\n  Cancelled.")
            return None
        except EOFError:
            return None


def confirm_prompt(message: str, default: bool = True) -> bool:
    """Ask yes/no confirmation."""
    default_str = "[Y/n]" if default else "[y/N]"
    response = get_user_input(f"{message} {default_str}: ", allow_empty=True)

    if response is None:
        return False
    if response == "":
        return default

    return response.lower() in ('y', 'yes')


# ============================================================================
# VPN STATUS
# ============================================================================
def check_network_connectivity(host: str = "8.8.8.8", port: int = 53,
                               timeout: int = 3) -> bool:
    """Check internet connectivity."""
    try:
        socket.setdefaulttimeout(timeout)
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((host, port))
        return True
    except socket.error:
        return False


def check_vpn_connectivity(test_ip: str) -> bool:
    """Check if we can reach internal IP (VPN connected)."""
    try:
        import re
        ip_match = re.search(r'(\d+\.\d+\.\d+\.\d+)', test_ip)
        if not ip_match:
            return False

        ip = ip_match.group(1)
        socket.setdefaulttimeout(5)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex((ip, 80))
        sock.close()

        if result != 0:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex((ip, 443))
            sock.close()

        return result == 0
    except Exception:
        return False


def get_vpn_info(district_name: str) -> Dict[str, str]:
    """Get VPN connection info for a district."""
    district = district_name.upper()
    config = district_config.get(district, {})
    vpn_data = config.get('VPN_DATA', 'Not configured')
    base_ip = config.get('BASE_IP', '')

    vpn_info: Dict[str, str] = {
        'raw': vpn_data,
        'bas_ip': base_ip,
        'type': 'Unknown',
        'address': '',
        'username': '',
        'password': '',
        'port': '',
        'preshare_key': '',
    }

    # Check .env
    env_vpn_type = os.environ.get(f'{district}_VPN_TYPE', '')
    env_vpn_gateway = os.environ.get(f'{district}_VPN_GATEWAY', '')
    env_vpn_port = os.environ.get(f'{district}_VPN_PORT', '')
    env_vpn_user = os.environ.get(f'{district}_VPN_USER', '')
    env_vpn_pass = os.environ.get(f'{district}_VPN_PASS', '')

    if env_vpn_type or env_vpn_gateway:
        vpn_info['type'] = env_vpn_type or 'Unknown'
        vpn_info['address'] = env_vpn_gateway
        vpn_info['port'] = env_vpn_port
        vpn_info['username'] = env_vpn_user
        vpn_info['password'] = env_vpn_pass
    else:
        # Parse from VPN_DATA string
        vpn_lower = vpn_data.lower()
        if 'forticlient' in vpn_lower:
            vpn_info['type'] = 'FortiClient'
        elif 'global protect' in vpn_lower:
            vpn_info['type'] = 'GlobalProtect'
        elif 'sonicwall' in vpn_lower:
            vpn_info['type'] = 'SonicWall'
        elif 'direct' in vpn_lower:
            vpn_info['type'] = 'Direct Access'

        import re
        addr_match = re.search(r'[Aa]d[dr]?ess?[:\s]+([^\s,]+)', vpn_data)
        if addr_match:
            vpn_info['address'] = addr_match.group(1)

        user_match = re.search(r'[Uu]ser(?:name)?[:\s]+([^\s,]+)', vpn_data)
        if user_match:
            vpn_info['username'] = user_match.group(1)

        pass_match = re.search(r'[Pp]ass(?:word)?[:\s]+([^\s,\)]+)', vpn_data)
        if pass_match:
            vpn_info['password'] = pass_match.group(1)

    # BAS credentials from credentials module (V2.0)
    bas_user, bas_pass = get_district_credentials(district)
    vpn_info['bas_username'] = bas_user
    vpn_info['bas_password'] = bas_pass

    return vpn_info


def display_vpn_credentials(vpn_info: Dict[str, str]) -> None:
    """Display VPN credentials."""
    print_separator()
    safe_print(f"  VPN Type:       {vpn_info['type']}")
    if vpn_info.get('address'):
        safe_print(f"  Gateway:        {vpn_info['address']}")
    if vpn_info.get('port'):
        safe_print(f"  Port:           {vpn_info['port']}")
    if vpn_info.get('username'):
        safe_print(f"  VPN User:       {vpn_info['username']}")
    if vpn_info.get('password'):
        safe_print(f"  VPN Pass:       {vpn_info['password']}")

    if vpn_info.get('bas_ip') or vpn_info.get('bas_username'):
        print_separator()
        safe_print("  NIAGARA/BAS:")
        if vpn_info.get('bas_ip'):
            safe_print(f"  BAS IP:         {vpn_info['bas_ip']}")
        if vpn_info.get('bas_username'):
            safe_print(f"  BAS User:       {vpn_info['bas_username']}")
        if vpn_info.get('bas_password'):
            safe_print(f"  BAS Pass:       {vpn_info['bas_password']}")

    if vpn_info.get('raw'):
        print_separator()
        safe_print(f"  Details: {vpn_info['raw'][:60]}...")
    print_separator()


# ============================================================================
# DISTRICT SELECTION
# ============================================================================
def list_districts_with_status() -> List[str]:
    """List all districts with status."""
    print_header("AVAILABLE DISTRICTS")

    districts = get_available_districts()

    for i, district in enumerate(districts, 1):
        config = district_config.get(district, {})
        base_ip = config.get('BASE_IP', 'N/A')
        vpn_type = get_vpn_info(district)['type']

        # Credential status via credentials module
        user, passwd = get_district_credentials(district)
        cred_status = SYM_CHECK if (user and passwd) else SYM_EMPTY

        # Point list status
        path, source = get_point_list_path(district)
        pl_status = SYM_CHECK if path else SYM_EMPTY

        vpn_short = vpn_type[:12] if len(vpn_type) > 12 else vpn_type
        safe_print(f"{i:2d}. [{cred_status}][{pl_status}] {district:25s} | {vpn_short:12s} | {base_ip[:30]}")

    print_separator("=")
    safe_print(f"Total: {len(districts)} | [Creds][PointList] {SYM_CHECK}=yes {SYM_EMPTY}=no")

    return districts


def select_district() -> Optional[List[str]]:
    """Interactive district selection."""
    districts = list_districts_with_status()

    safe_print("\nEnter number, name, or:")
    safe_print("  'q' quit | 'a' all | 'f' filter by VPN")

    while True:
        choice = get_user_input("\nSelect: ")

        if choice is None or choice.lower() == 'q':
            return None

        if choice.lower() == 'a':
            return districts

        if choice.lower() == 'f':
            return filter_districts_by_vpn(districts)

        try:
            choice_num = int(choice)
            if 1 <= choice_num <= len(districts):
                return [districts[choice_num - 1]]
            safe_print(f"  Invalid. Enter 1-{len(districts)}")
            continue
        except ValueError:
            pass

        choice_upper = choice.upper()
        if choice_upper in districts:
            return [choice_upper]

        matches = [d for d in districts if choice_upper in d]
        if len(matches) == 1:
            return matches
        elif len(matches) > 1:
            safe_print(f"  Multiple: {', '.join(matches)}")
            continue

        safe_print(f"  '{choice}' not found.")


def filter_districts_by_vpn(districts: List[str]) -> Optional[List[str]]:
    """Filter districts by VPN type."""
    vpn_types: Dict[str, List[str]] = {}
    for district in districts:
        vpn_type = get_vpn_info(district)['type']
        if vpn_type not in vpn_types:
            vpn_types[vpn_type] = []
        vpn_types[vpn_type].append(district)

    print_header("VPN TYPES")
    for i, (vpn_type, dlist) in enumerate(sorted(vpn_types.items()), 1):
        safe_print(f"{i:2d}. {vpn_type:20s} ({len(dlist)} districts)")

    choice = get_user_input("\nSelect type: ")
    try:
        choice_num = int(choice)
        vpn_list = sorted(vpn_types.items())
        if 1 <= choice_num <= len(vpn_list):
            return vpn_list[choice_num - 1][1]
    except (ValueError, IndexError):
        pass

    safe_print("  Invalid.")
    return None


# ============================================================================
# VPN CHECK WORKFLOW
# ============================================================================
def run_vpn_check_workflow(selected_districts: List[str]) -> bool:
    """Run VPN check workflow."""
    reload_env()

    print_header("VPN CONNECTION CHECK")

    safe_print("\n  Checking internet...")
    if not check_network_connectivity():
        safe_print(f"  {SYM_WARN} No internet connectivity!")
        return False
    safe_print(f"  {SYM_OK} Internet OK")

    # Group by VPN type
    vpn_groups: Dict[str, List[Tuple[str, Dict[str, str]]]] = {}
    for district in selected_districts:
        vpn_info = get_vpn_info(district)
        vpn_type = vpn_info['type']
        if vpn_type not in vpn_groups:
            vpn_groups[vpn_type] = []
        vpn_groups[vpn_type].append((district, vpn_info))

    safe_print(f"\n  {len(selected_districts)} district(s) require:")
    for vpn_type, group in sorted(vpn_groups.items()):
        safe_print(f"    {SYM_BULLET} {vpn_type}: {len(group)}")

    direct_access = [d for d, v in vpn_groups.get('Direct Access', [])]
    needs_vpn = [d for d in selected_districts if d not in direct_access]

    if not needs_vpn:
        safe_print(f"\n  {SYM_OK} No VPN required")
        return confirm_prompt("\n  Ready?")

    print_separator()
    vpn_connected = confirm_prompt("  Are you connected to VPN?", default=False)

    if not vpn_connected:
        print_header("VPN CREDENTIALS")

        for vpn_type, group in sorted(vpn_groups.items()):
            if vpn_type in ('Direct Access', 'Onsite Only'):
                continue

            safe_print(f"\n  === {vpn_type} ===")
            for district, vpn_info in group:
                safe_print(f"\n  District: {district}")
                display_vpn_credentials(vpn_info)

        safe_print("\n" + "=" * 70)
        safe_print("  Connect to VPN, then press ENTER")
        safe_print("=" * 70)

        input("\n  Press ENTER when connected...")

        safe_print("\n  Verifying...")
        test_district = needs_vpn[0]
        test_ip = district_config[test_district].get('BASE_IP', '')

        if test_ip and check_vpn_connectivity(test_ip):
            safe_print(f"  {SYM_OK} Connected to {test_district}")
        else:
            safe_print(f"  {SYM_WARN} Could not verify {test_district}")

        return confirm_prompt("\n  Proceed?")

    else:
        safe_print("\n  Verifying...")
        test_district = needs_vpn[0]
        test_ip = district_config[test_district].get('BASE_IP', '')

        if test_ip and check_vpn_connectivity(test_ip):
            safe_print(f"  {SYM_OK} VPN verified")
        else:
            safe_print(f"  {SYM_WARN} Could not verify")
            if not confirm_prompt("  Continue anyway?"):
                return False

        return True


# ============================================================================
# DOWNLOAD WORKFLOW
# ============================================================================
def run_download_workflow(selected_districts: List[str], days: int = DEFAULT_DAYS,
                         output_dir: Optional[str] = None,
                         workers: int = DEFAULT_WORKERS,
                         auto_fetch: bool = False) -> int:
    """Run download using fast parallel engine."""
    print_header("PARALLEL DOWNLOAD")

    safe_print(f"\n  Districts: {', '.join(selected_districts)}")
    safe_print(f"  Days:      {days}")
    safe_print(f"  Workers:   {workers}")
    if output_dir:
        safe_print(f"  Output:    {output_dir}")

    all_stats: List[Tuple[str, Any]] = []

    for i, district in enumerate(selected_districts, 1):
        if len(selected_districts) > 1:
            safe_print(f"\n[{i}/{len(selected_districts)}] {district}")

        print_header(f"PROCESSING: {district}")

        # URL generator
        try:
            url_gen = URLGenerator(district)
        except ValueError as e:
            safe_print(f"ERROR: {e}")
            logger.error("URLGenerator error for %s: %s", district, e)
            continue

        info = url_gen.info()
        safe_print(f"Base IP:    {info['base_ip']}")
        safe_print(f"Points:     {info['point_count']}")

        if not url_gen.has_point_list:
            safe_print(f"\n{SYM_FAIL} No point list found for {district}!")

            # Ask user if they want to create a new point list
            if FETCH_POINTLIST_AVAILABLE:
                if confirm_prompt(f"  Would you like to create a new point list now?", default=True):
                    safe_print(f"\n  Creating point list for {district}...")
                    safe_print(f"  Output: {get_pointlist_output_path(district)}")

                    # Ask which method to use
                    safe_print("\n  Fetch method:")
                    safe_print("    1. Browser (manual login, then auto-save)")
                    safe_print("    2. Selenium (automated login)")
                    safe_print("    q. Skip this district")

                    method = get_user_input("\n  Select [1]: ", allow_empty=True)

                    if method is None or method.lower() == 'q':
                        safe_print(f"  Skipping {district}...")
                        continue

                    success = False
                    if method == '' or method == '1':
                        success = fetch_pointlist_browser(district, auto_save=True)
                    elif method == '2':
                        success = fetch_pointlist_selenium(district, headless=False)
                    else:
                        safe_print("  Invalid option, skipping...")
                        continue

                    if success:
                        safe_print(f"\n  {SYM_OK} Point list created successfully!")
                        # Reload the URL generator to pick up the new point list
                        try:
                            url_gen = URLGenerator(district)
                            if not url_gen.has_point_list:
                                safe_print(f"  {SYM_WARN} Point list still not detected. Please check the file.")
                                continue
                            info = url_gen.info()
                            safe_print(f"  Points loaded: {info['point_count']}")
                        except Exception as e:
                            safe_print(f"  {SYM_FAIL} Error reloading: {e}")
                            continue
                    else:
                        safe_print(f"\n  {SYM_FAIL} Failed to create point list.")
                        continue
                else:
                    safe_print(f"  Run: python fetch_pointlist.py --district {district}")
                    continue
            else:
                safe_print(f"  Run: python fetch_pointlist.py --district {district}")
                continue

        # Output folder
        if output_dir:
            out_folder = os.path.join(output_dir, district)
        else:
            out_folder = info['output_folder']

        safe_print(f"Output:     {out_folder}")

        # Generate URLs
        safe_print("\nGenerating URLs...")
        url_list = url_gen.generate(days=days)
        safe_print(f"URLs:       {len(url_list)}")

        # Filter existing
        filtered, skipped = filter_existing_files(url_list, out_folder)
        if skipped > 0:
            safe_print(f"Skipping:   {skipped} (exist)")
            safe_print(f"Remaining:  {len(filtered)}")

        if not filtered:
            safe_print("\nAll files exist!")
            continue

        # Authenticate
        safe_print("\nAuthenticating...")
        auth = NiagaraAuth(district)
        cookies = auth.login(headless=False, keep_driver=False)

        if not cookies:
            safe_print(f"{SYM_FAIL} Auth failed")
            logger.error("Authentication failed for %s", district)
            continue

        # Download
        safe_print(f"\nDownloading ({workers} workers)...")
        print_separator()

        progress = ProgressPrinter(show_every=max(1, len(filtered) // 100))

        with DownloadEngine(
            cookies=cookies,
            max_workers=workers,
            progress_callback=progress
        ) as engine:
            stats = engine.download_batch(filtered, out_folder)

        auth.close()
        stats.skipped = skipped

        print_separator()
        safe_print(f"\n{SYM_OK} {district}: {stats.summary()}")
        all_stats.append((district, stats))

    # Summary
    if len(all_stats) > 1:
        print_header("SUMMARY")
        total_ok = sum(s.success for _, s in all_stats)
        total_fail = sum(s.failed for _, s in all_stats)
        total_skip = sum(s.skipped for _, s in all_stats)

        for d, s in all_stats:
            safe_print(f"  {d:25s} OK:{s.success:4d} Fail:{s.failed:3d} Skip:{s.skipped:4d}")

        print_separator()
        safe_print(f"  {'TOTAL':25s} OK:{total_ok:4d} Fail:{total_fail:3d} Skip:{total_skip:4d}")

    return 0


# ============================================================================
# MAIN MENU
# ============================================================================
def main_menu() -> int:
    """Main menu."""
    while True:
        clear_screen()
        print_header(f"NIAGARA BAS CLI v{APP_VERSION}")

        safe_print("\n  DOWNLOAD:")
        safe_print("    1. Download trend data (interactive)")
        safe_print("    2. Quick download (specify district)")

        safe_print("\n  VPN & CREDENTIALS:")
        safe_print("    3. View VPN credentials")
        safe_print("    4. Check credential status")

        safe_print("\n  SETTINGS:")
        safe_print("    5. Configure output directory")
        safe_print("    6. Performance settings")

        safe_print("\n  INFO:")
        safe_print("    7. List all districts")

        safe_print("\n  q. Quit")

        choice = get_user_input("\n  Select: ")

        if choice is None or choice.lower() == 'q':
            safe_print("\n  Goodbye!")
            return 0

        if choice == '1':
            # Interactive download
            clear_screen()
            selected = select_district()
            if selected:
                if run_vpn_check_workflow(selected):
                    output_dir = select_output_directory()
                    if output_dir is None:
                        safe_print("\n  Cancelled.")
                        input("\n  Press ENTER...")
                        continue

                    days = get_user_input("\n  Days [90]: ", allow_empty=True)
                    days = int(days) if days else DEFAULT_DAYS

                    workers = get_user_input(f"\n  Workers [{DEFAULT_WORKERS}]: ", allow_empty=True)
                    workers = int(workers) if workers else DEFAULT_WORKERS

                    run_download_workflow(selected, days, output_dir, workers)

                    input("\n  Press ENTER...")

        elif choice == '2':
            # Quick download
            clear_screen()
            print_header("QUICK DOWNLOAD")
            district = get_user_input("  District: ")
            if district:
                days = get_user_input("  Days [90]: ", allow_empty=True)
                days = int(days) if days else DEFAULT_DAYS

                selected = [district.upper()]
                if run_vpn_check_workflow(selected):
                    output_dir = select_output_directory()
                    if output_dir:
                        run_download_workflow(selected, days, output_dir)

            input("\n  Press ENTER...")

        elif choice == '3':
            # View VPN credentials
            clear_screen()
            reload_env()
            districts = list_districts_with_status()
            district = get_user_input("\n  District: ")

            try:
                num = int(district)
                if 1 <= num <= len(districts):
                    district = districts[num - 1]
            except ValueError:
                district = district.upper() if district else ''

            if district in districts:
                vpn_info = get_vpn_info(district)
                print_header(f"VPN - {district}")
                display_vpn_credentials(vpn_info)
            else:
                safe_print(f"\n  '{district}' not found")

            input("\n  Press ENTER...")

        elif choice == '4':
            # Credential status
            clear_screen()
            cred_script = os.path.join(SCRIPT_DIR, 'credentials.py')
            if os.path.exists(cred_script):
                os.system(f'"{sys.executable}" "{cred_script}" --status')
            else:
                safe_print("  credentials.py not found")
            input("\n  Press ENTER...")

        elif choice == '5':
            # Output directory
            clear_screen()
            output_dir = select_output_directory()
            if output_dir:
                safe_print(f"\n  {SYM_OK} Set to: {output_dir}")
            input("\n  Press ENTER...")

        elif choice == '6':
            # Performance settings
            clear_screen()
            print_header("PERFORMANCE SETTINGS")
            safe_print(f"\n  Current defaults:")
            safe_print(f"    Workers:  {DEFAULT_WORKERS}")
            safe_print(f"    Days:     {DEFAULT_DAYS}")
            safe_print(f"    Throttle: {DEFAULT_THROTTLE}s")
            safe_print(f"\n  (Settings are per-session, set at download time)")
            input("\n  Press ENTER...")

        elif choice == '7':
            # List districts
            clear_screen()
            list_districts_with_status()
            input("\n  Press ENTER...")

        else:
            safe_print(f"\n  Invalid: {choice}")
            time.sleep(1)

    return 0


# ============================================================================
# ENTRY POINT
# ============================================================================
def main() -> int:
    """Main entry point."""
    try:
        if len(sys.argv) > 1:
            # Pass to fast download script
            from download_niagara_fast import main as fast_main
            return fast_main()

        return main_menu()

    except KeyboardInterrupt:
        safe_print("\n\n  Cancelled.")
        return 1
    except Exception as e:
        safe_print(f"\n  Error: {e}")
        logger.exception("Unhandled exception in CLI")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
