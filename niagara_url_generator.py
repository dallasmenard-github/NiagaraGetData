"""
================================================================================
NIAGARA URL GENERATOR v2.0
================================================================================
Handles point list loading and URL construction for Niagara BAS systems.

Features:
    - Point list discovery (config + local fallback)
    - URL generation with date ranges
    - Point list validation

USAGE:
    from niagara_url_generator import URLGenerator

    gen = URLGenerator('WINDHAMSCHOOLSNH')
    urls = gen.generate(days=30)
================================================================================
"""

import os
from datetime import datetime
from pathlib import Path
from dateutil.relativedelta import relativedelta
from typing import List, Tuple, Optional, Union

from config_district_details import district_config
from logging_config import get_logger

logger = get_logger("url_generator")

# ============================================================================
# CONFIGURATION
# ============================================================================
SCRIPT_DIR = Path(__file__).parent
POINT_LISTS_DIR = SCRIPT_DIR / "point_lists"
POINT_LIST_PREFIX = "pointlist_"


def get_point_list_path(district_name: str) -> Tuple[Optional[str], str]:
    """
    Find point list file for a district.

    Checks in order:
    1. Config TREND_POINT_LIST path
    2. Local point_lists/pointlist_{DISTRICT}.txt

    Args:
        district_name: District name (case insensitive)

    Returns:
        Tuple of (path_if_exists, source) where source is 'config', 'local', or 'none'
    """
    district = district_name.upper()
    config = district_config.get(district, {})

    # Check config path
    config_path = config.get('TREND_POINT_LIST', '')
    if config_path and Path(config_path).exists():
        return config_path, 'config'

    # Check local path
    local_path = POINT_LISTS_DIR / f"{POINT_LIST_PREFIX}{district}.txt"
    if local_path.exists():
        return str(local_path), 'local'

    return None, 'none'


def load_point_list(filepath: str) -> List[str]:
    """
    Load points from a point list file.

    Args:
        filepath: Path to point list file

    Returns:
        List of point paths
    """
    points: List[str] = []

    with open(filepath, 'r', encoding='utf-8-sig') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue

            # Handle CSV format (first column is point path)
            point_path = line.split(',')[0].strip()
            point_path = point_path.replace('"', '').replace('\n', '')

            if point_path:
                points.append(point_path)

    return points


def format_datetime(dt: Union[datetime, str], tz_offset: str = '-04:00') -> str:
    """
    Format datetime for Niagara URL.

    Args:
        dt: datetime object or string 'YYYY-MM-DD'
        tz_offset: Timezone offset string

    Returns:
        Formatted datetime string
    """
    if isinstance(dt, str):
        dt = datetime.strptime(dt, '%Y-%m-%d')

    return dt.replace(minute=0, hour=0, second=0).strftime(
        f'%Y-%m-%dT%H:%M:%S.000{tz_offset}'
    )


class URLGenerator:
    """Generate download URLs for Niagara BAS trend data."""

    def __init__(self, district_name: str) -> None:
        self.district = district_name.upper()
        self.config = district_config.get(self.district)

        if not self.config:
            raise ValueError(f"District '{self.district}' not found in config")

        self.base_ip = self.config.get('BASE_IP', '')
        if not self.base_ip or self.base_ip.lower() in ('na', 'n/a', ''):
            raise ValueError(f"No BASE_IP configured for {self.district}")

        self.point_list_path, self.point_list_source = get_point_list_path(self.district)
        self.points: List[str] = []

        if self.point_list_path:
            self.points = load_point_list(self.point_list_path)

    @property
    def has_point_list(self) -> bool:
        """Check if point list is available."""
        return self.point_list_path is not None and len(self.points) > 0

    @property
    def point_count(self) -> int:
        """Number of points in list."""
        return len(self.points)

    @property
    def output_folder(self) -> str:
        """Get configured output folder for this district."""
        return self.config.get(
            'FOLDER_LOCATION_TREND_DATA',
            str(SCRIPT_DIR / 'output' / self.district)
        )

    def _build_url(self, point_path: str, start_time: str, end_time: str) -> str:
        """Build download URL for a single point."""
        return (
            f'{self.base_ip}/ord?history:{point_path}'
            f'?period=timeRange;start={start_time};end={end_time}'
            f'|bql:select%20timestamp,value|view:file:ITableToCsv'
        )

    def generate(
        self,
        days: Optional[int] = None,
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
        tz_offset: str = '-04:00'
    ) -> List[Tuple[str, str]]:
        """
        Generate list of (point_path, url) tuples.

        Args:
            days: Number of days from today
            start_date: Start date (YYYY-MM-DD string or datetime)
            end_date: End date (YYYY-MM-DD string or datetime)
            tz_offset: Timezone offset for URL

        Returns:
            List of (point_path, url) tuples
        """
        if not self.has_point_list:
            raise ValueError(
                f"No point list found for {self.district}. "
                f"Run: python fetch_pointlist.py --district {self.district}"
            )

        if days is not None:
            end_dt = datetime.today()
            start_dt = end_dt - relativedelta(days=days)
        elif start_date and end_date:
            start_dt = start_date if isinstance(start_date, datetime) else datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = end_date if isinstance(end_date, datetime) else datetime.strptime(end_date, '%Y-%m-%d')
        else:
            raise ValueError("Must specify either 'days' or both 'start_date' and 'end_date'")

        start_time = format_datetime(start_dt, tz_offset)
        end_time = format_datetime(end_dt, tz_offset)

        urls: List[Tuple[str, str]] = []
        for point_path in self.points:
            url = self._build_url(point_path, start_time, end_time)
            urls.append((point_path, url))

        return urls

    def get_point_list_url(self) -> str:
        """Get URL to fetch point list from Niagara."""
        return f"{self.base_ip.rstrip('/')}/ord?history:|bql:select%20id|view:file:ITableToCsv"

    def info(self) -> dict:
        """Get information about this generator."""
        return {
            'district': self.district,
            'base_ip': self.base_ip,
            'point_list_path': self.point_list_path,
            'point_list_source': self.point_list_source,
            'point_count': self.point_count,
            'output_folder': self.output_folder,
        }


def get_available_districts() -> List[str]:
    """Get list of all configured districts."""
    return sorted(district_config.keys())


def get_districts_with_pointlists() -> List[str]:
    """Get list of districts that have point lists available."""
    districts: List[str] = []
    for district in get_available_districts():
        path, source = get_point_list_path(district)
        if path:
            districts.append(district)
    return districts


def get_districts_missing_pointlists() -> List[Tuple[str, bool]]:
    """
    Get list of districts missing point lists.

    Returns:
        List of (district_name, has_base_ip) tuples
    """
    missing: List[Tuple[str, bool]] = []
    for district in get_available_districts():
        path, source = get_point_list_path(district)
        if not path:
            config = district_config.get(district, {})
            base_ip = config.get('BASE_IP', '')
            has_ip = bool(base_ip and base_ip.lower() not in ('na', 'n/a', ''))
            missing.append((district, has_ip))
    return missing


# ============================================================================
# CLI
# ============================================================================
if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='URL Generator for Niagara BAS v2.0')
    parser.add_argument('--district', type=str, help='District name')
    parser.add_argument('--list', action='store_true', help='List all districts')
    parser.add_argument('--info', action='store_true', help='Show district info')
    parser.add_argument('--count', type=str, help='Count points for district')

    args = parser.parse_args()

    if args.list:
        print("\nDistricts with point lists:")
        for d in get_districts_with_pointlists():
            path, _ = get_point_list_path(d)
            points = load_point_list(path) if path else []
            print(f"  {d}: {len(points)} points")

        print("\nDistricts missing point lists:")
        for d, has_ip in get_districts_missing_pointlists():
            ip_status = "has IP" if has_ip else "NO IP"
            print(f"  {d} ({ip_status})")

    elif args.info and args.district:
        try:
            gen = URLGenerator(args.district)
            info = gen.info()
            print(f"\nDistrict: {info['district']}")
            print(f"Base IP: {info['base_ip']}")
            print(f"Point List: {info['point_list_path']} ({info['point_list_source']})")
            print(f"Points: {info['point_count']}")
            print(f"Output: {info['output_folder']}")
        except ValueError as e:
            print(f"Error: {e}")

    elif args.count:
        path, source = get_point_list_path(args.count)
        if path:
            points = load_point_list(path)
            print(f"{args.count.upper()}: {len(points)} points ({source})")
        else:
            print(f"{args.count.upper()}: No point list found")

    else:
        print("Use --list to see districts or --district NAME --info for details")
