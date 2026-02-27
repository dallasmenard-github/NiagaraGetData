"""
================================================================================
CREDENTIALS LOADER v2.0
================================================================================
Loads credentials exclusively from .env file or environment variables.

SETUP:
    1. Copy .env.template to .env
    2. Fill in your credentials
    3. Add .env to .gitignore

USAGE:
    from credentials import get_district_credentials

    username, password = get_district_credentials('WINDHAMSCHOOLSNH')
================================================================================
"""

import os
import sys
from pathlib import Path
from typing import Tuple, Optional, List

from logging_config import get_logger

logger = get_logger("credentials")

# Try to load python-dotenv if available
try:
    from dotenv import load_dotenv
    DOTENV_AVAILABLE = True
except ImportError:
    DOTENV_AVAILABLE = False


def _find_env_file() -> Optional[Path]:
    """
    Search for .env file in common locations.

    Returns:
        Path to .env file or None if not found
    """
    search_paths = [
        Path(__file__).parent / '.env',
        Path.cwd() / '.env',
        Path.home() / '.env',
    ]

    for path in search_paths:
        if path.exists():
            return path

    return None


def _load_env_file():
    """Load .env file if python-dotenv is available."""
    if not DOTENV_AVAILABLE:
        return

    env_path = _find_env_file()
    if env_path:
        load_dotenv(env_path)
        logger.debug("Loaded .env from %s", env_path)


# Load .env on module import
_load_env_file()


def get_credential(key: str, fallback: str = '') -> str:
    """
    Get a credential value from environment.

    Args:
        key: Environment variable name
        fallback: Value to return if not found

    Returns:
        Credential value
    """
    return os.environ.get(key, fallback)


def get_district_credentials(district: str) -> Tuple[str, str]:
    """
    Get username and password for a district.

    Looks for environment variables in format:
        {DISTRICT}_USER and {DISTRICT}_PASS

    Args:
        district: District name (e.g., 'WINDHAMSCHOOLSNH')

    Returns:
        Tuple of (username, password)
    """
    district = district.upper()

    # Try district-specific environment variables
    env_user = os.environ.get(f'{district}_USER')
    env_pass = os.environ.get(f'{district}_PASS')

    if env_user and env_pass:
        return env_user, env_pass

    # Try generic Niagara credentials
    generic_user = os.environ.get('NIAGARA_USER')
    generic_pass = os.environ.get('NIAGARA_PASS')

    if generic_user and generic_pass:
        return generic_user, generic_pass

    # No credentials found - .env is the only source in v2.0
    return '', ''


def get_vpn_credentials(district: str) -> Tuple[str, str]:
    """
    Get VPN credentials for a district if different from Niagara credentials.

    Args:
        district: District name

    Returns:
        Tuple of (username, password)
    """
    district = district.upper()

    # Try district-specific VPN credentials
    vpn_user = os.environ.get(f'{district}_VPN_USER')
    vpn_pass = os.environ.get(f'{district}_VPN_PASS')

    if vpn_user and vpn_pass:
        return vpn_user, vpn_pass

    # Fall back to generic VPN credentials
    vpn_user = os.environ.get('VPN_USER')
    vpn_pass = os.environ.get('VPN_PASS')

    if vpn_user and vpn_pass:
        return vpn_user, vpn_pass

    return '', ''


def validate_credentials(district: str) -> bool:
    """
    Check if credentials are configured for a district.

    Args:
        district: District name

    Returns:
        True if credentials are available
    """
    username, password = get_district_credentials(district)
    return bool(username and password)


def list_configured_districts() -> List[str]:
    """
    List districts that have credentials configured in environment.

    Returns:
        List of district names with credentials
    """
    configured = []

    for key in os.environ:
        if key.endswith('_USER'):
            district = key[:-5]
            if os.environ.get(f'{district}_PASS'):
                configured.append(district)

    return sorted(set(configured))


def get_district_config(district: str) -> dict:
    """
    Get full configuration for a district from config_district_details.py.

    Args:
        district: District name

    Returns:
        Configuration dictionary or empty dict if not found
    """
    try:
        from config_district_details import district_config
        return district_config.get(district.upper(), {})
    except ImportError:
        return {}


def get_all_districts() -> List[str]:
    """
    Get list of all configured districts from config_district_details.py.

    Returns:
        List of district names
    """
    try:
        from config_district_details import district_config
        return sorted(district_config.keys())
    except ImportError:
        return []


# ============================================================================
# CREDENTIAL MIGRATION HELPER
# ============================================================================
def generate_env_template(output_path: Optional[str] = None) -> None:
    """
    Generate .env template from config_district_details.py.

    Args:
        output_path: Where to save template (default: .env.template)
    """
    if output_path is None:
        output_path = str(Path(__file__).parent / '.env.template')

    try:
        from config_district_details import district_config
    except ImportError:
        print("Could not import config_district_details.py")
        return

    lines = [
        "# ============================================================================",
        "# Niagara BAS Credentials - v2.0",
        "# ============================================================================",
        "# Copy this file to .env and fill in your credentials",
        "# NEVER commit .env to version control",
        "# ============================================================================",
        "",
        "# Output directory",
        "# OUTPUT_DIR=",
        "",
        "# ============================================================================",
        "# District-specific credentials",
        "# ============================================================================",
    ]

    for district in sorted(district_config.keys()):
        config = district_config[district]
        base_ip = config.get('BASE_IP', '')
        lines.append("")
        lines.append(f"# {district}")
        if base_ip:
            lines.append(f"# BASE_IP: {base_ip}")
        lines.append(f"{district}_USER=")
        lines.append(f"{district}_PASS=")

        # Add VPN entries if VPN type is configured
        vpn_data = config.get('VPN_DATA', 'na')
        if vpn_data and vpn_data.lower() not in ('na', 'n/a', ''):
            lines.append(f"{district}_VPN_TYPE={vpn_data}")
            lines.append(f"{district}_VPN_GATEWAY=")
            lines.append(f"{district}_VPN_USER=")
            lines.append(f"{district}_VPN_PASS=")

    with open(output_path, 'w') as f:
        f.write('\n'.join(lines))

    print(f"Template saved to: {output_path}")


def print_credential_status() -> None:
    """Print status of credentials for all districts."""
    print("\n" + "=" * 60)
    print("CREDENTIAL STATUS")
    print("=" * 60)
    print(f"{'District':<30} {'Env':<8} {'Status':<10}")
    print("-" * 60)

    for district in get_all_districts():
        env_user = os.environ.get(f'{district}_USER')
        env_pass = os.environ.get(f'{district}_PASS')
        has_env = bool(env_user and env_pass)

        status = "Y ENV" if has_env else "N None"
        env_sym = "Y" if has_env else "N"
        print(f"{district:<30} {env_sym:<8} {status:<10}")

    print("=" * 60)
    print("Y = configured, N = not configured")
    print("=" * 60 + "\n")


if __name__ == '__main__':
    if len(sys.argv) > 1:
        if sys.argv[1] == '--generate':
            generate_env_template()
        elif sys.argv[1] == '--status':
            print_credential_status()
        elif sys.argv[1] == '--help':
            print("Credential Loader v2.0")
            print("=" * 40)
            print("Options:")
            print("  --generate  Create .env.template")
            print("  --status    Show credential status for all districts")
            print("  --help      Show this help message")
        else:
            print(f"Unknown option: {sys.argv[1]}")
            print("Use --help for available options")
    else:
        print("Credential Loader v2.0")
        print("=" * 40)
        print(f"dotenv available: {DOTENV_AVAILABLE}")
        print(f".env file found:  {_find_env_file()}")
        print(f"Env credentials:  {list_configured_districts()}")
        print(f"Total districts:  {len(get_all_districts())}")
        print("")
        print("Run with --generate to create .env.template")
        print("Run with --status to see credential status for all districts")
