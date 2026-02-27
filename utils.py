"""
================================================================================
NIAGARA BAS UTILITIES v2.0
================================================================================
Shared utility functions used across the Niagara BAS suite.

Consolidates duplicated code from multiple modules into a single source.
================================================================================
"""

import sys
from typing import Any

# ============================================================================
# VERSION
# ============================================================================
APP_VERSION = "2.0"

# ============================================================================
# ASCII-SAFE SYMBOLS (for Windows console compatibility)
# ============================================================================
SYM_CHECK = "Y"
SYM_EMPTY = "N"
SYM_FAIL = "[X]"
SYM_OK = "[OK]"
SYM_WARN = "[!]"
SYM_BULLET = "*"


# ============================================================================
# CONSOLE
# ============================================================================
def setup_console_encoding() -> None:
    """Configure console for proper encoding on Windows."""
    if sys.platform == 'win32':
        try:
            if hasattr(sys.stdout, 'reconfigure'):
                sys.stdout.reconfigure(encoding='utf-8', errors='replace')
                sys.stderr.reconfigure(encoding='utf-8', errors='replace')
        except Exception:
            pass


def safe_print(text: Any, **kwargs) -> None:
    """Print with fallback for encoding errors."""
    try:
        print(text, **kwargs)
    except UnicodeEncodeError:
        if isinstance(text, str):
            print(text.encode('ascii', errors='replace').decode('ascii'), **kwargs)
        else:
            print(str(text), **kwargs)


def print_header(title: str, width: int = 70) -> None:
    """Print formatted header."""
    safe_print("\n" + "=" * width)
    safe_print(f" {title}")
    safe_print("=" * width)


def print_separator(char: str = "-", width: int = 70) -> None:
    """Print separator line."""
    safe_print(char * width)


# ============================================================================
# FILENAME UTILITIES
# ============================================================================
def standardize_filename(point_path: str) -> str:
    """
    Convert point path to a standardized filename.

    Replaces path separators and strips illegal characters for Windows.

    Args:
        point_path: Niagara point path (e.g., '/Building/RTU-Temp')

    Returns:
        Sanitized filename (e.g., 'Building_RTU-Temp')
    """
    filename = point_path.replace('/', '_').strip('_')
    for char in '<>:"|?*':
        filename = filename.replace(char, '_')
    return filename
