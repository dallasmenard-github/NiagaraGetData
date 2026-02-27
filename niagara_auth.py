"""
================================================================================
NIAGARA AUTHENTICATION v2.0
================================================================================
Handles authentication to Niagara BAS systems via Selenium or direct cookies.

Features:
    - Selenium-based login with Firefox
    - Cookie extraction from authenticated session
    - Session validation
    - Headless mode support

USAGE:
    from niagara_auth import NiagaraAuth

    auth = NiagaraAuth('WINDHAMSCHOOLSNH')
    cookies = auth.login()
================================================================================
"""

import os
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from config_district_details import district_config
from credentials import get_district_credentials
from logging_config import get_logger

logger = get_logger("auth")

# Driver paths
SCRIPT_DIR = Path(__file__).parent
DRIVERS_DIR = SCRIPT_DIR / "drivers"
GECKODRIVER_PATH = DRIVERS_DIR / "geckodriver.exe"
FIREFOX_BINARY = Path('C:/Program Files/Mozilla Firefox/firefox.exe')


class NiagaraAuth:
    """Authentication handler for Niagara BAS systems."""

    def __init__(self, district_name: str) -> None:
        self.district = district_name.upper()
        self.config = district_config.get(self.district, {})
        self.base_ip = self.config.get('BASE_IP', '')
        self.username, self.password = get_district_credentials(self.district)
        self._driver = None

    @property
    def has_credentials(self) -> bool:
        """Check if credentials are available."""
        return bool(self.username and self.password)

    @property
    def has_base_ip(self) -> bool:
        """Check if BASE_IP is configured."""
        return bool(self.base_ip and self.base_ip.lower() not in ('na', 'n/a', ''))

    @property
    def driver(self):
        """Get the Selenium driver (if active)."""
        return self._driver

    def login(self, headless: bool = False, keep_driver: bool = False) -> Optional[Dict[str, str]]:
        """
        Login to Niagara using Selenium and extract session cookies.

        Args:
            headless: Run browser in headless mode
            keep_driver: Keep driver open for page toggling

        Returns:
            Dictionary of cookies or None on failure
        """
        if not self.has_credentials:
            logger.error("No credentials configured for %s", self.district)
            return None

        if not self.has_base_ip:
            logger.error("No BASE_IP configured for %s", self.district)
            return None

        try:
            from selenium import webdriver
            from selenium.webdriver.firefox.service import Service
            from selenium.webdriver.firefox.options import Options
            from selenium.webdriver.common.keys import Keys
        except ImportError:
            logger.error("Selenium not installed. Run: pip install selenium")
            return None

        if not GECKODRIVER_PATH.exists():
            logger.error("geckodriver not found at %s", GECKODRIVER_PATH)
            return None

        logger.info("Logging into %s...", self.district)
        logger.info("  Base IP: %s", self.base_ip)
        logger.info("  Username: %s", self.username)

        # Configure Firefox
        firefox_options = Options()
        firefox_options.add_argument('--ignore-certificate-errors')
        firefox_options.accept_insecure_certs = True

        if headless:
            firefox_options.add_argument('--headless')

        if FIREFOX_BINARY.exists():
            firefox_options.binary_location = str(FIREFOX_BINARY)

        service = Service(executable_path=str(GECKODRIVER_PATH))
        driver = webdriver.Firefox(service=service, options=firefox_options)

        try:
            logger.info("  Navigating to login page...")
            driver.get(self.base_ip)
            time.sleep(5)

            logger.info("  Entering credentials...")
            username_field = driver.switch_to.active_element
            username_field.send_keys(self.username)
            username_field.send_keys(Keys.RETURN)
            time.sleep(3)

            password_field = driver.switch_to.active_element
            password_field.send_keys(self.password)
            password_field.send_keys(Keys.RETURN)
            time.sleep(5)

            # Extract cookies
            cookies: Dict[str, str] = {}
            for cookie in driver.get_cookies():
                if cookie['name'] in ['niagara_session', 'JSESSIONID']:
                    cookies[cookie['name']] = cookie['value']
                    logger.info("  Session cookie obtained: %s", cookie['name'])

            if not cookies:
                logger.warning("  No session cookie found, using all cookies")
                for cookie in driver.get_cookies():
                    cookies[cookie['name']] = cookie['value']

            if keep_driver:
                self._driver = driver
                logger.info("  Driver kept open for session management")
            else:
                driver.quit()

            logger.info("  Login successful!")
            return cookies

        except Exception as e:
            logger.error("  Login error: %s", e)
            driver.quit()
            return None

    def login_with_cookie(self, cookie_value: str) -> Dict[str, str]:
        """
        Use an existing session cookie instead of logging in.

        Args:
            cookie_value: Cookie string (JSESSIONID=xxx or just xxx)

        Returns:
            Dictionary of cookies
        """
        if '=' in cookie_value:
            name, value = cookie_value.split('=', 1)
            return {name: value}
        else:
            return {'JSESSIONID': cookie_value}

    def validate_session(self, cookies: Dict[str, str]) -> bool:
        """
        Validate that cookies provide a valid session.

        Args:
            cookies: Dictionary of cookies to test

        Returns:
            True if session is valid
        """
        import requests
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        if not self.has_base_ip:
            return False

        try:
            response = requests.get(
                self.base_ip,
                cookies=cookies,
                timeout=10,
                verify=False,
                allow_redirects=False
            )

            if response.status_code in (301, 302, 303):
                location = response.headers.get('Location', '').lower()
                if 'login' in location:
                    return False

            content = response.text.lower()
            if 'login' in content and 'password' in content:
                return False

            return response.status_code == 200

        except Exception:
            return False

    def toggle_page(self, page_num: int = 1) -> None:
        """Toggle browser page to prevent session timeout."""
        if not self._driver:
            return

        page_one = self.config.get('PAGE_TOGGLE_ONE', self.base_ip)
        page_two = self.config.get('PAGE_TOGGLE_TWO', self.base_ip)

        try:
            url = page_one if page_num == 1 else page_two
            self._driver.get(url)
            time.sleep(1)
        except Exception as e:
            logger.warning("Page toggle failed: %s", e)

    def close(self) -> None:
        """Close the browser driver if open."""
        if self._driver:
            self._driver.quit()
            self._driver = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def list_districts_with_credentials() -> List[str]:
    """Get list of districts that have credentials configured."""
    districts = []
    for district in sorted(district_config.keys()):
        username, password = get_district_credentials(district)
        if username and password:
            districts.append(district)
    return districts


# ============================================================================
# CLI
# ============================================================================
if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Niagara Authentication v2.0')
    parser.add_argument('--district', type=str, help='District to authenticate')
    parser.add_argument('--list', action='store_true', help='List districts with credentials')
    parser.add_argument('--validate', type=str, help='Validate cookie for district')
    parser.add_argument('--cookie', type=str, help='Cookie value to validate')

    args = parser.parse_args()

    if args.list:
        print("\nDistricts with credentials:")
        for d in list_districts_with_credentials():
            print(f"  {d}")

    elif args.validate and args.cookie:
        auth = NiagaraAuth(args.validate)
        cookies = auth.login_with_cookie(args.cookie)
        valid = auth.validate_session(cookies)
        print(f"Session valid: {valid}")

    elif args.district:
        auth = NiagaraAuth(args.district)
        print(f"\nDistrict: {auth.district}")
        print(f"Base IP: {auth.base_ip}")
        print(f"Has credentials: {auth.has_credentials}")
        print(f"Username: {auth.username}")

    else:
        print("Use --district NAME to check credentials or --list to see all")
