"""
================================================================================
BUILD SCRIPT - Package Niagara BAS Downloader as Windows .exe v2.0
================================================================================
Creates a standalone Windows executable using PyInstaller.

PREREQUISITES:
    pip install pyinstaller customtkinter requests python-dateutil
    pip install python-dotenv selenium urllib3

USAGE:
    python build_exe.py

This will create:
    dist/NiagaraBAS/NiagaraBAS.exe   (main executable)

Copy the entire dist/NiagaraBAS folder to the target machine.
================================================================================
"""

import os
import sys
import shutil
import subprocess

# ============================================================================
# CONFIGURATION
# ============================================================================
APP_NAME = "NiagaraBAS"
MAIN_SCRIPT = "niagara_gui.py"
ICON_FILE = None  # Set to "app.ico" if you have an icon file

# Files to include alongside the exe
DATA_FILES = [
    "config_district_details.py",
    "credentials.py",
    "niagara_auth.py",
    "niagara_cli.py",
    "niagara_download_engine.py",
    "niagara_url_generator.py",
    "download_niagara_fast.py",
    "fetch_pointlist.py",
    "utils.py",
    "logging_config.py",
    "requirements.txt",
]

# Directories to include
DATA_DIRS = [
    "point_lists",   # Point list files
    "drivers",       # geckodriver, etc.
]

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def check_prerequisites():
    """Verify required packages are installed."""
    required = ['PyInstaller', 'customtkinter']
    missing = []

    for pkg in required:
        try:
            __import__(pkg.lower().replace('-', '_'))
        except ImportError:
            # Special case for PyInstaller
            if pkg == 'PyInstaller':
                try:
                    __import__('PyInstaller')
                except ImportError:
                    missing.append(pkg)
            else:
                missing.append(pkg)

    if missing:
        print(f"Missing packages: {', '.join(missing)}")
        print(f"Install with: pip install {' '.join(missing)}")
        return False
    return True


def find_customtkinter_path():
    """Find customtkinter installation path for PyInstaller."""
    try:
        import customtkinter
        return os.path.dirname(customtkinter.__file__)
    except ImportError:
        return None


def build():
    """Build the .exe using PyInstaller."""
    print("=" * 60)
    print(f" Building {APP_NAME} v2.0")
    print("=" * 60)

    # Check prerequisites
    if not check_prerequisites():
        print("\nInstall missing packages and try again.")
        sys.exit(1)

    # Find customtkinter path
    ctk_path = find_customtkinter_path()
    if not ctk_path:
        print("ERROR: customtkinter not found")
        sys.exit(1)
    print(f"CustomTkinter path: {ctk_path}")

    # Check main script exists
    main_path = os.path.join(SCRIPT_DIR, MAIN_SCRIPT)
    if not os.path.exists(main_path):
        print(f"ERROR: {MAIN_SCRIPT} not found in {SCRIPT_DIR}")
        sys.exit(1)

    # Build PyInstaller command
    cmd = [
        sys.executable, '-m', 'PyInstaller',
        '--name', APP_NAME,
        '--windowed',               # No console window
        '--noconfirm',              # Overwrite without asking
        '--clean',                  # Clean cache

        # CustomTkinter requires its assets
        '--add-data', f'{ctk_path};customtkinter/',

        # Hidden imports that PyInstaller might miss
        '--hidden-import', 'customtkinter',
        '--hidden-import', 'requests',
        '--hidden-import', 'urllib3',
        '--hidden-import', 'dateutil',
        '--hidden-import', 'dateutil.relativedelta',
        '--hidden-import', 'dotenv',
        '--hidden-import', 'selenium',
        '--hidden-import', 'selenium.webdriver',
        '--hidden-import', 'selenium.webdriver.firefox',
        '--hidden-import', 'selenium.webdriver.firefox.webdriver',
        '--hidden-import', 'selenium.webdriver.firefox.service',
        '--hidden-import', 'selenium.webdriver.firefox.options',
        '--hidden-import', 'selenium.webdriver.common.keys',
        '--collect-submodules', 'selenium',
        '--hidden-import', 'config_district_details',
        '--hidden-import', 'credentials',
        '--hidden-import', 'niagara_auth',
        '--hidden-import', 'niagara_download_engine',
        '--hidden-import', 'niagara_url_generator',
        '--hidden-import', 'niagara_cli',
        '--hidden-import', 'download_niagara_fast',
        '--hidden-import', 'fetch_pointlist',
        '--hidden-import', 'utils',
        '--hidden-import', 'logging_config',

        # Collect all submodules
        '--collect-all', 'customtkinter',
    ]

    # Add data files
    for f in DATA_FILES:
        fpath = os.path.join(SCRIPT_DIR, f)
        if os.path.exists(fpath):
            cmd.extend(['--add-data', f'{fpath};.'])
            print(f"  Including: {f}")
        else:
            print(f"  Skipping (not found): {f}")

    # Add data directories
    for d in DATA_DIRS:
        dpath = os.path.join(SCRIPT_DIR, d)
        if os.path.exists(dpath):
            cmd.extend(['--add-data', f'{dpath};{d}/'])
            print(f"  Including dir: {d}/")

    # Add icon if available
    if ICON_FILE and os.path.exists(os.path.join(SCRIPT_DIR, ICON_FILE)):
        cmd.extend(['--icon', os.path.join(SCRIPT_DIR, ICON_FILE)])

    # Main script
    cmd.append(main_path)

    print(f"\nRunning PyInstaller...")
    print(f"Command: {' '.join(cmd[:5])} ...")
    print()

    result = subprocess.run(cmd, cwd=SCRIPT_DIR)

    if result.returncode != 0:
        print("\nBuild FAILED!")
        sys.exit(1)

    # Post-build: copy .env template and supporting files
    dist_dir = os.path.join(SCRIPT_DIR, 'dist', APP_NAME)
    if os.path.exists(dist_dir):
        # Copy .env.template
        env_template = os.path.join(SCRIPT_DIR, '.env.template')
        if os.path.exists(env_template):
            shutil.copy2(env_template, os.path.join(dist_dir, '.env.template'))
            print(f"Copied .env.template to dist/")

        # Copy .env if it exists (user's actual config)
        env_file = os.path.join(SCRIPT_DIR, '.env')
        if os.path.exists(env_file):
            shutil.copy2(env_file, os.path.join(dist_dir, '.env'))
            print(f"Copied .env to dist/")

        # Ensure point_lists directory exists
        pl_dir = os.path.join(dist_dir, 'point_lists')
        os.makedirs(pl_dir, exist_ok=True)

        # Copy point lists if they exist
        src_pl = os.path.join(SCRIPT_DIR, 'point_lists')
        if os.path.exists(src_pl):
            for f in os.listdir(src_pl):
                src = os.path.join(src_pl, f)
                if os.path.isfile(src):
                    shutil.copy2(src, os.path.join(pl_dir, f))
            print(f"Copied point_lists/")

        # Ensure drivers directory exists
        drv_dir = os.path.join(dist_dir, 'drivers')
        os.makedirs(drv_dir, exist_ok=True)
        src_drv = os.path.join(SCRIPT_DIR, 'drivers')
        if os.path.exists(src_drv):
            for f in os.listdir(src_drv):
                src = os.path.join(src_drv, f)
                if os.path.isfile(src):
                    shutil.copy2(src, os.path.join(drv_dir, f))
            print(f"Copied drivers/")

    print()
    print("=" * 60)
    print(f" BUILD COMPLETE!")
    print("=" * 60)
    print(f"\n  Output: {dist_dir}")
    print(f"  Executable: {os.path.join(dist_dir, APP_NAME + '.exe')}")
    print()
    print("  DEPLOYMENT STEPS:")
    print("  1. Copy the entire 'dist/NiagaraBAS' folder to the target PC")
    print("  2. Copy .env.template to .env and fill in your credentials")
    print("  3. Ensure point_lists/ folder has your point list CSV files")
    print("  4. Ensure drivers/ folder has geckodriver.exe")
    print("  5. Ensure Firefox is installed on the target PC")
    print("  6. Run NiagaraBAS.exe")
    print()


if __name__ == '__main__':
    build()
