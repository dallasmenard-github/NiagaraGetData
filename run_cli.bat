@echo off
REM ============================================================================
REM Niagara BAS Data Download - CLI Launcher v2.0
REM ============================================================================

cd /d "%~dp0"
if exist .venv\Scripts\activate.bat (
    call .venv\Scripts\activate.bat
    .venv\Scripts\python.exe niagara_cli.py %*
) else (
    python niagara_cli.py %*
)
pause
