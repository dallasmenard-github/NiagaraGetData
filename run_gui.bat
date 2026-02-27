@echo off
REM ============================================================================
REM Niagara BAS Data Download - GUI Launcher v2.0
REM ============================================================================

cd /d "%~dp0"
if exist .venv\Scripts\activate.bat (
    call .venv\Scripts\activate.bat
    start "" .venv\Scripts\pythonw.exe niagara_gui.py
) else (
    start "" pythonw.exe niagara_gui.py
)
