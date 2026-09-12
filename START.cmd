@echo off
setlocal
cd /d "%~dp0"
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8
if not defined CR_CRAWLER_PYTHON set "CR_CRAWLER_PYTHON=python"
if exist ".venv\Scripts\python.exe" set "CR_CRAWLER_PYTHON=%~dp0.venv\Scripts\python.exe"
"%CR_CRAWLER_PYTHON%" -m crawler.main %*
exit /b %ERRORLEVEL%
