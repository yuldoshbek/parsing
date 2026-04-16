@echo off
cd /d %~dp0
echo [WB Parser v5.0] Starting...
python main.py > run_out.txt 2> run_err.txt
echo Done. Check run_err.txt for logs.
pause
