@echo off
pyinstaller --distpath \dist --workpath \build --hiddenimport win32timezone -p \src\data_agent -n dagent-svc -F src/data_agent/win32/service.py
