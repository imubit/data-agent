@echo off
pyinstaller --distpath .\dist --workpath .\build --hiddenimport win32timezone -p .\src\data_agent -n imagent --additional-hooks-dir=hooks -F src/data_agent/main.py
