# Windows Service

## Installation Instructions

### Prerequisites

* Python 3.8.3 32bit for ALL user + add to path

### Uncompiled Service Install/Uninstall

```bash
copy "C:\Python38-32\lib\site-packages\pywin32_system32\pywintypes38.dll"  "C:\Python38-32\lib\site-packages\win32"


python service.py install
python service.py update
python service.py remove

python service.py start
```

Running interactive

```commandline
python service.py debug

```

### Service troubleshooting

#### Error starting service: The service did not respond to the start or control request in a timely fashion.

* https://stackoverflow.com/questions/41200068/python-windows-service-error-starting-service-the-service-did-not-respond-to-t

#### Error removing service: The specified service has been marked for deletion. (1072)

```commandline
sc queryex data_agent

```


## Building Executable with pyinstaller

You need to copy `c:\Python38-32\Lib\site-packages\pywin32_system32\pywintypes38.dll` to `c:\Windows\System32`
(https://github.com/fkie-cad/RoAMer/issues/5)

```commandline
pyinstaller --distpath .\dist --workpath .\build --hiddenimport win32timezone -F src/data_agent/win32/service.py
```

After SPEC file is creates you can do:

```commandline
pyinstaller dagent-svc.spec --clean
```
