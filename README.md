# data-agent

Add a short description here!


## Description

A longer description of your project goes here...

## Installation

### Python install

* Python 3.8.3 32bit for ALL user + add to path


### Command Line Start

```bash
imagent --service.id my_service --broker.uri amqp://guest:guest@192.168.4.23/
```



### Service Building Executable with pyinstaller

Copy `c:\Python38-32\Lib\site-packages\pywin32_system32\pywintypes38.dll` to `c:\Windows\System32`
(https://github.com/fkie-cad/RoAMer/issues/5)


```bash
pyinstaller imagent-svc.spec --clean
```

THis will recreate the `spec` file
```bash
pyinstaller --distpath .\dist --workpath .\build --hiddenimport win32timezone -F src/data_agent/win32/service.py
```

### Uncompiled Service Install/Uninstall

Installing

```bash
copy "C:\Python38-32\lib\site-packages\pywin32_system32\pywintypes38.dll"  "C:\Python38-32\lib\site-packages\win32"


python service.py install
python service.py update
python service.py remove

python service.py start
```

Running interactive

```bash
python service.py debug

```

#### RabbitMQ

Enable management panel:
```bash
rabbitmq-plugins enable rabbitmq_management
```

* A good explanation for RabbitMQ exchanges - https://derickbailey.com/2014/11/14/understanding-the-relationship-between-rabbitmq-exchanges-queues-and-bindings/

## Development

### Requirements Install

```bash
pip install --trusted-host 10.10.30.245 --index-url http://10.10.30.245:8081/repository//pypi-preview/ -r requirements.txt

```


### Service troubleshooting

#### Error starting service: The service did not respond to the start or control request in a timely fashion.

* https://stackoverflow.com/questions/41200068/python-windows-service-error-starting-service-the-service-did-not-respond-to-t

#### Error removing service: The specified service has been marked for deletion. (1072)

```bash
sc queryex imubit_opcda_agent

```
