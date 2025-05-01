[![PyPI-Server](https://img.shields.io/pypi/v/data-agent.svg)](https://pypi.org/project/data-agent/)
[![Coveralls](https://img.shields.io/coveralls/github/imubit/data-agent/main.svg)](https://coveralls.io/r/imubit/data-agent)



# Data Agent

Python package for accessing real-time and historical data on industrial historians and control systems.
Different historian protocols and APIs are implemented through standalone plugins:

* https://github.com/imubit/data-agent-osisoft-pi
* https://github.com/imubit/data-agent-aspen-ip21

## Description

The package provides a unified data access API having several usage scenarios:

* As a Python package
* As a command line CLI
* As a service (Windows or Linux) using AMQ protocol

## Installation

```commandline
pip install data-agent
```

Install the plugins required for communicating with the target systems

## Python Package Usage

```python
from data_agent.local_agent import LocalAgent

with LocalAgent() as agent:

    agent.api.list_supported_connectors()
    agent.api.create_connection(...)
```


## Command Line Usage

Start broker based agent
```bash
da-broker-svc --service.id my_service --broker.uri amqp://guest:guest@192.168.4.23/
```
