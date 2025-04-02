# CLI Usage

## Creating OPC UA Connection

### Matrikon simulator

```commandline
da-broker exec create_connection --conn_type=opc-ua --conn_name=ua --server_uri=opc.tcp://127.0.0.1:4840/freeopcua/server/
da-broker exec enable_connection --conn_name=ua
```

### Matrikon simulator
```
da-broker exec create_job --job_id=job1 --conn_name=ua --tags="['2:PLC Server/2:CyclicData', '2:PLC Server/2:BooleanData']" --seconds=2
```
