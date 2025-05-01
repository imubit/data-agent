# -*- coding: utf-8 -*-
import asyncio
import os

import numpy as np
import pandas as pd
import pytest
from aio_pika import ExchangeType, connect_robust
from amqp_fabric.amq_broker_connector import AmqBrokerConnector, CustomJsonRPC

from data_agent.api import ServiceApi
from data_agent.config_manager import ConfigManager
from data_agent.connection_manager import ConnectionManager
from data_agent.connectors.fake_connector import FakeConnector
from data_agent.daq_scheduler import create_daq_scheduler
from data_agent.exchanger import DataExchanger
from data_agent.history_harvester import HistoryHarvester
from data_agent.safe_manipulator import SafeManipulator

AMQP_URL = os.environ.get("BROKER_URL", "amqp://guest:guest@localhost/")
SERVICE_ID = os.environ.get("SERVICE_ID", "test-agent")
SERVICE_TYPE = os.environ.get("SERVICE_TYPE", "no-type")
SERVICE_DOMAIN = os.environ.get("SERVICE_DOMAIN", "some-domain")
RPC_EXCHANGE_NAME = os.environ.get(
    "RPC_EXCHANGE_NAME", f"{SERVICE_DOMAIN}.api.{SERVICE_TYPE}.{SERVICE_ID}"
)
DATA_EXCHANGE_NAME = os.environ.get("DATA_EXCHANGE_NAME", f"{SERVICE_DOMAIN}.data")


@pytest.fixture
def temp_config_file(tmp_path):
    """Creates a temporary config file location"""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("app:\n  interval: 30\nservice:\n  id: test_service\n")
    return str(config_path)


@pytest.fixture
def config_manager(temp_config_file):
    config = ConfigManager(config_file=temp_config_file)
    config.set("service.id", SERVICE_ID)
    yield config


@pytest.fixture
def fake_conn():
    conn = FakeConnector()
    conn.connect()
    yield conn
    conn.disconnect()


@pytest.fixture
def connection_manager(config_manager):
    connection_manager = ConnectionManager(
        config_manager,
        extra_connectors={"fake": FakeConnector},
    )
    yield connection_manager


@pytest.fixture
def safe_manipulator(config_manager, connection_manager):
    safe_manipulator = SafeManipulator(
        connection_manager,
        config=config_manager,
    )
    yield safe_manipulator


@pytest.fixture
def data_exchanger(connection_manager):
    data_exchanger = DataExchanger(
        connection_manager,
    )
    yield data_exchanger


@pytest.fixture
async def amq_broker():
    broker = AmqBrokerConnector(
        amqp_uri=AMQP_URL,
        service_domain=SERVICE_DOMAIN,
        service_type=SERVICE_TYPE,
        service_id=SERVICE_ID,
    )
    await broker.open()
    yield broker
    await broker.close()


@pytest.fixture
async def hist_harvest(connection_manager, amq_broker):
    harvester = HistoryHarvester(connection_manager, amq_broker)
    yield harvester


@pytest.fixture
async def mock_channel():
    conn = await connect_robust(
        AMQP_URL,
        client_properties={"connection_name": "rpc_srv"},
    )

    channel = await conn.channel()

    yield channel

    await channel.close()
    await conn.close()


@pytest.fixture
async def rpc_client():
    client_conn = await connect_robust(
        AMQP_URL, client_properties={"connection_name": "caller"}
    )

    async with client_conn:
        channel = await client_conn.channel()
        rpc = await CustomJsonRPC.create(channel, exchange=RPC_EXCHANGE_NAME)

        yield rpc

        await client_conn.close()


@pytest.fixture
async def rpc_server(
    config_manager, amq_broker, connection_manager, data_exchanger, safe_manipulator
):
    scheduler = create_daq_scheduler(
        amq_broker,
        connection_manager,
        config=config_manager,
    )
    api = ServiceApi(scheduler, connection_manager, data_exchanger, safe_manipulator)

    await amq_broker.rpc_register(api)
    await asyncio.sleep(0.2)

    yield amq_broker

    await amq_broker.close()


@pytest.fixture
async def data_queue():
    client_conn = await connect_robust(
        AMQP_URL,
        client_properties={"connection_name": "caller"},
    )

    channel = await client_conn.channel()

    await channel.exchange_delete(exchange_name=DATA_EXCHANGE_NAME)
    await channel.declare_exchange(
        name=DATA_EXCHANGE_NAME, type=ExchangeType.HEADERS, durable=True
    )

    queue = await channel.declare_queue("testing", auto_delete=True)

    await queue.bind(DATA_EXCHANGE_NAME, "")
    await queue.purge()

    yield queue

    await queue.unbind(DATA_EXCHANGE_NAME, "")
    await channel.close()
    await client_conn.close()


@pytest.fixture
def test_dataframe():
    # Create a sample DataFrame with mixed dtypes
    dates = pd.date_range(start="2025-01-01", periods=5, freq="h")
    df = pd.DataFrame(
        {
            "float_col": np.linspace(0.1, 0.5, 5),
            "int_col": np.arange(5),
            "str_col": [f"str{n}" for n in range(5)],
            "bool_col": [True, False, True, False, True],
        },
        index=dates,
    )
    df.index.name = "timestamp"
    return df
