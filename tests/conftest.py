# -*- coding: utf-8 -*-
import asyncio
import os
import sys

import pytest
from aio_pika import ExchangeType, connect_robust
from amqp_fabric.amq_broker_connector import AmqBrokerConnector, CustomJsonRPC

from data_agent import dist_name
from data_agent.api import ServiceApi
from data_agent.config_manager import PersistentComponent
from data_agent.config_persist import PersistConfig
from data_agent.config_template import (
    CONFIG_SECTION_CONNECTION_MANAGER,
    CONFIG_SECTION_DAQ_SCHEDULER,
    CONFIG_SECTION_SAFE_MANIPULATOR,
)
from data_agent.connection_manager import ConnectionManager
from data_agent.connectors.fake_connector import FakeConnector
from data_agent.daq_scheduler import create_daq_scheduler
from data_agent.exchanger import DataExchanger
from data_agent.safe_manipulator import SafeManipulator

AMQP_URL = os.environ.get("BROKER_URL", "amqp://guest:guest@localhost/")
SERVICE_ID = os.environ.get("SERVICE_ID", "test-agent")
SERVICE_TYPE = os.environ.get("SERVICE_TYPE", "no-type")
SERVICE_DOMAIN = os.environ.get("SERVICE_DOMAIN", "some-domain")
RPC_EXCHANGE_NAME = os.environ.get(
    "RPC_EXCHANGE_NAME", f"{SERVICE_DOMAIN}.api.{SERVICE_TYPE}.{SERVICE_ID}"
)
DATA_EXCHANGE_NAME = os.environ.get("DATA_EXCHANGE_NAME", f"{SERVICE_DOMAIN}.daq.data")


@pytest.fixture
def config_setup(request):
    config = PersistConfig(dist_name, f"data_agent.{sys.platform}")
    config.clear()
    config.read(user=False)
    config["service"]["id"] = SERVICE_ID
    yield config


@pytest.fixture
def fake_conn():
    conn = FakeConnector()
    conn.connect()
    yield conn
    conn.disconnect()


@pytest.fixture
def connection_manager(config_setup):
    connection_manager = ConnectionManager(
        PersistentComponent(
            config_setup, CONFIG_SECTION_CONNECTION_MANAGER, enable_persistence=False
        ),
        extra_connectors={"fake": FakeConnector},
    )
    yield connection_manager


@pytest.fixture
def safe_manipulator(config_setup, connection_manager):
    safe_manipulator = SafeManipulator(
        connection_manager,
        PersistentComponent(
            config_setup, CONFIG_SECTION_SAFE_MANIPULATOR, enable_persistence=True
        ),
    )
    yield safe_manipulator


@pytest.fixture
def data_exchanger(connection_manager):
    data_exchanger = DataExchanger(
        connection_manager,
    )
    yield data_exchanger


@pytest.fixture
async def broker_conn():
    conn = await connect_robust(
        AMQP_URL,
        client_properties={"connection_name": "rpc_srv"},
    )

    yield conn
    await conn.close()


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
    config_setup, connection_manager, data_exchanger, safe_manipulator
):
    srv_conn = AmqBrokerConnector(
        amqp_uri=AMQP_URL,
        service_domain=SERVICE_DOMAIN,
        service_type=SERVICE_TYPE,
        service_id=SERVICE_ID,
    )
    await srv_conn.open()

    scheduler = create_daq_scheduler(
        srv_conn,
        connection_manager,
        persistence=PersistentComponent(
            config_setup, CONFIG_SECTION_DAQ_SCHEDULER, enable_persistence=True
        ),
    )
    api = ServiceApi(scheduler, connection_manager, data_exchanger, safe_manipulator)

    await srv_conn.rpc_register(api)
    await asyncio.sleep(0.2)

    yield srv_conn

    await srv_conn.close()


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
