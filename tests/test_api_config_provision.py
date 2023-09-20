import asyncio
import json

import pytest

from data_agent.abstract_connector import SupportedOperation


@pytest.mark.asyncio
async def test_job_create_modify(rpc_client, rpc_server, data_queue):
    conn_name = "test1"
    config = {
        conn_name: {
            "daq_jobs": {
                "data_1::5": {"tags": ["Random.Real8"], "sample_rate": 1},
                "data_1::6": {
                    "tags": ["Random.Real8", "Random.Real8", "Static.Float"],
                    "sample_rate": 1.1,
                },
            },
            "manipulated_tags": {"Static.Int4": {"lb": 1, "rb": 5, "ub": 20}},
        }
    }

    # Create connection
    await rpc_client.proxy.create_connection(
        conn_name=conn_name, conn_type="fake", enabled=True
    )
    assert await rpc_client.proxy.list_connections() == [
        {
            "name": "test1",
            "type": "fake",
            "category": "historian",
            "supported_filters": [],
            "supported_operations": [
                SupportedOperation.READ_TAG_PERIOD,
                SupportedOperation.READ_TAG_META,
            ],
            "default_attributes": [["tag", {"Type": "str", "Name": "Tag Name"}]],
            "enabled": True,
        }
    ]

    await rpc_client.proxy.provision_config(config=config)
    assert await rpc_client.proxy.list_manipulated_tags(conn_name=conn_name) == [
        "Static.Int4"
    ]

    await asyncio.sleep(1.5)

    # Receive 2 messages (1 should be from 1st job and another from 2nd)
    for i in range(2):
        incoming_message = await data_queue.get(timeout=5)
        payload = json.loads(incoming_message.body.decode())
        # print('<<<<<<<< TAGS READ <<<<<<<<<<<<<', payload)
        assert (
            "Static.Int4" in payload["data"].keys()
            or "Random.Real8" in payload["data"].keys()
        )

    await rpc_client.proxy.delete_connection(conn_name=conn_name)
