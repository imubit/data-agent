import asyncio
import json

import pytest

from data_agent.abstract_connector import SupportedOperation


@pytest.mark.asyncio
async def test_job_create_modify(rpc_client, rpc_server, data_queue):
    conn_name = "test1"

    job1_id = "job1"
    job2_id = "job2"

    tags1 = ["Random.Real8", "Random.String"]
    tags2 = ["Static.Int4"]

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

    # Create 2 jobs
    await rpc_client.proxy.create_job(
        job_id=job1_id, conn_name=conn_name, tags=tags1, seconds=1
    )
    await rpc_client.proxy.create_job(
        job_id=job2_id, conn_name=conn_name, tags=tags2, seconds=1
    )
    assert await rpc_client.proxy.list_jobs() == [job1_id, job2_id]

    await asyncio.sleep(1.5)

    # Receive 2 messages (1 should be from 1st job and another from 2nd)
    for i in range(2):
        incoming_message = await data_queue.get(timeout=5)
        payload = json.loads(incoming_message.body.decode())
        # print('<<<<<<<< TAGS READ <<<<<<<<<<<<<', payload)
        assert (
            "Random.Real8" in payload["data"].keys()
            or "Static.Int4" in payload["data"].keys()
        )

    # Remove 1 job
    await rpc_client.proxy.remove_job(job_id=job1_id)
    assert await rpc_client.proxy.list_jobs() == [job2_id]

    assert await rpc_client.proxy.list_job_tags(job_id=job2_id) == ["Static.Int4"]

    # Add new tags to 2nd job
    await rpc_client.proxy.add_job_tags(job_id=job2_id, tags=["Static.Float"])
    assert await rpc_client.proxy.list_job_tags(job_id=job2_id) == [
        "Static.Int4",
        "Static.Float",
    ]
    await asyncio.sleep(1)
    incoming_message = await data_queue.get(timeout=5)
    payload = json.loads(incoming_message.body.decode())
    assert payload["data"]["Static.Float"]["Quality"] == "Good"

    # Remove tags from 2nd job
    await rpc_client.proxy.remove_job_tags(job_id=job2_id, tags=["Static.Int4"])
    assert await rpc_client.proxy.list_job_tags(job_id=job2_id) == ["Static.Float"]
    await asyncio.sleep(1)
    incoming_message = await data_queue.get(timeout=5)
    payload = json.loads(incoming_message.body.decode())
    assert payload["data"]["Static.Float"]["Quality"] == "Good"

    # Remove all jobs by terminating connection
    await rpc_client.proxy.delete_connection(conn_name=conn_name)
    assert await rpc_client.proxy.list_jobs() == []


# @pytest.mark.asyncio
# async def test_unavailable_tag(rpc_client, data_queue):
#
#     tags1 = ['Random.Notexist']
