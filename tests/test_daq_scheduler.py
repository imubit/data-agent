import asyncio
import json

import pytest
from conftest import DATA_EXCHANGE_NAME, SERVICE_ID

from data_agent.connection_manager import ConnectionManager
from data_agent.connectors.fake_connector import FakeConnector
from data_agent.daq_scheduler import create_daq_scheduler
from data_agent.exceptions import DaqJobAlreadyExists, UnrecognizedConnection


@pytest.mark.asyncio
async def test_job_lifecycle(
    config_manager, amq_broker, mock_channel, connection_manager
):
    conn_name = "fake_conn"

    job1_id = "job1"
    job2_id = "job2"

    tags1 = ["Random.Real8", "Random.String"]
    tags2 = ["Static.Int4"]

    scheduler = create_daq_scheduler(
        amq_broker,
        connection_manager,
        config=config_manager,
    )

    connection_manager.create_connection(conn_name, conn_type="fake", enabled=True)

    conn = connection_manager.connection(conn_name)

    # Mock (simulation objects)
    mock_queue = await mock_channel.declare_queue("testing", auto_delete=True)
    # await mock_queue.bind(DATA_EXCHANGE_NAME)
    await mock_queue.bind(
        DATA_EXCHANGE_NAME, arguments={"service_id": SERVICE_ID, "x-match": "all"}
    )
    await mock_queue.purge()

    # Job1
    scheduler.create_scan_job(
        job_id=job1_id, conn_name=conn_name, tags=[tags1[1]], seconds=2
    )
    assert scheduler.get_job(job1_id).trigger.interval.seconds == 2

    # Recreate with 1 second nd extra tag
    scheduler.create_scan_job(
        job_id=job1_id,
        conn_name=conn_name,
        tags=tags1,
        seconds=1,
        update_on_conflict=True,
    )
    assert scheduler.get_job(job1_id).trigger.interval.seconds == 1

    # Job2
    scheduler.create_scan_job(
        job_id=job2_id, conn_name=conn_name, tags=tags2, seconds=1, from_cache=False
    )

    # Try creating with the same name
    with pytest.raises(DaqJobAlreadyExists):
        scheduler.create_scan_job(
            job_id=job2_id, conn_name=conn_name, tags=tags2, seconds=1
        )

    assert scheduler.list_jobs() == [job1_id, job2_id]
    assert scheduler.list_jobs(conn_name=conn_name) == [job1_id, job2_id]
    assert scheduler.list_jobs(conn_name="some") == []

    # Try creating with non-existing connection
    with pytest.raises(UnrecognizedConnection):
        scheduler.create_scan_job(
            job_id="some", conn_name="non-existing", tags=tags2, seconds=1
        )

    # Add tags to 2nd job
    scheduler.add_tags(job2_id, ["Static.Float"])
    assert scheduler.list_tags(job2_id) == ["Static.Int4", "Static.Float"]

    await asyncio.sleep(1.5)

    # Receive 2 messages (1 should be from 1st job and another from 2nd)
    for i in range(2):
        incoming_message = await mock_queue.get(timeout=5)
        payload = json.loads(incoming_message.body.decode())
        # print('<<<<<<<< TAGS READ <<<<<<<<<<<<<', payload)
        assert incoming_message.headers["job_id"] in [job1_id, job2_id]
        assert (
            "Static.Float" in payload["data"].keys()
            or "Random.Real8" in payload["data"].keys()
        )

    # Cleanup
    await mock_queue.unbind(DATA_EXCHANGE_NAME, "")
    conn.disconnect()
    connection_manager.delete_connection(conn_name)


@pytest.mark.asyncio
async def test_job_persistence(config_manager, amq_broker, mock_channel):
    conn_name = "fake_conn"

    job1_id = "job1"
    job2_id = "job2"

    tags1 = ["Random.Real8", "Random.String"]
    tags2 = ["Static.Int4"]

    connection_manager = ConnectionManager(
        config=config_manager,
        extra_connectors={"fake": FakeConnector},
    )

    scheduler = create_daq_scheduler(
        amq_broker,
        connection_manager,
        config=config_manager,
    )

    connection_manager.create_connection(conn_name, conn_type="fake", enabled=True)

    conn = connection_manager.connection(conn_name)

    # Mock (simulation objects)
    mock_queue = await mock_channel.declare_queue("testing", auto_delete=True)
    await mock_queue.bind(
        DATA_EXCHANGE_NAME, arguments={"service_id": SERVICE_ID, "x-match": "all"}
    )
    await mock_queue.purge()

    scheduler.create_scan_job(
        job_id=job1_id, conn_name=conn_name, tags=tags1, seconds=2
    )
    assert scheduler.get_job(job1_id).trigger.interval.seconds == 2
    scheduler.create_scan_job(
        job_id=job1_id,
        conn_name=conn_name,
        tags=tags1,
        seconds=1,
        update_on_conflict=True,
    )
    assert scheduler.get_job(job1_id).trigger.interval.seconds == 1

    scheduler.create_scan_job(
        job_id=job2_id, conn_name=conn_name, tags=tags2, seconds=1
    )
    assert scheduler.list_jobs() == [job1_id, job2_id]

    await asyncio.sleep(1.5)

    # Receive 2 messages (1 should be from 1st job and another from 2nd)
    for i in range(2):
        incoming_message = await mock_queue.get(timeout=5)
        payload = json.loads(incoming_message.body.decode())
        # print('<<<<<<<< TAGS READ <<<<<<<<<<<<<', payload)
        assert incoming_message.headers["job_id"] in [job1_id, job2_id]
        assert (
            "Static.Int4" in payload["data"].keys()
            or "Random.Real8" in payload["data"].keys()
        )

    scheduler.reset()

    await asyncio.sleep(0.5)
    await mock_queue.purge()

    scheduler = create_daq_scheduler(
        amq_broker,
        connection_manager,
        config=config_manager,
    )
    assert scheduler.list_jobs() == [job1_id, job2_id]

    await asyncio.sleep(1.5)

    # Receive 2 messages (1 should be from 1st job and another from 2nd)
    for i in range(2):
        incoming_message = await mock_queue.get(timeout=5)
        payload = json.loads(incoming_message.body.decode())
        # print('<<<<<<<< TAGS READ <<<<<<<<<<<<<', payload)
        assert incoming_message.headers["job_id"] in [job1_id, job2_id]
        assert (
            "Static.Int4" in payload["data"].keys()
            or "Random.Real8" in payload["data"].keys()
        )

    # Cleanup
    await mock_queue.unbind(DATA_EXCHANGE_NAME, "")
    conn.disconnect()
    connection_manager.delete_connection(conn_name)


@pytest.mark.asyncio
async def test_job_reconnect2(
    config_manager, amq_broker, mock_channel, connection_manager
):
    conn_name = "fake_conn"

    job1_id = "job1"
    job2_id = "job2"

    tags1 = ["Random.Real8", "Random.String"]
    tags2 = ["Static.Int4"]

    scheduler = create_daq_scheduler(
        amq_broker,
        connection_manager,
        config=config_manager,
    )

    connection_manager.create_connection(conn_name, conn_type="fake", enabled=True)

    conn = connection_manager.connection(conn_name)

    # Mock (simulation objects)
    mock_queue = await mock_channel.declare_queue("testing", auto_delete=True)
    # await mock_queue.bind(DATA_EXCHANGE_NAME)
    await mock_queue.bind(
        DATA_EXCHANGE_NAME, arguments={"service_id": SERVICE_ID, "x-match": "all"}
    )
    await mock_queue.purge()

    # Job1
    scheduler.create_scan_job(
        job_id=job1_id, conn_name=conn_name, tags=[tags1[1]], seconds=2
    )
    assert scheduler.get_job(job1_id).trigger.interval.seconds == 2

    # Recreate with 1 second nd extra tag
    scheduler.create_scan_job(
        job_id=job1_id,
        conn_name=conn_name,
        tags=tags1,
        seconds=1,
        update_on_conflict=True,
    )
    assert scheduler.get_job(job1_id).trigger.interval.seconds == 1

    # Job2
    scheduler.create_scan_job(
        job_id=job2_id, conn_name=conn_name, tags=tags2, seconds=1, from_cache=False
    )

    # Try creating with the same name
    with pytest.raises(DaqJobAlreadyExists):
        scheduler.create_scan_job(
            job_id=job2_id, conn_name=conn_name, tags=tags2, seconds=1
        )

    assert scheduler.list_jobs() == [job1_id, job2_id]
    assert scheduler.list_jobs(conn_name=conn_name) == [job1_id, job2_id]
    assert scheduler.list_jobs(conn_name="some") == []

    # Try creating with non-existing connection
    with pytest.raises(UnrecognizedConnection):
        scheduler.create_scan_job(
            job_id="some", conn_name="non-existing", tags=tags2, seconds=1
        )

    # Add tags to 2nd job
    scheduler.add_tags(job2_id, ["Static.Float"])
    assert scheduler.list_tags(job2_id) == ["Static.Int4", "Static.Float"]

    await asyncio.sleep(1.5)

    # Receive 2 messages (1 should be from 1st job and another from 2nd)
    for i in range(2):
        incoming_message = await mock_queue.get(timeout=5)
        payload = json.loads(incoming_message.body.decode())
        # print('<<<<<<<< TAGS READ <<<<<<<<<<<<<', payload)
        assert incoming_message.headers["job_id"] in [job1_id, job2_id]
        assert (
            "Static.Float" in payload["data"].keys()
            or "Random.Real8" in payload["data"].keys()
        )

    # Check that connection was enabled by daq_schedule
    assert conn.connected

    # Cleanup
    await mock_queue.unbind(DATA_EXCHANGE_NAME, "")
    conn.disconnect()
    connection_manager.delete_connection(conn_name)
