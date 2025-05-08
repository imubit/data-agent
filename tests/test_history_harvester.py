import asyncio
import datetime as dt

import pytest
from conftest import DATA_EXCHANGE_NAME, SERVICE_ID

from data_agent.msg_packer import decode_payload


@pytest.mark.asyncio
async def test_lifecycle(hist_harvest, mock_channel):
    conn_name = "fake_conn"

    job1_id = "hist_job1"

    tags1 = ["Random.Real8", "Random.String"]

    hist_harvest.connection_manager.create_connection(
        conn_name, conn_type="fake", enabled=True
    )

    # Mock (simulation objects)
    mock_queue = await mock_channel.declare_queue("testing", auto_delete=True)
    # await mock_queue.bind(DATA_EXCHANGE_NAME)
    await mock_queue.bind(
        DATA_EXCHANGE_NAME, arguments={"service_id": SERVICE_ID, "x-match": "all"}
    )
    await mock_queue.purge()

    # Job1
    hist_harvest.create_delivery_job(
        job_id=job1_id,
        conn_name=conn_name,
        tags=tags1,
        first_timestamp=dt.datetime.now() - dt.timedelta(hours=1),
        last_timestamp=dt.datetime.now(),
        time_frequency=None,
        batch_size=dt.timedelta(minutes=10),
    )

    await asyncio.sleep(1.5)

    incoming_message = await mock_queue.get(timeout=5)
    assert incoming_message.headers["job_id"] == job1_id
    data = decode_payload(incoming_message.body)
    assert list(data.columns) == tags1
    assert len(data) == 100

    # Cleanup
    await mock_queue.unbind(DATA_EXCHANGE_NAME, "")
    hist_harvest.connection_manager.connection(conn_name).disconnect()
    hist_harvest.connection_manager.delete_connection(conn_name)
