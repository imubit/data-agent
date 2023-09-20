import pytest

from data_agent.abstract_connector import SupportedOperation
from data_agent.exceptions import (
    SafetyErrorManipulateOutsideOfRange,
    SafetyErrorManipulateUnauthorizedTag,
    UnrecognizedConnection,
)


@pytest.mark.asyncio
async def test_safe_manipulator(rpc_client, rpc_server, data_queue):
    conn_name = "test1"
    tag_name = "Static.Float"

    with pytest.raises(UnrecognizedConnection):
        await rpc_client.proxy.register_manipulated_tags(
            conn_name=conn_name,
            tags={
                tag_name: {
                    "lb": -1,
                    "ub": 1,
                    "rb": 0.1,
                }
            },
        )

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

    await rpc_client.proxy.register_manipulated_tags(
        conn_name=conn_name,
        tags={
            tag_name: {
                "lb": -1,
                "ub": 1,
                "rb": 0.1,
            }
        },
    )

    assert await rpc_client.proxy.list_manipulated_tags(conn_name=conn_name) == [
        tag_name
    ]

    with pytest.raises(SafetyErrorManipulateUnauthorizedTag):
        await rpc_client.proxy.write_manipulated_tags(
            conn_name=conn_name, tags={"unsafe_tag": 5}
        )

    with pytest.raises(SafetyErrorManipulateOutsideOfRange):
        await rpc_client.proxy.write_manipulated_tags(
            conn_name=conn_name, tags={tag_name: 5}
        )

    # Successful write
    await rpc_client.proxy.write_manipulated_tags(
        conn_name=conn_name, tags={tag_name: 0.1}
    )

    await rpc_client.proxy.unregister_manipulated_tags(
        conn_name=conn_name, tags=[tag_name]
    )

    assert await rpc_client.proxy.list_manipulated_tags(conn_name=conn_name) == []

    with pytest.raises(SafetyErrorManipulateUnauthorizedTag):
        await rpc_client.proxy.write_manipulated_tags(
            conn_name=conn_name, tags={tag_name: 5}
        )

    # Test that tags are deleted with connections
    await rpc_client.proxy.register_manipulated_tags(
        conn_name=conn_name,
        tags={
            tag_name: {
                "lb": -1,
                "ub": 1,
                "rb": 0.1,
            }
        },
    )

    assert await rpc_client.proxy.list_manipulated_tags(conn_name=conn_name) == [
        tag_name
    ]

    await rpc_client.proxy.delete_connection(conn_name=conn_name)
    assert await rpc_client.proxy.list_connections() == []

    with pytest.raises(UnrecognizedConnection):
        await rpc_client.proxy.list_manipulated_tags(conn_name=conn_name)
