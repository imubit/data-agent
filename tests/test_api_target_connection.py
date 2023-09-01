import asyncio

import pytest

from data_agent.abstract_connector import SupportedOperation
from data_agent.exceptions import (
    ConnectionAlreadyExists,
    ConnectionNotActive,
    SafetyErrorManipulateOutsideOfRange,
    SafetyErrorManipulateUnauthorizedTag,
    UnrecognizedConnectionType,
)


@pytest.mark.asyncio
async def test_lifecycle(rpc_client, rpc_server):
    conn_name = "test1"

    # Create
    assert await rpc_client.proxy.list_connections() == []
    await rpc_client.proxy.create_connection(conn_name=conn_name, conn_type="fake")
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
            "enabled": False,
        }
    ]

    # Try with the same name
    with pytest.raises(ConnectionAlreadyExists):
        await rpc_client.proxy.create_connection(conn_name=conn_name, conn_type="fake")

    # Try with invalid type
    with pytest.raises(UnrecognizedConnectionType):
        await rpc_client.proxy.create_connection(conn_name="some_name", conn_type="bbb")

    # Try engaging disabled connection
    with pytest.raises(ConnectionNotActive):
        await rpc_client.proxy.read_tag_attributes(
            conn_name=conn_name, tags=["Random.String", "Random.Real8"]
        )

    # Connect
    await rpc_client.proxy.enable_connection(conn_name=conn_name)
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

    # Properties
    properties = await rpc_client.proxy.read_tag_attributes(
        conn_name=conn_name, tags=["Random.String", "Random.Real8"]
    )
    assert properties["Random.Real8"]["DataType"] == "Double Float"

    # List tags
    tags = await rpc_client.proxy.list_tags(
        conn_name=conn_name, filter="", include_attributes=False
    )
    assert tags == {
        "Static": {"DisplayName": "Static", "HasChildren": True},
        "Random": {"DisplayName": "Random", "HasChildren": True},
    }

    tags = await rpc_client.proxy.list_tags(
        conn_name=conn_name, filter="", include_attributes=True
    )
    assert tags == {
        "Static": {"DisplayName": "Static", "HasChildren": True},
        "Random": {"DisplayName": "Random", "HasChildren": True},
    }

    # Read
    tags = await rpc_client.proxy.read_tag_values(
        conn_name=conn_name, tags=["Random.Real8", "Random.String"]
    )
    assert tags["Random.String"]["Quality"] == "Good"

    # Disconnect
    await rpc_client.proxy.disable_connection(conn_name=conn_name)
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
            "enabled": False,
        }
    ]

    # Delete
    await rpc_client.proxy.delete_connection(conn_name=conn_name)
    assert await rpc_client.proxy.list_connections() == []


@pytest.mark.asyncio
async def test_write(rpc_client, rpc_server):
    conn_name = "test1"
    tag1 = "Static.Float"

    # Create connection
    assert await rpc_client.proxy.list_connections() == []
    await rpc_client.proxy.create_connection(conn_name=conn_name, conn_type="fake")
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
            "enabled": False,
        }
    ]

    # Connect
    await rpc_client.proxy.enable_connection(conn_name=conn_name)
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

    # Register tags and write value
    await rpc_client.proxy.register_manipulated_tags(
        conn_name=conn_name,
        tags={
            tag1: {
                "lb": -1,
                "ub": 1,
                "rb": 0.1,
            }
        },
    )

    with pytest.raises(SafetyErrorManipulateOutsideOfRange):
        await rpc_client.proxy.write_manipulated_tags(
            conn_name=conn_name, tags={tag1: 5}
        )

    await rpc_client.proxy.write_manipulated_tags(
        conn_name=conn_name, tags={tag1: 0.01}
    )

    await asyncio.sleep(0.1)

    # Read
    tags = await rpc_client.proxy.read_tag_values(conn_name=conn_name, tags=[tag1])
    assert tags[tag1]["Value"] == 0.01
    assert tags[tag1]["Quality"] == "Good"

    # Unregister
    await rpc_client.proxy.unregister_manipulated_tags(conn_name=conn_name, tags=[tag1])

    with pytest.raises(SafetyErrorManipulateUnauthorizedTag):
        await rpc_client.proxy.write_manipulated_tags(
            conn_name=conn_name, tags={tag1: 0}
        )

    # Disconnect
    await rpc_client.proxy.disable_connection(conn_name=conn_name)
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
            "enabled": False,
        }
    ]

    # Delete
    await rpc_client.proxy.delete_connection(conn_name=conn_name)
    assert await rpc_client.proxy.list_connections() == []
