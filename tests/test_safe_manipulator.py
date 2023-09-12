import pytest
from conftest import CONFIG_SECTION_SAFE_MANIPULATOR

from data_agent.abstract_connector import SupportedOperation
from data_agent.config_manager import PersistentComponent
from data_agent.exceptions import (
    SafetyErrorManipulateOutsideOfRange,
    SafetyErrorManipulateUnauthorizedTag,
    SafetyErrorWritingInvalidValue,
)
from data_agent.safe_manipulator import SafeManipulator


def test_unauthorized_writes(config_setup, connection_manager):
    conn_name = "test1"
    tag_name = "Static.Float"
    tag_name2 = "Static.Int4"

    # Create
    assert connection_manager.list_connections() == []
    connection_manager.create_connection(
        conn_name=conn_name, conn_type="fake", enabled=False
    )
    assert connection_manager.list_connections() == [
        {
            "name": "test1",
            "type": "fake",
            "category": "historian",
            "supported_filters": [],
            "supported_operations": [
                SupportedOperation.READ_TAG_PERIOD,
                SupportedOperation.READ_TAG_META,
            ],
            "default_attributes": [("tag", {"Type": "str", "Name": "Tag Name"})],
            "enabled": False,
        }
    ]
    conn = connection_manager.connection(conn_name=conn_name, check_enabled=False)
    conn.connect()
    res = conn.write_tag_values({tag_name: 3})
    assert res[tag_name]["Quality"] == "Good"

    manipulator = SafeManipulator(
        connection_manager,
        PersistentComponent(
            config_setup, CONFIG_SECTION_SAFE_MANIPULATOR, enable_persistence=True
        ),
    )

    manipulator.register_tags(
        conn_name,
        {
            tag_name: {
                "lb": -1,
                "ub": 1,
                "rb": 0.1,
            }
        },
    )

    assert manipulator.list_tags(conn_name) == [tag_name]

    manipulator.register_tags(
        conn_name,
        {
            tag_name2: {
                "lb": None,
                "ub": 2,
                "rb": 3,
            }
        },
    )

    assert manipulator.list_tags(conn_name) == [tag_name, tag_name2]

    with pytest.raises(SafetyErrorManipulateUnauthorizedTag):
        manipulator.write_tags(conn_name, {"unsafe_tag": 5})

    with pytest.raises(SafetyErrorManipulateOutsideOfRange):
        manipulator.write_tags(conn_name, {tag_name: 5})

    with pytest.raises(SafetyErrorManipulateOutsideOfRange):
        manipulator.write_tags(conn_name, {tag_name: -2})

    # Unrestricted lower bound
    manipulator.write_tags(conn_name, {tag_name2: -2})

    with pytest.raises(SafetyErrorManipulateOutsideOfRange):
        manipulator.write_tags(conn_name, {tag_name2: 4})

    with pytest.raises(SafetyErrorWritingInvalidValue):
        manipulator.write_tags(conn_name, {tag_name: None})

    # Successful write
    manipulator.write_tags(conn_name, {tag_name: 0.1})

    manipulator.unregister_tags(conn_name, [tag_name])

    assert manipulator.list_tags(conn_name) == [tag_name2]

    with pytest.raises(SafetyErrorManipulateUnauthorizedTag):
        manipulator.write_tags(conn_name, {tag_name: 5})

    connection_manager.delete_connection(conn_name)
