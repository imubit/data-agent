import pytest
from conftest import CONFIG_SECTION_CONNECTION_MANAGER

from data_agent.abstract_connector import SupportedOperation
from data_agent.config_manager import PersistentComponent, component_config_view
from data_agent.connection_manager import ConnectionManager
from data_agent.connectors.fake_connector import FakeConnector
from data_agent.exceptions import (
    ConnectionAlreadyExists,
    ConnectionRedefinitionNotSupported,
    UnrecognizedConnectionType,
)


def test_list_supported_connectors(connection_manager):
    assert connection_manager.list_supported_connectors() == {}


def test_target_info(connection_manager):
    info = connection_manager.target_info(target_ref="localhost", conn_type="fake")
    assert info == {"Name": "absolute-fake", "Endpoints": []}


def test_connection_lifecycle(connection_manager):
    conn_name = "test1"

    # Create
    assert connection_manager.list_connections() == []
    connection_manager.create_connection(conn_name=conn_name, conn_type="fake")
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

    # Try with the same name
    with pytest.raises(ConnectionAlreadyExists):
        connection_manager.create_connection(conn_name=conn_name, conn_type="fake")

    with pytest.raises(ConnectionRedefinitionNotSupported):
        connection_manager.create_connection(
            conn_name=conn_name, conn_type="fake2", ignore_existing=True
        )

    # This should succeed, but not do anything
    connection_manager.create_connection(
        conn_name=conn_name, conn_type="fake", ignore_existing=True
    )

    # Connect
    connection_manager.enable_connection(conn_name=conn_name)

    # Delete
    connection_manager.reset()
    assert connection_manager.list_connections() == []


def test_connection_autodelete(connection_manager):
    conn_name = "test1"

    # Create
    assert connection_manager.list_connections() == []
    connection_manager.create_connection(conn_name=conn_name, conn_type="fake")
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

    del connection_manager


def test_connection_persistence(config_setup):
    man = ConnectionManager(
        PersistentComponent(
            config_setup, CONFIG_SECTION_CONNECTION_MANAGER, enable_persistence=True
        ),
        extra_connectors={"fake": FakeConnector},
    )

    # Create
    assert man.list_connections() == []
    man.create_connection(conn_name="test1", conn_type="fake")
    assert man.list_connections() == [
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
    man.create_connection(conn_name="test2", conn_type="fake")
    assert man.list_connections() == [
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
        },
        {
            "name": "test2",
            "type": "fake",
            "category": "historian",
            "supported_filters": [],
            "supported_operations": [
                SupportedOperation.READ_TAG_PERIOD,
                SupportedOperation.READ_TAG_META,
            ],
            "default_attributes": [("tag", {"Type": "str", "Name": "Tag Name"})],
            "enabled": False,
        },
    ]
    man.enable_connection("test2")
    assert man.list_connections() == [
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
        },
        {
            "name": "test2",
            "type": "fake",
            "category": "historian",
            "supported_filters": [],
            "supported_operations": [
                SupportedOperation.READ_TAG_PERIOD,
                SupportedOperation.READ_TAG_META,
            ],
            "default_attributes": [("tag", {"Type": "str", "Name": "Tag Name"})],
            "enabled": True,
        },
    ]

    man2 = ConnectionManager(
        PersistentComponent(
            config_setup, CONFIG_SECTION_CONNECTION_MANAGER, enable_persistence=True
        ),
        extra_connectors={"fake": FakeConnector},
    )
    assert man2.list_connections() == [
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
        },
        {
            "name": "test2",
            "type": "fake",
            "category": "historian",
            "supported_filters": [],
            "supported_operations": [
                SupportedOperation.READ_TAG_PERIOD,
                SupportedOperation.READ_TAG_META,
            ],
            "default_attributes": [("tag", {"Type": "str", "Name": "Tag Name"})],
            "enabled": True,
        },
    ]

    config_view = component_config_view(config_setup, CONFIG_SECTION_CONNECTION_MANAGER)
    assert config_view == {
        "test1": {"type": "fake", "params": {}, "enabled": False},
        "test2": {"type": "fake", "params": {}, "enabled": True},
    }

    man2.disable_connection("test2")
    config_view = component_config_view(config_setup, CONFIG_SECTION_CONNECTION_MANAGER)
    assert config_view == {
        "test1": {"type": "fake", "params": {}, "enabled": False},
        "test2": {"type": "fake", "params": {}, "enabled": False},
    }

    man2.delete_connection("test2")
    del man2

    man3 = ConnectionManager(
        PersistentComponent(
            config_setup, CONFIG_SECTION_CONNECTION_MANAGER, enable_persistence=True
        ),
        extra_connectors={"fake": FakeConnector},
    )
    assert man3.list_connections() == [
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
        },
    ]
    del man3


def test_error_handling(connection_manager):
    # Create
    assert connection_manager.list_connections() == []
    connection_manager.create_connection(conn_name="test1", conn_type="fake")

    with pytest.raises(ConnectionAlreadyExists):
        connection_manager.create_connection(conn_name="test1", conn_type="fake")

    # Try with invalid type
    with pytest.raises(UnrecognizedConnectionType):
        connection_manager.create_connection(conn_name="test2", conn_type="bbb")
