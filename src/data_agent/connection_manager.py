import logging
import sys
from importlib.metadata import entry_points

from .exceptions import (
    ConnectionAlreadyExists,
    ConnectionNotActive,
    ConnectionRedefinitionNotSupported,
    UnrecognizedConnection,
    UnrecognizedConnectionType,
)

log = logging.getLogger(__name__)


def _validate_connection_exists(func):
    def wrapper(self, *args, **kwargs):
        if "conn_name" in kwargs:
            conn_name = kwargs.get("conn_name")
        elif len(args) > 0:
            conn_name = args[0]

        if conn_name not in self.list_connections(include_details=False):
            raise UnrecognizedConnection(f'Connection "{conn_name}" does not exists.')

        return func(self, *args, **kwargs)

    return wrapper


def _validate_connection_enabled(func):
    def wrapper(self, *args, **kwargs):
        if "conn_name" in kwargs:
            conn_name = kwargs.get("conn_name")
        elif len(args) > 0:
            conn_name = args[0]

        if conn_name not in self.list_connections(include_details=False):
            raise UnrecognizedConnection(f'Connection "{conn_name}" does not exists.')

        if not self._connections_map[conn_name].connected:
            raise ConnectionNotActive("Connection not active")

        return func(self, *args, **kwargs)

    return wrapper


class ConnectionManager:
    def __init__(self, persistence, extra_connectors=None):
        self._persistence = persistence
        self._connections_map = {}
        self._connector_classes = {
            entry.name: entry.load() for entry in self.list_plugins()
        }

        if extra_connectors:
            for conn in extra_connectors:
                self._connector_classes[conn] = extra_connectors[conn]

        # Recreate connections from config
        connections = self._persistence.list_items()
        for conn in connections:
            self._create_connection(
                conn_name=conn,
                conn_type=connections[conn]["type"],
                **connections[conn]["params"],
            )
            if connections[conn]["enabled"]:
                try:
                    self.enable_connection(conn)
                except Exception as e:
                    log.error(f"Error enabling connection {conn} - {e}. ")
                    # self.disable_connection(conn)

        log.info(
            f"ConnectionManager initialized: supported connection types: {list(self._connector_classes.keys())}, "
            f"configured connections: {list(self._connections_map.keys()) if self._connections_map else ''}"
        )

    def __del__(self):
        self.close()

    def close(self):
        if not self._connector_classes:
            return

        # Remove connections, but not from persistance
        existing_connections = list(self._connections_map.keys())
        for conn in existing_connections:
            self._delete_connection(conn)

        self._connector_classes = None
        log.info("ConnectionManager terminated successfully.")

    def reset(self):
        existing_connections = list(self._connections_map.keys())
        for conn in existing_connections:
            self.delete_connection(conn)

    @staticmethod
    def list_plugins():
        if sys.version_info[:3] < (3, 10):
            return entry_points().get("data_agent.connectors", [])

        return entry_points(group="data_agent.connectors")

    @staticmethod
    def list_supported_connectors():
        entries = {
            entry.name: entry.load() for entry in ConnectionManager.list_plugins()
        }

        return {
            entry: {
                "category": entries[entry].CATEGORY,
                "connection_fields": entries[entry].list_connection_fields(),
            }
            for entry in entries
            if entries[entry].plugin_supported()
        }

    def target_info(self, target_ref, conn_type):
        return self._connector_classes[conn_type].target_info(target_ref)

    def list_connections(self, include_details=True):
        if not include_details:
            return list(self._connections_map.keys())

        return [self._conn_descriptor(v) for k, v in self._connections_map.items()]

    @_validate_connection_exists
    def is_connected(self, conn_name):
        return self._connections_map[conn_name].connected

    @_validate_connection_exists
    def connection(self, conn_name, check_enabled=True):
        if check_enabled and not self._connections_map[conn_name].connected:
            raise ConnectionNotActive("Connection not active")

        return self._connections_map[conn_name]

    def create_connection(
        self, conn_name, conn_type, enabled=False, ignore_existing=False, **kwargs
    ):
        if conn_name in self._connections_map.keys():
            if not ignore_existing:
                raise ConnectionAlreadyExists(
                    f'Connection "{conn_name}" already exists.'
                )

            existing_conn = self._connections_map[conn_name]
            if existing_conn.TYPE != conn_type:
                raise ConnectionRedefinitionNotSupported(
                    "Existing connection with the same name is different from the created one."
                )

            return self._conn_descriptor(self._connections_map[conn_name])

        conn = self._create_connection(
            conn_name=conn_name, conn_type=conn_type, **kwargs
        )
        if enabled:
            conn.connect()

        self._persistence.add_item(
            conn_name, {"type": conn_type, "params": kwargs, "enabled": enabled}
        )

        log.info(f"Connection '{conn_name}' of type '{conn_type}' created.")
        return self._conn_descriptor(conn)

    def _create_connection(self, conn_name, conn_type, **kwargs):
        if conn_type not in self._connector_classes.keys():
            raise UnrecognizedConnectionType(
                f'Unrecognized connection type "{conn_type}".'
            )

        self._connections_map[conn_name] = self._connector_classes[conn_type](
            conn_name=conn_name, **kwargs
        )
        return self._connections_map[conn_name]

    @staticmethod
    def _conn_descriptor(conn):
        return {
            "name": conn.name,
            "type": conn.TYPE,
            "category": conn.CATEGORY,
            "supported_filters": conn.SUPPORTED_FILTERS,
            "supported_operations": conn.SUPPORTED_OPERATIONS,
            "default_attributes": conn.DEFAULT_ATTRIBUTES,
            "enabled": conn.connected,
        }

    @_validate_connection_exists
    def delete_connection(self, conn_name):
        self._delete_connection(conn_name)
        self._persistence.remove_item(conn_name)

    def _delete_connection(self, conn_name):
        if self._connections_map[conn_name].connected:
            log.debug(f"Disconnecting '{conn_name}' connection...")
            self._connections_map[conn_name].disconnect()

        del self._connections_map[conn_name]

    @_validate_connection_exists
    def enable_connection(self, conn_name):
        if not self._connections_map[conn_name].connected:
            self._connections_map[conn_name].connect()

        self._persistence.update_subitem(conn_name, "enabled", True)

    @_validate_connection_exists
    def disable_connection(self, conn_name):
        if self._connections_map[conn_name].connected:
            self._connections_map[conn_name].disconnect()

        self._persistence.update_subitem(conn_name, "enabled", False)
