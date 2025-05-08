import logging
import numbers

from .exceptions import (  # SafetyErrorManipulateOutsideOfRateBound,
    SafetyErrorBounderiesNotSpecified,
    SafetyErrorManipulateOutsideOfRange,
    SafetyErrorManipulateUnauthorizedTag,
    SafetyErrorWritingInvalidValue,
    UnrecognizedConnection,
)

log = logging.getLogger(__name__)

CONFIG_KEY = "manipulated_tags"
CONFIG_DOT_NOTATION = "\\\\D"


def _validate_connection_exists(func):
    def wrapper(self, *args, **kwargs):
        if "conn_name" in kwargs:
            conn_name = kwargs.get("conn_name")
        elif len(args) > 0:
            conn_name = args[0]

        if conn_name not in self.connection_manager.list_connections(
            include_details=False
        ):
            raise UnrecognizedConnection(
                'Connection "{}" does not exists.'.format(conn_name)
            )

        return func(self, *args, **kwargs)

    return wrapper


def _validate_connection_enabled(func):
    def wrapper(self, *args, **kwargs):
        if "conn_name" in kwargs:
            conn_name = kwargs.get("conn_name")
        elif len(args) > 0:
            conn_name = args[0]

        if conn_name not in self.connection_manager.list_connections(
            include_details=False
        ):
            raise UnrecognizedConnection(
                'Connection "{}" does not exists.'.format(conn_name)
            )

        # Check if connection active
        self.connection_manager.connection(conn_name)

        return func(self, *args, **kwargs)

    return wrapper


class SafeManipulator:
    def __init__(self, connection_manager, config):
        self.config = config
        self.connection_manager = connection_manager

    @_validate_connection_exists
    def list_tags(self, conn_name, include_attributes=False):
        conns = self.config.get(CONFIG_KEY)
        if conn_name not in conns:
            return []

        if include_attributes:
            tags = self.config.get(f"{CONFIG_KEY}.{conn_name}")

            # Remove escaped notation
            tags = {tag.replace(CONFIG_DOT_NOTATION, "."): tags[tag] for tag in tags}

        else:
            tags = list(self.config.get(f"{CONFIG_KEY}.{conn_name}").keys())

            # Remove escaped notation
            tags = [tag.replace(CONFIG_DOT_NOTATION, ".") for tag in tags]
            tags.sort()

        return tags

    @_validate_connection_exists
    def register_tags(self, conn_name, tags):
        for tag in tags:
            if not set(["ub", "lb", "rb"]).issubset(tags[tag].keys()):
                raise SafetyErrorBounderiesNotSpecified(
                    "One of the bounderies (Upper/Lower/Rate Bound) is not specified"
                )

            escaped_tag_name = tag.replace(".", CONFIG_DOT_NOTATION)
            self.config.set(f"{CONFIG_KEY}.{conn_name}.{escaped_tag_name}", tags[tag])

    @_validate_connection_exists
    def unregister_tags(self, conn_name, tags):
        for tag in tags:
            escaped_tag_name = tag.replace(".", CONFIG_DOT_NOTATION)
            self.config.remove(f"{CONFIG_KEY}.{conn_name}.{escaped_tag_name}")

    @_validate_connection_enabled
    def write_tags(self, conn_name, tags, wait_for_result=True, **kwargs):
        # Test safety
        for tag in tags:
            reg_tags = self.list_tags(conn_name, include_attributes=True)

            if tag not in reg_tags:
                raise SafetyErrorManipulateUnauthorizedTag(
                    f"{tag} is not registered as manipulated tag"
                )

            if not isinstance(tags[tag], numbers.Number):
                raise SafetyErrorWritingInvalidValue(
                    f"Cannot write a value of type {type(tags[tag])}"
                )

            constraints = reg_tags[tag]

            if (
                isinstance(constraints["lb"], numbers.Number)
                and tags[tag] < constraints["lb"]
            ):
                raise SafetyErrorManipulateOutsideOfRange(
                    f'Lower Bound constraint violated - LB: {constraints["lb"]}, Value: {tags[tag]}'
                )

            if (
                isinstance(constraints["ub"], numbers.Number)
                and tags[tag] > constraints["ub"]
            ):
                raise SafetyErrorManipulateOutsideOfRange(
                    f'Upper Bound constraint violated - UB: {constraints["ub"]}, Value: {tags[tag]}'
                )

        conn = self.connection_manager.connection(conn_name)
        conn.write_tag_values(tags, wait_for_result, **kwargs)
