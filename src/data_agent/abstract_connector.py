from abc import ABC, abstractmethod
from functools import wraps
from typing import Union

import pandas as pd

from .exceptions import ConnectionNotActive, TagsGroupNotFound


class SupportedOperation:
    READ_TAG_VALUE = 1
    WRITE_TAG_VALUE = 2
    READ_TAG_PERIOD = 3
    WRITE_TAG_PERIOD = 4
    APPEND_TAG_PERIOD = 5
    OVERRIDE_TAG_PERIOD = 6
    READ_TAG_META = 7
    WRITE_TAG_META = 8
    CREATE_TAG = 9
    DELETE_TAG = 10


STANDARD_ATTRIBUTES = {
    "Name": {"Type": "str", "Name": "Tag Name"},
    "Type": {"Type": "str", "Name": "Data Type"},
    "EngUnits": {"Type": "str", "Name": "Eng. Units"},
    "Path": {"Type": "str", "Name": "Tag Path"},
    "Description": {"Type": "str", "Name": "Description"},
    "HasChildren": {"Type": "str", "Name": "HasChildren"},
}


def active_connection(func):
    @wraps(func)
    def inner(self, *args, **kwargs):
        if not self.connected:
            raise ConnectionNotActive("Not connected")

        return func(self, *args, **kwargs)

    return inner


def group_exists(func):
    @wraps(func)
    def inner(self, *args, **kwargs):
        if args[0] not in self.list_groups():
            raise TagsGroupNotFound(f'Group "{args[0]}" not found')

        return func(self, *args, **kwargs)

    return inner


class AbstractConnector(ABC):
    TYPE = "abstract_connector"
    _name = ""

    @staticmethod
    def plugin_supported():
        return False

    @staticmethod
    def list_connection_fields():
        return {}

    @staticmethod
    @abstractmethod
    def target_info(target_ref):
        return {}

    def __init__(self, conn_name):
        self._name = conn_name

    def __del__(self):
        if self.connected:
            self.disconnect()

    @property
    def name(self):
        return self._name

    @property
    @abstractmethod
    def connected(self):
        pass

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def disconnect(self):
        pass

    @abstractmethod
    def connection_info(self):
        pass

    @abstractmethod
    def list_tags(
        filter: Union[str, list] = "",
        include_attributes: Union[bool, list] = False,
        recursive: bool = False,
        max_results: int = 0,
    ) -> dict:
        pass

    @abstractmethod
    def read_tag_attributes(self, tags: list, attributes: list = None) -> dict:
        pass

    @abstractmethod
    def read_tag_values(self, tags: list) -> dict:
        pass

    @abstractmethod
    def read_tag_values_period(
        self,
        tags: list,
        first_timestamp=None,
        last_timestamp=None,
        time_frequency=None,
        result_format="dataframe",
        progress_callback=None,
    ) -> Union[dict, pd.DataFrame]:
        pass

    @abstractmethod
    def write_tag_values(self, tags: dict, wait_for_result: bool, **kwargs) -> dict:
        pass

    # @abstractmethod
    # def list_groups(self) -> list:
    #     pass
    #
    # @abstractmethod
    # def register_group(self, group_name: str, tags: list, refresh_rate_ms: int = 1000):
    #     pass
    #
    # @abstractmethod
    # def unregister_group(self, group_name: str):
    #     pass
    #
    # @abstractmethod
    # def read_group_values(self, group_name: str, from_cache: bool = True) -> dict:
    #     pass
    #
    # @abstractmethod
    # def write_group_values(
    #     self, group_name: str, tags: dict, wait_for_result: bool, **kwargs
    # ) -> dict:
    #     pass
