import random
from datetime import datetime
from typing import Union

import numpy as np
import pandas as pd

from data_agent.abstract_connector import SupportedOperation, active_connection
from data_agent.exceptions import ErrorAddingTagsToGroup
from data_agent.groups_aware_connector import GroupsAwareConnector

np.random.seed(1)


def _get_from_dict(dataDict, mapList):
    if mapList == [""]:
        return dataDict

    for k in mapList:
        dataDict = dataDict[k]
    return dataDict


class FakeConnector(GroupsAwareConnector):
    TYPE = "fake"
    CATEGORY = "historian"
    SUPPORTED_FILTERS = []
    SUPPORTED_OPERATIONS = [
        SupportedOperation.READ_TAG_PERIOD,
        SupportedOperation.READ_TAG_META,
    ]
    DEFAULT_ATTRIBUTES = [
        ("tag", {"Type": "str", "Name": "Tag Name"}),
    ]

    @staticmethod
    def supported():
        return True

    @staticmethod
    def list_connection_fields():
        return {}

    @staticmethod
    def target_info(_):
        return {"Name": "absolute-fake", "Endpoints": []}

    def __init__(self, conn_name="fake_client", **kwargs):
        super(FakeConnector, self).__init__(conn_name)
        self._connected = False
        self._tags = {
            "Static": {
                "Float": {
                    "Value": 83289.48243,
                    "Quality": "Good",
                    "Timestamp": "09/02/2021T07:42:22.040",
                    "DataType": "Double Float",
                },
                "Int4": {
                    "Value": 12345,
                    "Quality": "Good",
                    "Timestamp": "09/02/2021T07:40:22.040",
                    "DataType": "Int4",
                },
            },
            "Random": {
                "Real8": {
                    "Value": 4289.84243,
                    "Quality": "Good",
                    "Timestamp": "09/02/2021T07:42:22.040",
                    "DataType": "Double Float",
                },
                "String": {
                    "Value": "Hello",
                    "Quality": "Good",
                    "Timestamp": "09/02/2021T07:40:22.040",
                    "DataType": "Int4",
                },
            },
        }

    def _update_random(self):
        self._tags["Random"]["Real8"]["Value"] = random.random() * 1000
        self._tags["Random"]["Real8"]["Timestamp"] = str(datetime.utcnow())
        self._tags["Random"]["String"]["Value"] = random.choice(
            "We are going to win this race.".split(" ")
        )
        self._tags["Random"]["String"]["Timestamp"] = str(datetime.utcnow())

    @property
    def connected(self):
        return self._connected

    def connect(self):
        self._connected = True

    def disconnect(self):
        self._connected = False
        self._groups = {}

    def connection_info(self):
        pass

    @active_connection
    def list_tags(
        self,
        filter: Union[str, list] = "",
        include_attributes: Union[bool, list] = False,
        recursive: bool = False,
        max_results: int = 0,
    ):
        self._update_random()
        path_list = filter.split(".")
        subtree = _get_from_dict(self._tags, path_list)

        res = {}
        for item in subtree:
            res[f"{filter}.{item}" if filter else item] = {
                "DisplayName": item,
                "HasChildren": subtree[item] and "Value" not in subtree[item],
            }

            if include_attributes and "Value" in subtree[item]:
                res[f"{filter}.{item}"].update(subtree[item])

        return res

    @active_connection
    def read_tag_attributes(self, tags: list, attributes: list = None):
        self._update_random()

        res = {}

        for tag in tags:
            path_list = tag.split(".")
            subtree = _get_from_dict(self._tags, path_list)

            if "Value" in subtree:
                if not attributes:
                    res[tag] = subtree
                else:
                    res[tag] = {}
                    for attr in attributes:
                        if attr in subtree:
                            res[tag][attr] = subtree[attr]

        return res

    @active_connection
    def read_tag_values(self, tags: list):
        self._update_random()

        res = {}

        for tag in tags:
            path_list = tag.split(".")
            subtree = _get_from_dict(self._tags, path_list)

            if "Value" in subtree:
                res[tag] = {
                    "Value": subtree["Value"],
                    "Quality": subtree["Quality"],
                    "Timestamp": subtree["Timestamp"],
                }

        return res

    @active_connection
    def read_tag_values_period(
        self,
        tags: list,
        first_timestamp=None,
        last_timestamp=None,
        time_frequency=None,
        max_results=None,
        result_format="dataframe",
        progress_callback=None,
    ):
        # Check if in path
        for tag in tags:
            path_list = tag.split(".")
            _get_from_dict(self._tags, path_list)

        self._update_random()
        cols = len(tags)
        rows = max_results if max_results else 100
        first_timestamp = first_timestamp or "2019-01-01"
        data = np.random.rand(rows, cols)
        tidx = pd.date_range(first_timestamp, periods=rows, freq="MS")
        df = pd.DataFrame(data, columns=tags, index=tidx)
        df.index.name = "timestamp"
        return df

    @active_connection
    def write_tag_values(self, tags: dict, wait_for_result: bool = True, **kwargs):
        self._update_random()
        res = {}

        for tag in tags:
            path_list = tag.split(".")
            subtree = _get_from_dict(self._tags, path_list)
            if "Value" in subtree:
                subtree["Value"] = tags[tag]

            res[tag] = {"Quality": "Good"}

        return res

    @active_connection
    def register_group(self, group_name: str, tags: list, refresh_rate_ms: int = 1000):
        self._update_random()

        # if group_name in self._groups:
        #     raise TagsGroupAlreadyExists(f'Group {group_name} already exists')

        try:
            for tag in tags:
                path_list = tag.split(".")
                _get_from_dict(self._tags, path_list)

            self._groups[group_name] = tags
        except KeyError:
            raise ErrorAddingTagsToGroup(f"Tag {tag} does not exist.")

    @active_connection
    def read_group_values(self, group_name: str, from_cache: bool = True):
        self._update_random()
        return super(FakeConnector, self).read_group_values(group_name, from_cache)

    @active_connection
    def write_group_values(
        self, group_name: str, tags: dict, wait_for_result: bool = True, **kwargs
    ):
        self._update_random()
        return super(FakeConnector, self).write_group_values(
            group_name, tags, wait_for_result, **kwargs
        )
