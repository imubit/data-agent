import random

import pytest

from data_agent.connectors.fake_connector import FakeConnector
from data_agent.exceptions import ErrorAddingTagsToGroup


def test_sanity():
    conn = FakeConnector()
    conn.connect()

    assert conn.supported()
    assert conn.TYPE == "fake"
    assert conn.name == "fake_client"

    info = conn.connection_info()
    print(info)
    # assert info['Version'].startswith('1')

    conn.disconnect()


def test_list_tags(fake_conn):
    res = fake_conn.list_tags("", include_attributes=False)
    assert res == {
        "Static": {"DisplayName": "Static", "HasChildren": True},
        "Random": {"DisplayName": "Random", "HasChildren": True},
    }

    res = fake_conn.list_tags("Static", include_attributes=False)
    assert res == {
        "Static.Float": {"DisplayName": "Float", "HasChildren": False},
        "Static.Int4": {"DisplayName": "Int4", "HasChildren": False},
    }

    res = fake_conn.list_tags("Static", include_attributes=True)
    assert res == {
        "Static.Float": {
            "DisplayName": "Float",
            "HasChildren": False,
            "Value": 83289.48243,
            "Quality": "Good",
            "Timestamp": "09/02/2021T07:42:22.040",
            "DataType": "Double Float",
        },
        "Static.Int4": {
            "DisplayName": "Int4",
            "HasChildren": False,
            "Value": 12345,
            "Quality": "Good",
            "Timestamp": "09/02/2021T07:40:22.040",
            "DataType": "Int4",
        },
    }


def test_tag_attributes(fake_conn):
    attr = fake_conn.read_tag_attributes(tags=["Static.Float", "Static.Int4"])
    assert attr["Static.Float"]["Timestamp"] == "09/02/2021T07:42:22.040"
    assert attr["Static.Int4"]["DataType"] == "Int4"


def test_read_tag_values(fake_conn):
    tags = ["Random.Real8", "Random.String"]

    res = fake_conn.read_tag_values(tags)
    assert res["Random.String"]["Value"] in "We are going to win this race.".split(" ")


async def test_read_period(fake_conn):
    tag1 = "Static.Float"

    # Create connection
    df = fake_conn.read_tag_values_period(tags=[tag1], max_results=20)
    assert df.size == 20
    assert df.columns == [tag1]


def test_group_read(fake_conn):
    group_name = "group1"
    tags = ["Static.Float", "Static.Int4"]

    assert fake_conn.list_groups() == []

    fake_conn.register_group(group_name, tags)

    assert fake_conn.list_groups() == ["group1"]

    res = fake_conn.read_group_values(group_name)
    assert res == {
        "Static.Float": {
            "Value": 83289.48243,
            "Quality": "Good",
            "Timestamp": "09/02/2021T07:42:22.040",
        },
        "Static.Int4": {
            "Value": 12345,
            "Quality": "Good",
            "Timestamp": "09/02/2021T07:40:22.040",
        },
    }

    fake_conn.unregister_group(group_name)


def test_group_read_non_existing(fake_conn):
    group_name = "group1"
    tags = ["Static.Float", "non_existing", "Static.Int4"]

    with pytest.raises(ErrorAddingTagsToGroup):
        fake_conn.register_group(group_name, tags)


def test_group_write(fake_conn):
    group_name = "group1"
    tags = {
        "Static.Float": random.uniform(2.5, 10.0),
        "Static.Int4": random.randrange(100),
    }
    fake_conn.register_group(group_name, tags.keys())

    res = fake_conn.write_group_values(group_name, tags=tags, wait_for_result=True)
    assert res == {
        "Static.Float": {"Quality": "Good"},
        "Static.Int4": {"Quality": "Good"},
    }

    # Wait to propagate the value
    # time.sleep(2)

    res = fake_conn.read_group_values(group_name)

    for tag in tags:
        assert tags[tag] == res[tag]["Value"]

    fake_conn.unregister_group(group_name)
