from .abstract_connector import AbstractConnector, active_connection, group_exists


class GroupsAwareConnector(AbstractConnector):
    TYPE = "groups_aware_connector"
    _name = ""

    def __init__(self, conn_name):
        self._groups = {}
        super(GroupsAwareConnector, self).__init__(conn_name)

    @active_connection
    def list_groups(self) -> list:
        return list(self._groups)

    @active_connection
    def register_group(self, group_name: str, tags: list, refresh_rate_ms: bool = 1000):
        self._groups[group_name] = tags

    @active_connection
    @group_exists
    def unregister_group(self, group_name: str):
        del self._groups[group_name]

    @active_connection
    @group_exists
    def read_group_values(self, group_name: str, from_cache: bool = True) -> dict:
        tags = self._groups[group_name]
        return self.read_tag_values(tags)

    @active_connection
    @group_exists
    def write_group_values(
        self, group_name: str, tags: dict, wait_for_result: bool = True, **kwargs
    ) -> dict:
        return self.write_tag_values(tags, wait_for_result, **kwargs)
