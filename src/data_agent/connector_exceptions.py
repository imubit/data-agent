class ConnectionNotActive(Exception):
    pass


class TargetConnectionError(Exception):
    pass


class CannotBrowseTargetTags(Exception):
    pass


class TagsGroupAlreadyExists(Exception):
    pass


class TagsGroupNotFound(Exception):
    pass


class ErrorAddingTagsToGroup(Exception):
    pass


class ErrorReadingTags(Exception):
    pass


class ErrorSettingWriteValue(Exception):
    pass


class GroupErrorWritingValues(Exception):
    pass
