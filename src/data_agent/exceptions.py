class UnrecognizedConnection(Exception):
    pass


class UnrecognizedConnectionType(Exception):
    pass


class ConnectionAlreadyExists(Exception):
    pass


class ConnectionNotActive(Exception):
    pass


class TargetConnectionError(Exception):
    pass


class ConnectionRedefinitionNotSupported(Exception):
    pass


class CannotBrowseTargetTags(Exception):
    pass


class ErrorAddingTagsToGroup(Exception):
    pass


class TagsGroupAlreadyExists(Exception):
    pass


class ErrorReadingTags(Exception):
    pass


class ErrorWritingReadonlyTag(Exception):
    pass


class TagsGroupNotFound(Exception):
    pass


class GroupErrorWritingValues(Exception):
    pass


class GroupAlreadyExists(Exception):
    pass


class SafetyErrorWritingInvalidValue(Exception):
    pass


class SafetyErrorBounderiesNotSpecified(Exception):
    pass


class SafetyErrorManipulateUnauthorizedTag(Exception):
    pass


class SafetyErrorManipulateOutsideOfRange(Exception):
    pass


class SafetyErrorManipulateOutsideOfRateBound(Exception):
    pass


class DaqJobAlreadyExists(Exception):
    pass
