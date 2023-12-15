from enum import Enum


class MatchType(Enum):
    FULL_MATCH = 0
    PARTIAL_MATCH = 1
    NO_MATCH = 2


#  todo What if data points not available in 1 of the DB, how to get the know that user property is not available

class UserProperty(Enum):
    class DOB(Enum):
        FULL_MATCH = MatchType.FULL_MATCH
        PARTIAL_MATCH = MatchType.PARTIAL_MATCH
        NO_MATCH = MatchType.NO_MATCH

    class NAME(Enum):
        FULL_MATCH = MatchType.FULL_MATCH
        PARTIAL_MATCH = MatchType.PARTIAL_MATCH
        NO_MATCH = MatchType.NO_MATCH
