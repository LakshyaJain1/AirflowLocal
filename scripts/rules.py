from user_properties import UserProperty
from universal_decision import UniversalDecision


class DecisionMaker:
    """
    This class help to decide the final merge decision between 2 users from 2 different tenants
    Update MATCHING_ALGO_VERSION whenever you update decision tree
    """
    DECISION_TREE = {
        (UserProperty.DOB.FULL_MATCH, UserProperty.NAME.FULL_MATCH): UniversalDecision.MERGE,
        (UserProperty.DOB.FULL_MATCH, UserProperty.NAME.PARTIAL_MATCH): UniversalDecision.MERGE,
        (UserProperty.DOB.FULL_MATCH, UserProperty.NAME.NO_MATCH): UniversalDecision.REVIEW,
        (UserProperty.DOB.PARTIAL_MATCH, UserProperty.NAME.FULL_MATCH): UniversalDecision.MERGE,
        (UserProperty.DOB.PARTIAL_MATCH, UserProperty.NAME.PARTIAL_MATCH): UniversalDecision.REVIEW,
        (UserProperty.DOB.PARTIAL_MATCH, UserProperty.NAME.NO_MATCH): UniversalDecision.REJECT,
        (UserProperty.DOB.NO_MATCH, UserProperty.NAME.FULL_MATCH): UniversalDecision.REVIEW,
        (UserProperty.DOB.NO_MATCH, UserProperty.NAME.PARTIAL_MATCH): UniversalDecision.REJECT,
        (UserProperty.DOB.NO_MATCH, UserProperty.NAME.NO_MATCH): UniversalDecision.REJECT,
    }

    @staticmethod
    def make_decision(dob_match: UserProperty.DOB, name_match: UserProperty.NAME) -> UniversalDecision:
        decision = DecisionMaker.DECISION_TREE.get((dob_match, name_match))
        return decision if decision else UniversalDecision.REJECT
