"""Decisions.

The purpose is to make it easier to work with the API.

See http://docs.amazonwebservices.com/amazonswf/latest/apireference/API_Decision.html.
"""

_decision_types = [
        "ScheduleActivityTask", 
        "RequestCancelActivityTask", 
        "CompleteWorkflowExecution", 
        "FailWorkflowExecution", 
        "CancelWorkflowExecution", 
        "ContinueAsNewWorkflowExecution", 
        "RecordMarker", 
        "StartTimer", 
        "CancelTimer", 
        "SignalExternalWorkflowExecution", 
        "RequestCancelExternalWorkflowExecution", 
        "StartChildWorkflowExecution",
        ]

def _attr_key_name(s):
    return s[0].lower() + s[1:] + 'DecisionAttributes'

_attr_keys = map(_attr_key_name, _decision_types)
_attr_key_lookup = dict(zip(_decision_types, _attr_keys))


def skeleton(decision_type):
    """Skeleton for decisions.

    :returns: A tuple of two dicts. The first element is the full decision dict
              with ``"decisionType"`` and an attributes key. The second element
              is a reference to the attributes.
    """
    attributes_key = _attr_key_lookup[decision_type]
    attributes_dict = {}
    decision = {'decisionType': decision_type, attributes_key: attributes_dict}
    return decision, attributes_dict
