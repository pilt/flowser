# Copyright (c) 2012 Memoto AB
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to
# deal in the Software without restriction, including without limitation the
# rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
# sell copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

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
