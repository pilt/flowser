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

"""History events.

The purpose is to make it easier to work with the API.

See http://docs.amazonwebservices.com/amazonswf/latest/apireference/API_HistoryEvent.html.
"""
from flowser import serializing

_event_types = [
        "WorkflowExecutionStarted",
        "WorkflowExecutionCancelRequested",
        "WorkflowExecutionCompleted",
        "CompleteWorkflowExecutionFailed",
        "WorkflowExecutionFailed",
        "FailWorkflowExecutionFailed",
        "WorkflowExecutionTimedOut",
        "WorkflowExecutionCanceled",
        "CancelWorkflowExecutionFailed",
        "WorkflowExecutionContinuedAsNew",
        "ContinueAsNewWorkflowExecutionFailed",
        "WorkflowExecutionTerminated",
        "DecisionTaskScheduled",
        "DecisionTaskStarted",
        "DecisionTaskCompleted",
        "DecisionTaskTimedOut",
        "ActivityTaskScheduled",
        "ScheduleActivityTaskFailed",
        "ActivityTaskStarted",
        "ActivityTaskCompleted",
        "ActivityTaskFailed",
        "ActivityTaskTimedOut",
        "ActivityTaskCanceled",
        "ActivityTaskCancelRequested",
        "RequestCancelActivityTaskFailed",
        "WorkflowExecutionSignaled",
        "MarkerRecorded",
        "TimerStarted",
        "StartTimerFailed",
        "TimerFired",
        "TimerCanceled",
        "CancelTimerFailed",
        "StartChildWorkflowExecutionInitiated",
        "StartChildWorkflowExecutionFailed",
        "ChildWorkflowExecutionStarted",
        "ChildWorkflowExecutionCompleted",
        "ChildWorkflowExecutionFailed",
        "ChildWorkflowExecutionTimedOut",
        "ChildWorkflowExecutionCanceled",
        "ChildWorkflowExecutionTerminated",
        "SignalExternalWorkflowExecutionInitiated",
        "SignalExternalWorkflowExecutionFailed",
        "ExternalWorkflowExecutionSignaled",
        "RequestCancelExternalWorkflowExecutionInitiated",
        "RequestCancelExternalWorkflowExecutionFailed",
        "ExternalWorkflowExecutionCancelRequested",
        ]

def _attr_key_name(s):
    return s[0].lower() + s[1:] + 'EventAttributes'

_attr_keys = map(_attr_key_name, _event_types)
_attr_key_lookup = dict(zip(_event_types, _attr_keys))

_auto_unserialize_attrs = {
        "ActivityTaskCompleted": ['result'],
        }

def attrs(result):
    """Get event attributes.

    :param result: An event structure returned from the API.
    :returns: The attributes dict.
    """
    event_type = result['eventType']
    attributes_key = _attr_key_lookup[event_type]
    ev_attrs = result[attributes_key]
    for key in _auto_unserialize_attrs.get(event_type, []):
        value = ev_attrs[key]
        try:
            ev_attrs[key] = serializing.loads(value)
        except TypeError:
            pass
    return ev_attrs


class Event(object):

    def __init__(self, result):
        self.id = result['eventId']
        self.time_stamp = result['eventTimestamp']
        self.type = result['eventType']
        self.attrs = attrs(result)

    def __repr__(self):
        return "<Event id(%s) type(%s) time_stamp(%s)>" % (
                self.id, self.type, self.time_stamp)
