from flowser import serializing
from flowser import decisions
from flowser.events import Event
from flowser.exceptions import Error


class WorkflowExecution(object):
    """Wrapper for the API data type.

    See http://docs.amazonwebservices.com/amazonswf/latest/apireference/API_WorkflowExecution.html.
    """

    def __init__(self, result, domain=None):
        self.run_id = result['runId']
        self.workflow_id = result['workflowId']
        self._domain = domain

    def __str__(self):
        return "run_id(%s) workflow_id(%s)" % (self.run_id, self.workflow_id)

    def __repr__(self):
        return "<WorkflowExecution %s>" % self

    def request_cancel(self):
        if self._domain is None:
            raise Error("domain missing")
        self._domain.conn.request_cancel(self._domain.name, self.workflow_id, 
                                         run_id=self.run_id)

    def signal(self, name, input=None):
        if self._domain is None:
            raise Error("domain missing")
        serialized_input = None
        if input is not None:
            serialized_input = serializing.dumps(input)
        self._domain.conn.signal_workflow_execution(
                self._domain.name, name, self.workflow_id, 
                input=serialized_input, run_id=self.run_id)

    def terminate(self, details=None, reason=None):
        self._terminate('TERMINATE', details, reason)

    def abandon(self, details=None, reason=None):
        self._terminate('ABANDON', details, reason)

    def terminate_request_cancel(self, details=None, reason=None):
        # XXX sp: How is this different from RequestCancelWorkflowExecution?
        # See:
        #
        #  * http://docs.amazonwebservices.com/amazonswf/latest/apireference/API_RequestCancelWorkflowExecution.html
        #  * http://docs.amazonwebservices.com/amazonswf/latest/apireference/API_TerminateWorkflowExecution.html
        #
        # It seems that the RequestCancelelWorkflowExecution API call is 
        # preferable because it allows the workflow to gracefully close whereas
        # the terminate call does not.
        self._terminate('REQUEST_CANCEL', details, reason)

    def _terminate(self, child_policy, details, reason):
        if self._domain is None:
            raise Error("domain missing")
        self._domain.conn.terminate_workflow_execution(
                self._domain.name, self.workflow_id, 
                child_policy=child_policy, details=details, reason=reason,
                run_id=self.run_id)


class WorkflowType(object):
    """Wrapper for the API data type.

    See http://docs.amazonwebservices.com/amazonswf/latest/apireference/API_WorkflowType.html.
    """

    def __init__(self, result):
        self.name = result['name']
        self.version = result['version']

    def __str__(self):
        return "%s (%s)" % (self.name, self.version)

    def __repr__(self):
        return "<WorkflowType %s>" % self


class ActivityType(object):
    """Wrapper for the API data type.

    See http://docs.amazonwebservices.com/amazonswf/latest/apireference/API_ActivityType.html.
    """

    def __init__(self, result):
        self.name = result['name']
        self.version = result['version']

    def __str__(self):
        return "%s (%s)" % (self.name, self.version)

    def __repr__(self):
        return "<ActivityType %s>" % self


class Decision(object):
    """Wrapper for "PollForDecisionTask" results.

    See http://docs.amazonwebservices.com/amazonswf/latest/apireference/API_PollForDecisionTask.html.

    This class assumes that history events are in reverse order (most recent
    first).
    """

    def __init__(self, result, domain=None):
        """
        :param result: Result structure from the API. 
        :param domain: A domain instance (optional). Needed for responses.
        """
        self.events = [Event(r) for r in result['events']]
        self.next_page_token = result.get('nextPageToken', None)
        self.previous_started_event_id = result['previousStartedEventId']
        self.started_event_id = result['startedEventId']
        self.task_token = result['taskToken']
        self.workflow_execution = WorkflowExecution(
                result['workflowExecution'])
        self.workflow_type = WorkflowType(result['workflowType'])

        self._decisions = []
        self._domain = domain

    def __repr__(self):
        return "<Decision workflow_type(%s) %s>" % (
                self.workflow_type, self.workflow_execution)

    def most_recent(self, event_type):
        # FIXME sp: Handle pagination of events.
        for event in self.events:
            if event.type == event_type:
                return event
        raise Error('%s event not found' % event_type)

    @property
    def start_input(self):
        started_event = self.most_recent('WorkflowExecutionStarted')
        return serializing.loads(started_event.attrs['input'])

    def mark(self, name, details=None):
        """Adds a RecordMarker decision. """
        dec, attrs = decisions.skeleton("RecordMarker")
        attrs['markerName'] = name
        if details:
            attrs['details'] = details
        self._decisions.append(dec)
        return self

    def schedule(self, activity_type, *args, **kwargs):
        """Schedule activity. 

        Internally, this method calls the schedule classmethod on the 
        activity type with the given args and kwargs.

        :param activity_type: Subclass of ``types.Activity``.
        """
        dec = activity_type.schedule(*args, **kwargs)
        self._decisions.append(dec)
        return self

    def start_child(self, workflow_type, *args, **kwargs):
        """Start child workflow. 

        Internally, this method calls the start_child classmethod on the 
        workflow type with the given args and kwargs.

        :param workflow_type: Subclass of ``types.Workflow``.
        """
        dec = workflow_type.start_child(*args, **kwargs)
        self._decisions.append(dec)
        return self

    def complete(self, context=None):
        if self._domain is None:
            raise Error("domain missing")
        execution_context = None
        if context is not None:
            execution_context = serializing.dumps(context)
        self._domain.conn.respond_decision_task_completed(
                self.task_token, decisions=self._decisions,
                execution_context=execution_context)

    def fail(self, details=None, reason=None):
        if self._domain is None:
            raise Error("domain missing")
        self._domain.conn.respond_decision_task_failed(
                self.task_token, details=details, reason=reason)


class Activity(object):
    """Wrapper for "PollForActivityTask" results.

    See http://docs.amazonwebservices.com/amazonswf/latest/apireference/API_PollForActivityTask.html.
    """

    def __init__(self, result, domain=None):
        """
        :param result: Result structure from the API. 
        :param domain: A domain instance (optional). Needed for responses.
        """
        self.activity_id = result['activityId']
        self.activity_type = ActivityType(result['activityType'])
        self.input = serializing.loads(result['input'])
        self.started_event_id = result['startedEventId']
        self.task_token = result['taskToken']
        self.workflow_execution = WorkflowExecution(
                result['workflowExecution'])

        self._domain = domain

    def __repr__(self):
        return "<Activity activity_type(%s) %s>" % (
                self.activity_type, self.workflow_execution)

    def complete(self, result=None):
        if self._domain is None:
            raise Error("domain missing")
        serialized_result = None
        if result is not None:
            serialized_result = serializing.dumps(result)
        return self._domain.conn.respond_activity_task_completed(
                self.task_token, result=serialized_result)

    def fail(self, details=None, reason=None):
        if self._domain is None:
            raise Error("domain missing")
        self._domain.conn.respond_activity_task_failed(
                self.task_token, details=details, reason=reason)

    def cancel(self, details=None):
        if self._domain is None:
            raise Error("domain missing")
        self._domain.conn.respond_activity_task_canceled(
                self.task_token, details=details)
