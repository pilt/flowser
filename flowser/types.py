import time

from boto.swf.exceptions import SWFTypeAlreadyExistsError

from flowser import serializing
from flowser import tasks
from flowser.exceptions import Error
from flowser.exceptions import EmptyTaskPollResult
from flowser import decisions

ONE_HOUR = 60 * 60
ONE_DAY = ONE_HOUR * 24


def _raise_if_empty_poll_result(result):
    """Return result or raise ``EmptyTaskPollResult``. """
    if 'taskToken' not in result:
        raise EmptyTaskPollResult('empty result (no task token)')
    return result


class Type(object):
    """Base class for Simple Workflow types (activities, workflows).

    Subclasses must set ``name``, ``task_list`` and ``version`` properties.
    """

    # Override this in a subclass to the name (string) of a register method on
    # the connection object (as returned by boto.connect_swf).
    _reg_func_name = None

    def __init__(self, domain):
        for needed_prop in ['name', 'task_list', 'version']:
            if not hasattr(self, needed_prop):
                raise Error(needed_prop)
        self.domain = domain

    @property
    def conn(self):
        return self.domain.conn

    def register(self, raise_exists=False):
        assert self._reg_func_name is not None, "no reg func configured"
        reg_func = getattr(self.conn, self._reg_func_name)
        try:
            reg_func(self.domain.name, self.name, self.version)
        except SWFTypeAlreadyExistsError:
            if raise_exists:
                raise Error(self)

    def _poll_for_activity_task(self, identity=None):
        """Low-level wrapper for boto's method with the same name. 

        This method raises an exception if no task is returned.

        :raises: EmptyTaskPollResult
        """
        result = self.conn.poll_for_activity_task(
                self.domain.name, self.task_list, identity)
        return _raise_if_empty_poll_result(result)

    def _poll_for_decision_task(self, identity=None, maximum_page_size=None, 
                               next_page_token=None, reverse_order=None):
        """Low-level wrapper for boto's method with the same name. 

        This method raises an exception if no task is returned.

        :raises: EmptyTaskPollResult
        """
        result = self.conn.poll_for_decision_task( 
                self.domain.name, self.task_list, identity, maximum_page_size, 
                next_page_token, reverse_order)
        return _raise_if_empty_poll_result(result)

    @property
    def activities(self):
        """High-level interface to iterate over activity tasks.

        This method polls for new tasks indefinitely.
        """
        while True:
            try:
                result = self._poll_for_activity_task()
            except EmptyTaskPollResult:
                continue
            else:
                yield tasks.Activity(result, domain=self.domain)

    @property
    def decisions(self):
        """High-level interface to iterate over decision tasks.

        This method polls for new tasks indefinitely.
        """
        while True:
            try:
                result = self._poll_for_decision_task(reverse_order=True)
            except EmptyTaskPollResult:
                continue
            else:
                yield tasks.Decision(result, domain=self.domain)


class Activity(Type):
    """Base class for activity types. 
    
    Subclasses must set ``name``, ``task_list`` and ``version`` properties and
    implement a ``schedule`` class method.
    """

    _reg_func_name = 'register_activity_type'

    heartbeat_timeout = str(ONE_HOUR)
    schedule_to_close_timeout = str(ONE_HOUR)
    schedule_to_start_timeout = str(ONE_HOUR)
    start_to_close_timeout = str(ONE_HOUR)

    @classmethod
    def _schedule(cls, activity_id, input, control=None):
        "Called from subclasses' ``schedule`` class method. "
        dec, attrs = decisions.skeleton("ScheduleActivityTask")
        attrs['activityId'] = activity_id
        attrs['activityType'] = {
                'name': cls.name,
                'version': cls.version}
        attrs['taskList'] = {'name': cls.task_list}
        attrs['input'] = serializing.dumps(input)
        attrs['heartbeatTimeout'] = cls.heartbeat_timeout
        attrs['scheduleToCloseTimeout'] = cls.schedule_to_close_timeout
        attrs['scheduleToStartTimeout'] = cls.schedule_to_start_timeout
        attrs['startToCloseTimeout'] = cls.start_to_close_timeout
        if control is not None:
            attrs['control'] = serializing.dumps(control)
        return dec

    @classmethod
    def _request_cancel(cls, activity_id):
        dec, attrs = decisions.skeleton("RequestCancelActivityTask")
        attrs['activityId'] = activity_id
        return dec


class Workflow(Type):
    """Base class for workflow types.

    Subclasses must set ``name`` and ``task_list`` properties and implement 
    a ``start`` method and a ``start_child`` class method.
    """

    _reg_func_name = 'register_workflow_type'

    # These may be overridden in subclasses.
    execution_start_to_close_timeout = '600'
    task_start_to_close_timeout = '120'
    child_policy = 'TERMINATE'
    default_filter_tag = None
    default_tag_list = None

    def _get_static_start_kwargs(self):
        "Get start execeution arguments that never change. "
        return {
                'domain': self.domain.name,
                'workflow_name': self.name,
                'workflow_version': self.version,
                'execution_start_to_close_timeout': \
                    self.execution_start_to_close_timeout,
                'task_start_to_close_timeout': \
                    self.task_start_to_close_timeout,
                'child_policy': self.child_policy,
                'tag_list': self.default_tag_list,
                'task_list': self.task_list,
                }

    @classmethod
    def _get_static_child_start_attrs(cls):
        attrs = {}
        attrs['childPolicy'] = cls.child_policy
        attrs['executionStartToCloseTimeout'] = \
                cls.execution_start_to_close_timeout
        attrs['workflowType'] = {'name': cls.name, 'version': cls.version}
        if cls.default_tag_list:
            attrs['tagList'] = cls.default_tag_list
        attrs['taskList'] = {'name': cls.task_list}
        attrs['taskStartToCloseTimeout'] = \
                cls.task_start_to_close_timeout
        return attrs

    def _list_open(self, latest_date=None, oldest_date=None):
        if latest_date is None:
            latest_date = time.time()
        if oldest_date is None:
            oldest_date = latest_date - ONE_DAY
        return self.conn.list_open_workflow_executions(
                self.domain.name,
                latest_date=latest_date,
                oldest_date=oldest_date,
                workflow_name=self.name,
                tag=self.default_filter_tag)

    def _list_closed(self, start_latest_date=None, start_oldest_date=None):
        if start_latest_date is None:
            start_latest_date = time.time()
        if start_oldest_date is None:
            start_oldest_date = start_latest_date - ONE_DAY
        return self.conn.list_closed_workflow_executions(
                self.domain.name,
                start_latest_date=start_latest_date,
                start_oldest_date=start_oldest_date,
                workflow_name=self.name,
                tag=self.default_filter_tag)

    def _start(self, workflow_id, execution_input=None):
        """Start workflow execution. ``execution_info`` is serialized.

        A subclass should call this method from its ``start`` method.
        """
        kwargs = self._get_static_start_kwargs()
        kwargs['workflow_id'] = workflow_id
        kwargs['input'] = serializing.dumps(execution_input)
        return self.conn.start_workflow_execution(**kwargs)

    @classmethod
    def _start_child(cls, workflow_id, input, control=None):
        """Start child workflow.

        A subclass should call this method from its ``start_child`` class 
        method.
        """
        dec, attrs = decisions.skeleton("StartChildWorkflowExecution")
        attrs.update(cls._get_static_child_start_attrs())
        attrs['workflowId'] = workflow_id
        attrs['input'] = serializing.dumps(input)
        if control is not None:
            attrs['control'] = serializing.dumps(control)
        return dec
