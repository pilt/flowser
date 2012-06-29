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

from boto.swf.exceptions import SWFDomainAlreadyExistsError

from flowser import tasks
from flowser.exceptions import Error
from flowser.exceptions import EmptyTaskPollResult


class Domain(object):
    """Represents a Simple Workflow domain.

    Subclasses must set a ``name`` property. They may also set a 
    ``retention_period`` property (defaults to '30').

    To register types, ``workflow_types`` and ``activity_types`` need to be
    set. They should be lists of ``types.Workflow`` and ``types.Activity``
    subclasses.
    """

    retention_period = '30'
    workflow_types = None
    activity_types = None

    def __init__(self, conn):
        """
        :param conn: A ``boto.swf`` connection.
        """
        self.conn = conn

    def register(self, raise_exists=False):
        "Register domain and associated types on AWS. " 
        try:
            self.conn.register_domain(self.name, self.retention_period)
        except SWFDomainAlreadyExistsError:
            if raise_exists:
                raise Error(self)
        types = (self.workflow_types or []) + (self.activity_types or [])
        [t(self)._register(raise_exists=raise_exists) for t in types]

    def start(self, t, input):
        """Start execution.

        Internally, this method creates an instance of ``t`` and calls its
        ``start`` method with the given input.

        :param t: Subclass of ``types.Type``.
        """
        return t(self)._start(input)

    def decisions(self, t):
        """High-level interface to iterate over decision tasks.

        This method polls for new tasks of the given type indefinitely.

        :param t: Subclass of ``types.Type``.
        """
        poll_kwargs = {'reverse_order': True}
        return self._poll_indefinitely(
                t, '_poll_for_decision_task', tasks.Decision, poll_kwargs)

    def activities(self, t):
        """High-level interface to iterate over activity tasks.

        This method polls for new tasks of the given type indefinitely.

        :param t: Subclass of ``types.Type``.
        """
        return self._poll_indefinitely(
                t, '_poll_for_activity_task', tasks.Activity)

    def _poll_indefinitely(self, t, method_name, task_class, poll_kwargs=None):
        instance = t(self)
        poll_method = getattr(instance, method_name)
        kwargs = {}
        if poll_kwargs is not None:
            kwargs.update(poll_kwargs)
        while True:
            try:
                result = poll_method(**kwargs)
            except EmptyTaskPollResult:
                continue
            else:
                yield task_class(result, instance)
