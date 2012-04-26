from boto.swf.exceptions import SWFDomainAlreadyExistsError

from flowser.exceptions import Error
from flowser import types


class Domain(object):
    """Represents a Simple Workflow domain.

    Subclasses must set a ``name`` property. They may also set a 
    ``retention_period`` property (defaults to '30').

    Activity and workflow instances are available in two dict attributes on
    domain instances: ``activity`` and ``workflow``. Keys are classes and values
    are instances of activity and workflow types bound to the domain.
    """

    retention_period = '30'

    def __init__(self, conn, activities, workflows):
        """
        :param conn: A ``boto.swf`` connection.
        :param activities: List of activity types (subclasses of
                           ``types.Activity``.
        :param workflows: List of workflow types (subclasses of
                           ``types.Workflow``.
        """
        self.conn = conn

        # Bind activities and workflows to domain.
        self.activity = {}
        for act_class in activities:
            self.activity[act_class] = act_class(self)
        self.workflow = {}
        for workflow_class in workflows:
            self.workflow[workflow_class] = workflow_class(self)

    def register(self, raise_exists=False):
        "Register domain and associated types on AWS. " 
        try:
            self.conn.register_domain(self.name, self.retention_period)
        except SWFDomainAlreadyExistsError:
            if raise_exists:
                raise Error(self)
        for t in self.activity.values() + self.workflow.values():
            t.register(raise_exists=raise_exists)
