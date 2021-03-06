.. examples-arithmetic

==================
Arithmetic Example
==================

Utilities 
---------

Ids for workflows and activities are created behind the scenes by
inspecting inputs. ``get_id_from_input`` beneath assumes that the input
has an "id" key and appends this value to the class name of the workflow
or activity::

    import flowser

    @classmethod
    def get_id_from_input(cls, input):
        return ".".join([cls.name, input['id']])

This function sets version and task list properties on decorated
classes. It also makes classes use our ``get_id_from_input`` function
defined earlier::

    def auto_configured(cls):
        """Class decorator for workflows and activities. """
        cls.version = '1.0.0'
        cls.task_list = '-'.join([cls.name, cls.version])
        cls.get_id_from_input = get_id_from_input
        return cls

Domain, Workflow and Activities
-------------------------------

Definition of a workflow that performs arithmetic::

    @auto_configured
    class ArithmeticWorkflow(flowser.types.Workflow):
        """Workflow for performing arithmetics. """
        name = 'ArithmeticWorkflow'
        execution_start_to_close_timeout = '600'
        task_start_to_close_timeout = '120'
        child_policy = 'TERMINATE'

Definition of two activities. These will be scheduled from arithmetic
workflows::

    @auto_configured
    class MultiplyActivity(flowser.types.Activity):
        name = 'MultiplyActivity'
        heartbeat_timeout = '60'
        schedule_to_close_timeout = '60'
        schedule_to_start_timeout = '60'
        start_to_close_timeout = '60'

    @auto_configured
    class SumActivity(flowser.types.Activity):
        name = 'SumActivity'
        heartbeat_timeout = '60'
        schedule_to_close_timeout = '60'
        schedule_to_start_timeout = '60'
        start_to_close_timeout = '60'

Definition of a math domain::

    class MathDomain(flowser.Domain):
        name = 'math'
        workflow_types = [ArithmeticWorkflow]
        activity_types = [MultiplyActivity, SumActivity]

Register domain, workflows and activities::

    import boto
    domain = MathDomain(boto.connect_swf())
    domain.register()

Implementations of worker threads for our workflow and activities::

    import threading

    class ArithmeticWorkflowDecider(threading.Thread):

        def run(self):
            op_to_activity = {
                    'multiply': MultiplyActivity,
                    'sum': SumActivity,
                    }
            for task in domain.decisions(ArithmeticWorkflow):
                is_new = len(task.filter('DecisionTaskScheduled')) == 1
                if is_new:
                    # Schedule tasks from "operations" input. 
                    for op_id, op, input in task.start_input['operations']:
                        activity = op_to_activity[op]
                        task.schedule(activity, {
                            'id': task.start_input['id'], 
                            'operation': [op_id, input],
                            })
                    task.complete()
                else:
                    op_ids = set([x[0] for x in task.start_input['operations']])
                    results = {}
                    for ev in task.filter('ActivityTaskCompleted'):
                        result = ev.attrs['result']
                        results[result[0]] = result[1]
                    result_ids = set(results.keys())

                    got_all = op_ids == result_ids
                    if got_all:
                        # Got results for all operations. Complete workflow.
                        task.workflow_execution.complete(results)
                    else:
                        task.complete()


    class WorkerThread(threading.Thread):

        def run(self):
            for task in domain.activities(self.activity_class):
                self.handle_task(task)


    class MultiplyWorker(WorkerThread):

        activity_class = MultiplyActivity

        def handle_task(self, task):
            op = task.input['operation']
            result = reduce(lambda a, b: a * b, op[1])
            task.complete([op[0], result])


    class SumWorker(WorkerThread):

        activity_class = SumActivity

        def handle_task(self, task):
            op = task.input['operation']
            task.complete([op[0], sum(op[1])])

Starting a Workflow Execution
-----------------------------

Start workers and an execution::

    MultiplyWorker().start()
    SumWorker().start()
    ArithmeticWorkflowDecider().start()

    import uuid
    arithmetic_input = {
        'id': str(uuid.uuid4()),
        'operations': [
            ['mult_id', 'multiply', [1, 2, 3]],
            ['sum_id', 'sum', [1, 2, 3, 4]],
            ],
        }
    domain.start(ArithmeticWorkflow, arithmetic_input)
