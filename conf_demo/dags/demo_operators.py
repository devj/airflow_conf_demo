from airflow.utils.decorators import apply_defaults
from airflow.contrib.operators.qubole_operator import QuboleOperator
from airflow.contrib.hooks.qubole_hook import QuboleHook
from cStringIO import StringIO

from operators.mojave_pseudo_operator import MojavePseudoOperator
from util.utils import xcom_result


class DemoPythonOperator(MojavePseudoOperator):
    def __init__(self, id=None, **kwargs):
        super(DemoPythonOperator, self).__init__(id, **kwargs)
        self.run = kwargs.get('run')

    def call(self, dag):
        if not self.config_task:
            raise Exception("Config is must")

        t = PythonOperator(
            task_id=self.id,
            python_callable=self.run,
            provide_context=True,
            templates_dict={

            },
            dag=dag
        )
        # The real operator is the PythonOperator
        self.task = t
        return t

    def default_format_function(self, record, var=None, config=None):
        return record

    @abc.abstractmethod
    def run(self, **context):
        raise NotImplementedError(
            "The 'run' method of the '{}' MySqlOperator was not implemented".format(self.id))
        return

    @abc.abstractmethod
    def get_results(self):
        raise NotImplementedError(
            "The 'run' method of the '{}' MySqlOperator was not implemented".format(self.id))
        return


class DemoQuboleOperator(MojavePseudoOperator):
    def __init__(self, id=None, **kwargs):
        super(DemoQuboleOperator, self).__init__(id, **kwargs)
        self.run = kwargs.get('run')

    def call(self, dag):
        if not self.config_task:
            raise Exception("Config is must")

        t = PythonOperator(
            task_id=self.id,
            python_callable=self.run,
            provide_context=True,
            templates_dict={

            },
            dag=dag
        )
        # The real operator is the PythonOperator
        self.task = t
        return t


    def default_format_function(self, record, var=None, config=None):
        return record

    @abc.abstractmethod
    def run(self, **context):
        raise NotImplementedError(
            "The 'run' method of the '{}' MySqlOperator was not implemented".format(self.id))
        return

    @abc.abstractmethod
    def get_results(self):
        self.hook = QuboleHook(*self.args, **self.kwargs)
        self.hook.execute(context)
        cmd = self.hook.cmd

        if cmd is not None:
            query_result_buffer = StringIO()
            cmd.get_results(fp=query_result_buffer, inline=True)
            query_result = query_result_buffer.getvalue().strip()
            query_result_buffer.close()
            row_list = filter(None, query_result.split('\r\n'))
            return row_list
