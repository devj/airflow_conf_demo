from airflow.utils.decorators import apply_defaults
from airflow.contrib.operators.qubole_operator import QuboleOperator
from airflow.contrib.hooks.qubole_hook import QuboleHook
from cStringIO import StringIO


class QuboleResultOperator(QuboleOperator):
    """
    Execute tasks (commands) on QDS (https://qubole.com).
    Push the results of these tasks to
    an xcom named 'qbol_cmd_results' if the number
    of rows in the results are less than or equal to 10
    """

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(QuboleResultOperator, self).__init__(*args, **kwargs)

    def execute(self, context):

        self.hook = QuboleHook(*self.args, **self.kwargs)
        self.hook.execute(context)
        cmd = self.hook.cmd

        ti = context['ti']

        if cmd is not None:
            query_result_buffer = StringIO()
            cmd.get_results(fp=query_result_buffer, inline=True)
            query_result = query_result_buffer.getvalue().strip()
            query_result_buffer.close()
            row_list = filter(None, query_result.split('\r\n'))
            if len(row_list) <= 10:
                ti.xcom_push(key='qbol_cmd_results', value=query_result)
