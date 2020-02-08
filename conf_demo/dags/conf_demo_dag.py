import logging
from datetime import timedelta, datetime
from functools import partial
import os
import abc
import json
import re

from airflow import DAG

from airflow.operators import BaseOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from airflow.utils.decorators import apply_defaults
try:
    from StringIO import StringIO ## for Python 2
except ImportError:
    from io import StringIO ## for Python 3



def xcom_get(context, task="task0"):
    ti = context['ti']

    pulled_value = ti.xcom_pull(key=None, task_ids=task)
    if not pulled_value:
        raise ValueError('You pushed garbage')

    return json.loads(pulled_value)


class MojavePseudoOperator(object):
    __metaclass__ = abc.ABCMeta
    __instances__ = {}      # dagid.taskid : Operator

    def __init__(self, id=None, **kwargs):
        self.id = self.__class__.__name__ if id is None else id
        self.kwargs = kwargs


    def __call__(self, dag):
        self.dag_id = dag.dag_id
        key = "{}.{}".format(dag.dag_id, self.id)
        task = self.call(dag)

        assert task != None, "Calling '{}' returned 'None' where a 'BaseOperator' " \
                                 "instance was expected.".format(
                self.id)
        assert isinstance(task,
                              BaseOperator), "Calling '{}' did not return a BaseOperator " \
                                             "instance (but a '{}' instead).".format(
                self.id, task.__class__.__name__)
        assert task.task_id == self.id, \
                                   "task_id '{}' must be identical to the FOperator.id '{}'"\
                                              .format(task.task_id, self.id)
        MojavePseudoOperator.__instances__[key] = task
        return task


    def get_task(self):
        key = "{}.{}".format(self.dag_id, self.id)
        return MojavePseudoOperator.__instances__[key]


    def get_vars(self, context):
        vars = None
        try:
            vars = json.loads(get_param(self.upstream_task.id, context))
        except Exception as ex:
            print("xcom not set by the upstream task")

        return vars


    def read_file_data(self, storage_driver):
        data = None
        task_id = self.id
        if not self.upstream_task.parallelism:
            parallelism = get_downstream_parallelism(self.upstream_task)
        else:
            parallelism = self.upstream_task.parallelism

        files = storage_driver.list_files()
        if not files:
            return None

        i = get_task_index(task_id)
        if i < len(files):
            data = storage_driver.read(i)
        elif parallelism == 1:
            data = storage_driver.read(0)

        if not data or len(data) == 0:
            data = None

        return data


    def write_file_data(self, records, storage_driver, num_files):
        if not records or len(records) == 0:
            return

        lindex = 0
        rindex = len(records)/num_files if len(records) > num_files else len(records)
        file_count = 1
        while file_count <= num_files:
            storage_driver.write(file_count, json.dumps(records[lindex : rindex]))
            file_count = file_count + 1
            lindex = rindex


    @abc.abstractmethod
    def call(self, dag):
        raise NotImplementedError(
            "The 'call' method of the '{}' BifrostOperator was not implemented".format(self.id))
        return



class DemoPythonOperator(MojavePseudoOperator):
    def __init__(self, id=None, **kwargs):
        super(DemoPythonOperator, self).__init__(id, **kwargs)
        self.var_name = kwargs.get('variable_name')
        self.run = kwargs.get('run')

    def call(self, dag):
        t = PythonOperator(
            task_id=self.id,
            python_callable=self.run,
            provide_context=True,
            templates_dict={
                "variable_name": self.var_name
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

'''
class DemoQuboleOperator(MojavePseudoOperator):
    def __init__(self, id=None, **kwargs):
        super(DemoQuboleOperator, self).__init__(id, **kwargs)
        self.run = kwargs.get('run')

    def call(self, dag):
        if not self.config_task:
            raise Exception("Config is must")

        t = QuboleOperator(
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
'''






































import logging
import os
import json
import re

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator




log = logging.getLogger(__name__)



default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'wait_for_downstream': True,
    'start_date': datetime.strptime('2020-02-05T00:00:00', '%Y-%m-%dT%H:%M:%S'),
    'email': ['admin@qubole.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'queue': 'default'
}


dag = DAG(
        'meetup_demo',
        default_args=default_args,
        max_active_runs=1,
        schedule_interval='@hourly'
      )



def task0_exec(**context):
    """
    This method fetches the config saved as a Airflow Variable.
    """
    try:
        var = context.get("templates_dict").get("variable_name")
        from airflow.models import Variable
        return Variable.get(var)
    except Exception as exception:
        log.error("Error while trying to fetch the variables from airflow ")
        raise exception


def task1_exec(**context):
    log.info("Executing task 1")

    fname = xcom_get(context, "task0").get("fname")

    wordfreq = {}
    with open(fname, 'r') as f:
        for line in f:
            freq = []
            wordlist = re.split(r'[^\w]', line)
            for w in wordlist:
                freq.append(wordlist.count(w))
                wmap = dict(list(zip(wordlist, freq)))
                wordfreq.update(wmap)

    return json.dumps(wordfreq)


def task2_exec(**context):
    log.info("Executing task 2")
    limit = xcom_get(context, "task0").get("limit")

    word_freq = xcom_get(context, "task1")
    sorted_word_freq = sorted(word_freq, key=word_freq.get, reverse=True)

    return sorted_word_freq[ : limit]


# DAG definition starts here

op0 = DemoPythonOperator("task0", run=task0_exec, variable_name="vars")
op0(dag)

op1 = DemoPythonOperator("task1", run=task1_exec)
op1(dag)

op2 = DemoPythonOperator("task2", run=task2_exec)
op2(dag)

op1.get_task().set_upstream(op0.get_task())
op2.get_task().set_upstream(op1.get_task())
