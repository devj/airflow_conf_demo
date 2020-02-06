import abc
import json

from data_app.utility.qubole_utils import get_task_index, xcom_result, get_param, \
                                            get_downstream_parallelism
from data_app.utility.file_storage import FileStorageDriver, S3StorageDriver

from airflow.operators import BaseOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


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


    def get_task(self, dag, task):
        key = "{}.{}".format(dag.dag_id, task.id)
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
        rindex = if len(records) > num_files len(records) / num_files else len(records)
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
