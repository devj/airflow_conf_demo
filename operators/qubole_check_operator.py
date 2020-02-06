# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from airflow.contrib.operators.qubole_operator import QuboleOperator
from airflow.utils.decorators import apply_defaults
from data_app.hooks.qubole_check_hook import QuboleCheckHook
from data_app.operators.check_operator import CheckOperator, ValueCheckOperator
from airflow.exceptions import AirflowException


class QuboleCheckOperator(CheckOperator, QuboleOperator):
    """
    Performs checks against Qubole Commands. The ``QuboleueryCheckOperator`` expects
    a qubole command that will return a single row. Each value on that
    first row is evaluated using python ``bool`` casting. If any of the
    values return ``False`` the check is failed and errors out.

    Note that Python bool casting evals the following as ``False``:

    * ``False``
    * ``0``
    * Empty string (``""``)
    * Empty list (``[]``)
    * Empty dictionary or set (``{}``)

    Given a query like ``SELECT COUNT(*) FROM foo``, it will fail only if
    the count ``== 0``. You can craft much more complex query that could,
    for instance, check that the table has the same number of rows as
    the source table upstream, or that the count of today's partition is
    greater than yesterday's partition, or that a set of metrics are less
    than 3 standard deviation for the 7 day average.

    This operator can be used as a data quality check in your pipeline, and
    depending on where you put it in your DAG, you have the choice to
    stop the critical path, preventing from
    publishing dubious data, or on the side and receive email alerts
    without stopping the progress of the DAG.

    :param qubole_conn_id: Connection id which consists of qds auth_token
    :type qubole_conn_id: str

    kwargs:
        :command_type: type of command to be executed, e.g. hivecmd, shellcmd, hadoopcmd
        :tags: array of tags to be assigned with the command
        :cluster_label: cluster label on which the command will be executed
        :name: name to be given to command
        :notify: whether to send email on command completion or not (default is False)
        :results_parser_callable: a python_callable which parses a list of rows
        returned by the qubole command. By default, only the first
        row is parsed. The python_callable should return a list of records
        on which the checks are performed.

        **Arguments specific to command types**

        hivecmd:
            :query: inline query statement
            :script_location: s3 location containing query statement
            :sample_size: size of sample in bytes on which to run query
            :macros: macro values which were used in query
        prestocmd:
            :query: inline query statement
            :script_location: s3 location containing query statement
            :macros: macro values which were used in query
        hadoopcmd:
            :sub_commnad: must be one these ["jar", "s3distcp", "streaming"] followed by
                1 or more args
        shellcmd:
            :script: inline command with args
            :script_location: s3 location containing query statement
            :files: list of files in s3 bucket as file1,file2 format. These files will be
                copied into the working directory where the qubole command is being
                executed.
            :archives: list of archives in s3 bucket as archive1,archive2 format. These
                will be unarchived intothe working directory where the qubole command is
                being executed
            :parameters: any extra args which need to be passed to script (only when
                script_location is supplied)
        pigcmd:
            :script: inline query statement (latin_statements)
            :script_location: s3 location containing pig query
            :parameters: any extra args which need to be passed to script (only when
                script_location is supplied
        sparkcmd:
            :program: the complete Spark Program in Scala, SQL, Command, R, or Python
            :cmdline: spark-submit command line, all required information must be specify
                in cmdline itself.
            :sql: inline sql query
            :script_location: s3 location containing query statement
            :language: language of the program, Scala, SQL, Command, R, or Python
            :app_id: ID of an Spark job server app
            :arguments: spark-submit command line arguments
            :user_program_arguments: arguments that the user program takes in
            :macros: macro values which were used in query
        dbtapquerycmd:
            :db_tap_id: data store ID of the target database, in Qubole.
            :query: inline query statement
            :macros: macro values which were used in query
        dbexportcmd:
            :mode: 1 (simple), 2 (advance)
            :hive_table: Name of the hive table
            :partition_spec: partition specification for Hive table.
            :dbtap_id: data store ID of the target database, in Qubole.
            :db_table: name of the db table
            :db_update_mode: allowinsert or updateonly
            :db_update_keys: columns used to determine the uniqueness of rows
            :export_dir: HDFS/S3 location from which data will be exported.
            :fields_terminated_by: hex of the char used as column separator in the dataset
        dbimportcmd:
            :mode: 1 (simple), 2 (advance)
            :hive_table: Name of the hive table
            :dbtap_id: data store ID of the target database, in Qubole.
            :db_table: name of the db table
            :where_clause: where clause, if any
            :parallelism: number of parallel db connections to use for extracting data
            :extract_query: SQL query to extract data from db. $CONDITIONS must be part
                of the where clause.
            :boundary_query: Query to be used get range of row IDs to be extracted
            :split_column: Column used as row ID to split data into ranges (mode 2)

    .. note:: Following fields are template-supported : ``query``, ``script_location``,
        ``sub_command``, ``script``, ``files``, ``archives``, ``program``, ``cmdline``,
        ``sql``, ``where_clause``, ``extract_query``, ``boundary_query``, ``macros``,
        ``tags``, ``name``, ``parameters``, ``dbtap_id``, ``hive_table``, ``db_table``,
        ``split_column``, ``db_update_keys``, ``export_dir``, ``partition_spec``. You
        can also use ``.txt`` files for template driven use cases.
    """

    template_fields = QuboleOperator.template_fields + CheckOperator.template_fields
    template_ext = QuboleOperator.template_ext
    ui_fgcolor = '#000'

    @apply_defaults
    def __init__(self, qubole_conn_id="qubole_default", *args, **kwargs):
        sql = get_sql_from_qbol_cmd(kwargs)
        super(QuboleCheckOperator, self).__init__(qubole_conn_id=qubole_conn_id, sql=sql, *args, **kwargs)
        self.on_failure_callback = QuboleCheckHook.handle_failure_retry
        self.on_retry_callback = QuboleCheckHook.handle_failure_retry

    def execute(self, context=None):
        try:
            self.hook = QuboleCheckHook(context=context, *self.args, **self.kwargs)
            super(QuboleCheckOperator, self).execute(context=context)
        except AirflowException as e:
            cmd = self.hook.cmd
            if cmd is not None:
                if cmd.status == 'done':
                    qubole_command_results = self.hook.get_query_results()
                    qubole_command_id = cmd.id
                    exception_message = '\nQubole Command Id: {qubole_command_id}' \
                                        '\nQubole Command Results:\n{qubole_command_results}'.format(**locals())
                    raise AirflowException(e.message + exception_message)
            raise AirflowException(e.message)

    def get_db_hook(self):
        return self.hook

    def __getattribute__(self, name):
        if name in QuboleCheckOperator.template_fields:
            if name in self.kwargs:
                return self.kwargs[name]
            else:
                return ''
        else:
            return object.__getattribute__(self, name)

    def __setattr__(self, name, value):
        if name in QuboleCheckOperator.template_fields:
            self.kwargs[name] = value
        else:
            object.__setattr__(self, name, value)


class QuboleValueCheckOperator(ValueCheckOperator, QuboleOperator):
    """
    Performs a simple value check using qubole command.

    :param qubole_conn_id: Connection id which consists of qds auth_token
    :type qubole_conn_id: str

    kwargs:
        :command_type: type of command to be executed, e.g. hivecmd, shellcmd, hadoopcmd
        :tags: array of tags to be assigned with the command
        :cluster_label: cluster label on which the command will be executed
        :name: name to be given to command
        :notify: whether to send email on command completion or not (default is False)
        :results_parser_callable: a python_callable which parses a list of rows
        returned by the qubole command. By default, only the first
        row is parsed. The python_callable should return a list of records
        on which the checks are performed.

        **Arguments specific to command types**

        hivecmd:
            :query: inline query statement
            :script_location: s3 location containing query statement
            :sample_size: size of sample in bytes on which to run query
            :macros: macro values which were used in query
        prestocmd:
            :query: inline query statement
            :script_location: s3 location containing query statement
            :macros: macro values which were used in query
        hadoopcmd:
            :sub_commnad: must be one these ["jar", "s3distcp", "streaming"] followed by
                1 or more args
        shellcmd:
            :script: inline command with args
            :script_location: s3 location containing query statement
            :files: list of files in s3 bucket as file1,file2 format. These files will be
                copied into the working directory where the qubole command is being
                executed.
            :archives: list of archives in s3 bucket as archive1,archive2 format. These
                will be unarchived intothe working directory where the qubole command is
                being executed
            :parameters: any extra args which need to be passed to script (only when
                script_location is supplied)
        pigcmd:
            :script: inline query statement (latin_statements)
            :script_location: s3 location containing pig query
            :parameters: any extra args which need to be passed to script (only when
                script_location is supplied
        sparkcmd:
            :program: the complete Spark Program in Scala, SQL, Command, R, or Python
            :cmdline: spark-submit command line, all required information must be specify
                in cmdline itself.
            :sql: inline sql query
            :script_location: s3 location containing query statement
            :language: language of the program, Scala, SQL, Command, R, or Python
            :app_id: ID of an Spark job server app
            :arguments: spark-submit command line arguments
            :user_program_arguments: arguments that the user program takes in
            :macros: macro values which were used in query
        dbtapquerycmd:
            :db_tap_id: data store ID of the target database, in Qubole.
            :query: inline query statement
            :macros: macro values which were used in query
        dbexportcmd:
            :mode: 1 (simple), 2 (advance)
            :hive_table: Name of the hive table
            :partition_spec: partition specification for Hive table.
            :dbtap_id: data store ID of the target database, in Qubole.
            :db_table: name of the db table
            :db_update_mode: allowinsert or updateonly
            :db_update_keys: columns used to determine the uniqueness of rows
            :export_dir: HDFS/S3 location from which data will be exported.
            :fields_terminated_by: hex of the char used as column separator in the dataset
        dbimportcmd:
            :mode: 1 (simple), 2 (advance)
            :hive_table: Name of the hive table
            :dbtap_id: data store ID of the target database, in Qubole.
            :db_table: name of the db table
            :where_clause: where clause, if any
            :parallelism: number of parallel db connections to use for extracting data
            :extract_query: SQL query to extract data from db. $CONDITIONS must be part
                of the where clause.
            :boundary_query: Query to be used get range of row IDs to be extracted
            :split_column: Column used as row ID to split data into ranges (mode 2)

    .. note:: Following fields are template-supported : ``query``, ``script_location``,
        ``sub_command``, ``script``, ``files``, ``archives``, ``program``, ``cmdline``,
        ``sql``, ``where_clause``, ``extract_query``, ``boundary_query``, ``macros``,
        ``tags``, ``name``, ``parameters``, ``dbtap_id``, ``hive_table``, ``db_table``,
        ``split_column``, ``db_update_keys``, ``export_dir``, ``partition_spec``,
        ``pass_value``. You can also use ``.txt`` files for template driven use cases.
    """

    template_fields = QuboleOperator.template_fields + ValueCheckOperator.template_fields
    template_ext = QuboleOperator.template_ext
    ui_fgcolor = '#000'

    @apply_defaults
    def __init__(self, pass_value, tolerance=None, qubole_conn_id="qubole_default", *args, **kwargs):

        sql = get_sql_from_qbol_cmd(kwargs)
        super(QuboleValueCheckOperator, self).__init__(
            qubole_conn_id=qubole_conn_id,
            sql=sql, pass_value=pass_value, tolerance=tolerance,
            *args, **kwargs)

        self.on_failure_callback = QuboleCheckHook.handle_failure_retry
        self.on_retry_callback = QuboleCheckHook.handle_failure_retry

    def execute(self, context=None):
        try:
            self.hook = QuboleCheckHook(context=context, *self.args, **self.kwargs)
            super(QuboleValueCheckOperator, self).execute(context=context)
        except AirflowException as e:
            cmd = self.hook.cmd
            if cmd is not None:
                if cmd.status == 'done':
                    qubole_command_results = self.hook.get_query_results()
                    qubole_command_id = cmd.id
                    exception_message = '\nQubole Command Id: {qubole_command_id}' \
                                        '\nQubole Command Results:\n{qubole_command_results}'.format(**locals())
                    raise AirflowException(e.message + exception_message)
            raise AirflowException(e.message)


    def get_db_hook(self):
        return self.hook

    def __getattribute__(self, name):
        if name in QuboleValueCheckOperator.template_fields:
            if name in self.kwargs:
                return self.kwargs[name]
            else:
                return ''
        else:
            return object.__getattribute__(self, name)

    def __setattr__(self, name, value):
        if name in QuboleValueCheckOperator.template_fields:
            self.kwargs[name] = value
        else:
            object.__setattr__(self, name, value)


def get_sql_from_qbol_cmd(params):
    sql = ''
    if 'query' in params:
        sql = params['query']
    elif 'sql' in params:
        sql = params['sql']
    return sql
