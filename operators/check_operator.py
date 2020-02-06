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

from builtins import zip
from builtins import str
import logging

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CheckOperator(BaseOperator):
    """
    Performs checks against a db. The ``CheckOperator`` expects
    a sql query that will return a single row. Each value on that
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

    Note that this is an abstract class and get_db_hook
    needs to be defined. Whereas a get_db_hook is hook that gets a
    single record from an external source.

    :param sql: the sql to be executed
    :type sql: string
    """

    template_fields = ('sql',)
    template_ext = ('.hql', '.sql',)
    ui_color = '#fff7e6'

    @apply_defaults
    def __init__(
            self, sql,
            conn_id=None,
            *args, **kwargs):
        super(CheckOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.sql = sql

    def execute(self, context=None):
        logging.info('Executing SQL check: ' + self.sql)
        records = self.get_db_hook().get_first(self.sql)
        logging.info("Record: " + str(records))
        if not records:
            raise AirflowException("The query returned None")
        elif not all([bool(r) for r in records]):
            exceptstr = "Test failed.\nQuery:\n{q}\nRecords checked:\n{r!s}"
            raise AirflowException(exceptstr.format(q=self.sql, r=records))
        logging.info("Success.")

    def get_db_hook(self):
        return BaseHook.get_hook(conn_id=self.conn_id)


def _convert_to_float_if_possible(s):
    '''
    A small helper function to convert a string to a numeric value
    if appropriate

    :param s: the string to be converted
    :type s: str
    '''
    try:
        ret = float(s)
    except (ValueError, TypeError):
        ret = s
    return ret


class ValueCheckOperator(BaseOperator):
    """
    Performs a simple value check using sql code.

    Note that this is an abstract class and get_db_hook
    needs to be defined. Whereas a get_db_hook is hook that gets a
    single record from an external source.

    :param sql: the sql to be executed
    :type sql: string
    """

    __mapper_args__ = {
        'polymorphic_identity': 'ValueCheckOperator'
    }
    template_fields = ('sql', 'pass_value',)
    template_ext = ('.hql', '.sql',)
    ui_color = '#fff7e6'

    @apply_defaults
    def __init__(
            self, sql, pass_value, tolerance=None,
            conn_id=None,
            *args, **kwargs):
        super(ValueCheckOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.conn_id = conn_id
        self.pass_value = str(pass_value)
        tol = _convert_to_float_if_possible(tolerance)
        self.tol = tol if isinstance(tol, float) else None
        self.has_tolerance = self.tol is not None


    def execute(self, context=None):
        logging.info('Executing SQL check: ' + self.sql)
        records = self.get_db_hook().get_first(self.sql)
        if not records:
            raise AirflowException("The query returned None")

        pass_value_conv = _convert_to_float_if_possible(self.pass_value)
        is_numeric_value_check = isinstance(pass_value_conv, float)

        tolerance_pct_str = None
        if (self.tol is not None):
            tolerance_pct_str = str(self.tol * 100) + '%'

        except_temp = "Test failed.\nPass value:{pass_value_conv}\n" \
                      "Tolerance:{tolerance_pct_str}\n" \
                      "Query:\n{self.sql}\nRecords checked:\n{records!s}"
        if not is_numeric_value_check:
            tests = [str(r) == pass_value_conv for r in records]
        elif is_numeric_value_check:
            try:
                num_rec = [float(r) for r in records]
            except (ValueError, TypeError) as e:
                cvestr = "Converting a result to float failed.\n"
                raise AirflowException(cvestr+except_temp.format(**locals()))
            if self.has_tolerance:
                tests = [
                    pass_value_conv * (1 - self.tol) <= r <= pass_value_conv * (1 + self.tol)
                    for r in num_rec]
            else:
                tests = [r == pass_value_conv for r in num_rec]
        if not all(tests):
            raise AirflowException(except_temp.format(**locals()))

    def get_db_hook(self):
        return BaseHook.get_hook(conn_id=self.conn_id)