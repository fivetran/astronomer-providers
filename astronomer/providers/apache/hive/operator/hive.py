import os
import re
from typing import TYPE_CHECKING

from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.utils import operator_helpers
from airflow.utils.operator_helpers import context_to_airflow_vars

if TYPE_CHECKING:
    from airflow.utils.context import Context


class HiveOperatorAsync(HiveOperator):
    """
    Executes hql code or hive script in a specific Hive database.

    :param hql: the hql to be executed. Note that you may also use
        a relative path from the dag file of a (template) hive
        script. (templated)
    :param hive_cli_conn_id: Reference to the
    :param hiveconfs: if defined, these key value pairs will be passed
        to hive as ``-hiveconf "key"="value"``
    :param hiveconf_jinja_translate: when True, hiveconf-type templating
        ${var} gets translated into jinja-type templating {{ var }} and
        ${hiveconf:var} gets translated into jinja-type templating {{ var }}.
        Note that you may want to use this along with the
        ``DAG(user_defined_macros=myargs)`` parameter. View the DAG
        object documentation for more details.
    :param script_begin_tag: If defined, the operator will get rid of the
        part of the script before the first occurrence of `script_begin_tag`
    :param run_as_owner: Run HQL code as a DAG's owner.
    :param mapred_queue: queue used by the Hadoop CapacityScheduler. (templated)
    :param mapred_queue_priority: priority within CapacityScheduler queue.
        Possible settings include: VERY_HIGH, HIGH, NORMAL, LOW, VERY_LOW
    :param mapred_job_name: This name will appear in the jobtracker.
        This can make monitoring easier.
    """

    def prepare_template(self) -> None:
        """Prepare a hql query from the template"""
        if self.hiveconf_jinja_translate:
            self.hql = re.sub(r"(\$\{(hiveconf:)?([ a-zA-Z0-9_]*)\})", r"{{ \g<3> }}", self.hql)
        if self.script_begin_tag and self.script_begin_tag in self.hql:
            self.hql = "\n".join(self.hql.split(self.script_begin_tag)[1:])

    def execute(self, context: "Context") -> None:
        """Execute the hql query"""
        self.log.info("Executing: %s", self.hql)
        self.hook = self.get_hook()

        # set the mapred_job_name if it's not set with dag, task, execution time info
        if not self.mapred_job_name:
            ti = context["ti"]
            self.hook.mapred_job_name = self.mapred_job_name_template.format(
                dag_id=ti.dag_id,
                task_id=ti.task_id,
                execution_date=ti.execution_date.isoformat(),
                hostname=ti.hostname.split(".")[0],
            )

        if self.hiveconf_jinja_translate:
            self.hiveconfs = context_to_airflow_vars(context)
        else:
            self.hiveconfs.update(context_to_airflow_vars(context))

        self.log.info("Passing HiveConf: %s", self.hiveconfs)
        self.hook.run_cli(hql=self.hql, schema=self.schema, hive_conf=self.hiveconfs)

    def dry_run(self) -> None:
        """Reset airflow environment variables to prevent existing env vars from impacting behavior."""
        self.clear_airflow_vars()

        self.hook = self.get_hook()
        self.hook.test_hql(hql=self.hql)

    def on_kill(self) -> None:
        """Kill the execution"""
        if self.hook:
            self.hook.kill()

    def clear_airflow_vars(self) -> None:
        """Reset airflow environment variables to prevent existing ones from impacting behavior."""
        blank_env_vars = {
            value["env_var_format"]: "" for value in operator_helpers.AIRFLOW_VAR_NAME_FORMAT_MAPPING.values()
        }
        os.environ.update(blank_env_vars)
