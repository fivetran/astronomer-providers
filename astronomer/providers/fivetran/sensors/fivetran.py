from typing import Any, Dict, Optional, Tuple, TYPE_CHECKING

from airflow.exceptions import AirflowException
from airflow.utils.context import Context
from fivetran_provider.sensor.fivetran import FivetranSensor

from fivetran_provider_async.hooks.fivetran import FivetranHookAsync
from fivetran_provider_async.triggers.fivetran import FivetranSensorTrigger


class FivetranSensorAsync(FivetranSensor):
    def __init__(
        self,
        *,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)

    def execute(self, context: Dict[str, Any]) -> None:
        """Check for the target_status and defers using the trigger"""
        self.defer(
            timeout=self.execution_timeout,
            trigger=FivetranSensorTrigger(
                task_id=self.task_id,
                fivetran_conn_id=self.fivetran_conn_id,
                connector_id=self.connector_id,
                previous_completed_at=self.previous_completed_at,
                polling_period_seconds=self.poke_interval,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: "Context", event: Optional[Dict[Any, Any]] = None) -> None:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event:
            if "status" in event and event["status"] == "error":
                msg = "{0}: {1}".format(event["status"], event["message"])
                raise AirflowException(msg)
            if "status" in event and event["status"] == "success":
                self.log.info("%s completed successfully.", self.task_id)
                self.log.info(
                    "Fivetran connector %s finished syncing at  %s", self.connector_id, self.previous_completed_at
                )
                return None
        self.log.info("%s completed successfully.", self.task_id)
        return None