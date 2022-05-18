import asyncio
import pendulum
from typing import Any, AsyncIterator, Dict, List, Tuple

from airflow.triggers.base import BaseTrigger, TriggerEvent

from fivetran_provider_async.hooks.fivetran import FivetranHookAsync

class FivetranSensorTrigger(BaseTrigger):

    def __init__(
        self,
        task_id: str,
        polling_period_seconds: float,
        connector_id: str,
        fivetran_conn_id: str,
        previous_completed_at: pendulum.datetime.DateTime,
    ):
        super().__init__()
        self.task_id = task_id
        self.polling_period_seconds = polling_period_seconds
        self.connector_id = connector_id
        self.fivetran_conn_id = fivetran_conn_id
        self.previous_completed_at = previous_completed_at

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        return (
            "fivetran_provider.triggers.fivetran_trigger.FivetranTrigger",
            {
                "task_id": self.task_id,
                "polling_period_seconds": self.polling_period_seconds,
                "connector_id": self.connector_id,
                "fivetran_conn_id": self.fivetran_conn_id,
                "previous_completed_at": self.previous_completed_at,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        try:
            hook = FivetranHookAsync(fivetran_conn_id=self.fivetran_conn_id)
            if self.previous_completed_at is None:
                self.previous_completed_at = await hook.get_last_sync_async(self.connector_id)
            while true:
                res = await hook.get_sync_status_async(self.connector_id, self.previous_completed_at)
                if (res["status"] == "success" or res["status"] == "error":
                    yield TriggerEvent(res)
                    return
                await asyncio.sleep(self.polling_period_seconds)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e), "type": "ERROR"})
            return
