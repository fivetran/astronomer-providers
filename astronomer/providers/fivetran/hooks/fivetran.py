import asyncio
from typing import Any, Dict

import aiohttp
from aiohttp import ClientResponseError
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook

from asgiref.sync import sync_to_async

class FivetranHookAsync(FivetranHook):
    api_user_agent = 'airflow_provider_fivetran_async/1.0.0'
    
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    async def _do_api_call_async(self, endpoint_info, json=None):
        method, endpoint = endpoint_info
        
        if not self.fivetran_conn:
            self.fivetran_conn = await sync_to_async(self.get_connection)(self.fivetran_conn_id)
        auth = (self.fivetran_conn.login, self.fivetran_conn.password)
        url = f"{self.api_protocol}://{self.api_host}/{endpoint}"
        headers = {
            "User-Agent": self.api_user_agent
        }

        async with aiohttp.ClientSession() as session:
            if method == "GET":
                request_func = session.get
            elif method == "POST":
                request_func = session.post
            elif method == "PATCH":
                request_func = session.patch
                headers.update({"Content-Type": "application/json;version=2"})
            else:
                raise AirflowException("Unexpected HTTP Method: " + method)

            attempt_num = 1
            while True:
                try:
                    response = await request_func(
                        url,
                        data=json if method in ("POST", "PATCH") else None,
                        params=json if method == "GET" else None,
                        auth=auth,
                        headers=headers,
                    )
                    response.raise_for_status()
                    return cast(Dict[str, Any], await response.json())
                except ClientResponseError as e:
                    if not _retryable_error_async(e):
                        # In this case, the user probably made a mistake.
                        # Don't retry.
                        return {"Response": {e.message}, "Status Code": {e.status}}
                    self._log_request_error(attempt_num, str(e))

                if attempt_num == self.retry_limit:
                    raise AirflowException(
                        f"API requests to Fivetran failed {self.retry_limit} times."
                        " Giving up.")
               
                attempt_num += 1
                await asyncio.sleep(self.retry_delay)


    async def get_connector_async(self, connector_id):
        if connector_id == "":
            raise ValueError("No value specified for connector_id")
        endpoint = self.api_path_connectors + connector_id
        resp = await self._do_api_call_async(("GET", endpoint))
        return resp["data"]
  
    async def get_sync_status_async(self, connector_id, previous_completed_at):
        connector_details = await self.get_connector_async(connector_id)
        succeeded_at = self._parse_timestamp(connector_details["succeeded_at"])
        failed_at = self._parse_timestamp(connector_details["failed_at"])
        current_completed_at = (
            succeeded_at if succeeded_at > failed_at else failed_at
        )

        # The only way to tell if a sync failed is to check if its latest
        # failed_at value is greater than then last known "sync completed at" value.
        if failed_at > previous_completed_at:
            service_name = connector_details["service"]
            schema_name = connector_details["schema"]
            raise AirflowException(
                f'Fivetran sync for connector "{connector_id}" failed; '
                f"please see logs at "
                f"{self._connector_ui_url_logs(service_name, schema_name)}"
            )

        sync_state = connector_details["status"]["sync_state"]
        self.log.info(f'Connector "{connector_id}": sync_state = {sync_state}')

        # Check if sync started by FivetranOperator has finished
        # indicated by new 'succeeded_at' timestamp
        if current_completed_at > previous_completed_at:
            self.log.info('Connector "{}": succeeded_at: {}'.format(
                connector_id, succeeded_at.to_iso8601_string())
            )
            return True
        else:
            return False

    async def get_last_sync_async(self, connector_id):
        connector_details = await self.get_connector_async(connector_id)
        succeeded_at = self._parse_timestamp(connector_details["succeeded_at"])
        failed_at = self._parse_timestamp(connector_details["failed_at"])
        return succeeded_at if succeeded_at > failed_at else failed_at


def _retryable_error_async(exception: ClientResponseError) -> bool:
    return exception.status >= 500