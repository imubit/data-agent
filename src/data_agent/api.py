import functools
import logging
import time
from typing import Union

from amqp_fabric.abstract_service_api import AbstractServiceApi

log = logging.getLogger(__name__)


def traceapi(func):
    """Decorates a function to show its trace."""

    @functools.wraps(func)
    def traceapi_closure(*args, **kwargs):
        """The closure."""

        params_str = ", ".join([f"{a}={kwargs[a]}" for a in kwargs])
        try:
            curr_time = time.time()
            result = func(*args, **kwargs)
            if time.time() - curr_time > 0.5:
                log.warning(
                    f"SLOW API CALL: ({time.time() - curr_time} sec.) {func.__name__}({params_str}) => {result}"
                )
            else:
                log.debug(f"{func.__name__}({params_str}) => {result}")
            return result
        except Exception as e:
            log.error(
                f"{func.__name__}({params_str}) => Exception {type(e)} raise: {e}"
            )
            log.exception(e)
            raise e

    return traceapi_closure


class ServiceApi(AbstractServiceApi):
    def __init__(self, scheduler, connection_manager, data_exchanger, safe_manipulator):
        self._scheduler = scheduler
        self._connection_manager = connection_manager
        self._data_exchanger = data_exchanger
        self._safe_manipulator = safe_manipulator

    @traceapi
    def list_supported_connectors(self):
        """Return a list of supported connector types by this agent"""
        return self._connection_manager.list_supported_connectors()

    @traceapi
    def target_info(self, target_ref: str, conn_type: str):
        """Retrieve information about target host (including endpoint enumeration if availailble)

        :param target_ref:
        :param conn_type:
        :return:
        """
        return self._connection_manager.target_info(target_ref, conn_type)

    @traceapi
    def list_connections(self):
        """List configured connections with connection status and type

        :return:
        """
        return self._connection_manager.list_connections()

    @traceapi
    def create_connection(
        self,
        conn_name: str,
        conn_type: str,
        enabled=False,
        ignore_existing=False,
        **kwargs,
    ):
        """Create new data connection

        :param conn_name:
        :param conn_type:
        :param enabled: Should be enabled by default
        :param ignore_existing:
        :param kwargs:
        :return:
        """
        return self._connection_manager.create_connection(
            conn_name,
            conn_type,
            enabled=enabled,
            ignore_existing=ignore_existing,
            **kwargs,
        )

    @traceapi
    def delete_connection(self, conn_name: str):
        """Delete existing connection

        :param conn_name:
        :return:
        """

        # We should remove all associated jobs
        if self._scheduler:
            jobs = self._scheduler.list_jobs(conn_name=conn_name)
            self._scheduler.remove_job(jobs)

        # And manipulated tages
        tags = self._safe_manipulator.list_tags(conn_name)
        self._safe_manipulator.unregister_tags(conn_name, tags)

        self._connection_manager.delete_connection(conn_name)

    @traceapi
    def is_connected(self, conn_name: str):
        """Check if connection is active

        :param conn_name:
        :return:
        """
        return self._connection_manager.is_connected(conn_name)

    @traceapi
    def enable_connection(self, conn_name: str):
        """Enable connection (connect if needed)

        :param conn_name:
        :return:
        """
        self._connection_manager.enable_connection(conn_name)

    @traceapi
    def disable_connection(self, conn_name: str):
        """Disable connection (disconnect if needed)

        :param conn_name:
        :return:
        """
        self._connection_manager.disable_connection(conn_name)

    @traceapi
    def connection_info(self, conn_name: str):
        """Retrieve connection information

        :param conn_name:
        :return:
        """
        return self._connection_manager.connection(conn_name).connection_info()

    @traceapi
    def read_tag_attributes(self, conn_name: str, tags: list, attributes: list = None):
        """Retrieve properties of provided tags

        :param conn_name:
        :param tags:
        :param attributes: None - retrieve all attributes (or provide 1 or more attributes to retrieve)
        :return:
        """
        return self._connection_manager.connection(conn_name).read_tag_attributes(
            tags, attributes
        )

    @traceapi
    def list_tags(
        self,
        conn_name: str,
        filter: Union[str, list] = "",
        include_attributes: Union[bool, list] = False,
        recursive: bool = False,
        max_results: int = 0,
    ):
        """

        :param conn_name:
        :param filter: Filter, path (str) or list of files (list)
        :param include_attributes:
        :return:
        """
        return self._connection_manager.connection(conn_name).list_tags(
            filter,
            include_attributes=include_attributes,
            recursive=recursive,
            max_results=max_results,
        )

    @traceapi
    def read_tag_values(self, conn_name: str, tags: list):
        """Read tag values

        :param conn_name:
        :param tags:
        :return:
        """
        return self._connection_manager.connection(conn_name).read_tag_values(tags)

    @traceapi
    def read_tag_values_period(
        self,
        conn_name: str,
        tags: list,
        first_timestamp=None,
        last_timestamp=None,
        time_frequency=None,
        max_results=None,
        result_format="dataframe",
        progress_callback=None,
    ):
        """Read tag values period (usually historical)

        :param conn_name:
        :param tags:
        :return:
        """
        return self._connection_manager.connection(conn_name).read_tag_values_period(
            tags=tags,
            first_timestamp=first_timestamp,
            last_timestamp=last_timestamp,
            time_frequency=time_frequency,
            max_results=max_results,
            result_format=result_format,
            progress_callback=progress_callback,
        )

    @traceapi
    def delete_tag(self, conn_name: str, tags: list):
        """Delete Tag

        :param conn_name:
        :param tags:
        :return:
        """
        return self._connection_manager.connection(conn_name).delete_tag(tags)

    # ================== Exchanger ===================
    @traceapi
    def copy_period(
        self,
        src_conn,
        tags,
        dest_conn,
        dest_group,
        first_timestamp,
        last_timestamp,
        time_frequency=None,
        on_conflict="skip",
        progress_callback=None,
        batch_process=False,
    ):
        self._data_exchanger.copy_period(
            src_conn=src_conn,
            tags=tags,
            dest_conn=dest_conn,
            dest_group=dest_group,
            first_timestamp=first_timestamp,
            last_timestamp=last_timestamp,
            time_frequency=time_frequency,
            on_conflict=on_conflict,
            progress_callback=progress_callback,
            batch_process=False,
        )

    @traceapi
    def copy_attributes(
        self,
        src_conn,
        tags,
        dest_conn,
        dest_group,
        attributes=None,
    ):
        self._data_exchanger.copy_attributes(
            src_conn=src_conn,
            tags=tags,
            dest_conn=dest_conn,
            dest_group=dest_group,
            attributes=attributes,
        )

    # ================== Manipulated Tags ===================

    @traceapi
    def list_manipulated_tags(self, conn_name: str):
        """

        :param conn_name:
        :return:
        """
        return self._safe_manipulator.list_tags(conn_name)

    @traceapi
    def register_manipulated_tags(self, conn_name: str, tags: dict):
        """

        :param conn_name:
        :param tags:
        :return:
        """
        self._safe_manipulator.register_tags(conn_name, tags)

    @traceapi
    def unregister_manipulated_tags(self, conn_name: str, tags: list):
        """

        :param conn_name:
        :param tags:
        :return:
        """
        self._safe_manipulator.unregister_tags(conn_name, tags)

    @traceapi
    def write_manipulated_tags(
        self, conn_name: str, tags: dict, wait_for_result=True, **kwargs
    ):
        """

        :param conn_name:
        :param tags:
        :return:
        """
        self._safe_manipulator.write_tags(conn_name, tags, wait_for_result, **kwargs)

    # ================== JOBS ===================
    @traceapi
    def list_jobs(self):
        """List all running DAQ jobs

        :return:
        """

        return self._scheduler.list_jobs()

    @traceapi
    def create_job(
        self,
        job_id: str,
        conn_name: str,
        tags: list,
        seconds: int,
        update_on_conflict: bool = False,
        from_cache: bool = True,
    ):
        """Create new DAQ job

        :param job_id:
        :param conn_name:
        :param tags:
        :param seconds:
        :param update_on_conflict:
        :return:
        """
        self._scheduler.create_scan_job(
            job_id=job_id,
            conn_name=conn_name,
            tags=tags,
            seconds=seconds,
            update_on_conflict=update_on_conflict,
            from_cache=from_cache,
        )

    @traceapi
    def remove_job(self, job_id: str):
        """Remove DAQ job by id

        :param job_id:
        :return:
        """
        self._scheduler.remove_job(job_id)

    @traceapi
    def list_job_tags(self, job_id: str):
        """Return list of tags in the job

        :return:
        """

        return self._scheduler.list_tags(job_id)

    @traceapi
    def add_job_tags(self, job_id: str, tags: list):
        """Add additional tags to job

        :param job_id:
        :param tags:
        :return:
        """
        self._scheduler.add_tags(job_id, tags)

    @traceapi
    def remove_job_tags(self, job_id: str, tags: list):
        """Remove tags from job

        :param job_id:
        :param tags: tags to remove
        :return:
        """
        self._scheduler.remove_tags(job_id, tags)

    @traceapi
    def provision_config(self, config: dict):
        """Provision configuration as a single command (including jobs and manipulated tags)

        :param config:
        :return:
        """

        existing_jobs = self._scheduler.list_jobs()

        for conn_name in config.keys():
            # connection = self._connection_manager.connection(conn_name)

            # Add DAQ tags
            for job_id in config[conn_name]["daq_jobs"].keys():
                tags_to_add = config[conn_name]["daq_jobs"][job_id]["tags"]
                sample_rate = config[conn_name]["daq_jobs"][job_id]["sample_rate"]

                # if job not exists - create it, otherwise add missing tags
                if job_id not in existing_jobs:
                    self._scheduler.create_scan_job(
                        job_id=job_id,
                        conn_name=conn_name,
                        tags=tags_to_add,
                        seconds=sample_rate,
                    )

                else:  # add missing tags
                    # TODO: check if sample rate is the same

                    existing_tags = self._scheduler.list_job_tags(job_id=job_id)
                    missing_tags = [
                        tag for tag in tags_to_add if tag not in existing_tags
                    ]
                    self._scheduler.add_job_tags(job_id=job_id, tags=missing_tags)

            # Add manipulated items
            self._safe_manipulator.register_tags(
                conn_name, config[conn_name]["manipulated_tags"]
            )
