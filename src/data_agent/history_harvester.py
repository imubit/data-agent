import datetime as dt
import logging
import time as tm

from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from data_agent.exceptions import HistoryHarvesterJobAlreadyExists
from data_agent.msg_packer import encode_dataframe

log = logging.getLogger(__name__)


class HistoryHarvester:
    def __init__(self, connection_manager, broker):
        self.connection_manager = connection_manager
        self.broker = broker

        self._thread_pool_executor = ThreadPoolExecutor(max_workers=20)
        self._scheduler = AsyncIOScheduler(thread_pool=self._thread_pool_executor)
        self._scheduler.start()

    async def _delivery_job_func(
        self,
        job_id,
        conn,
        tags,
        first_timestamp,
        last_timestamp,
        time_frequency,
        batch_size,
        iteration,
    ):
        try:
            start_time = tm.time()

            next_period_end = min(last_timestamp, first_timestamp + batch_size)

            df = conn.read_tag_values_period(
                tags=tags,
                first_timestamp=first_timestamp,
                last_timestamp=next_period_end,
                time_frequency=time_frequency,
            )

            read_time = tm.time() - start_time

            if df.empty:
                log.warning(
                    f"No data read for job '{job_id}' for period {first_timestamp} - {next_period_end}"
                )

            else:  # Publish data
                headers = {
                    "data_category": "historical",
                    "connection": conn.name,
                    "job_id": job_id,
                    "batch_num": iteration,
                }

                payload = encode_dataframe(df)

                log.debug(
                    f"(#{iteration}): Job {job_id}: "
                    f"Data publish: read time={read_time:.2f}s), {len(df)} samples, "
                    f"period: {first_timestamp} - {next_period_end}"
                )
                self.broker.publish_data(payload, headers=headers)

            if next_period_end < last_timestamp:
                # Reschedule next run
                self._scheduler.add_job(
                    func=self._delivery_job_func,
                    trigger="date",
                    # next_run_time=dt.datetime.now(),
                    coalesce=True,  # Always run once
                    id=job_id,
                    max_instances=2,
                    replace_existing=True,
                    args=[
                        job_id,
                        conn,
                        tags,
                        next_period_end,
                        last_timestamp,
                        time_frequency,
                        batch_size,
                        iteration + 1,
                    ],
                )

        except Exception as e:
            log.exception(f'Exception in history harvester job "{job_id}" - {e}')

    def create_delivery_job(
        self,
        job_id: str,
        conn_name: str,
        tags: list,
        first_timestamp: dt.datetime,
        last_timestamp: dt.datetime,
        time_frequency: dt.timedelta,
        batch_size: dt.timedelta = None,
        progress_callback=None,
    ):
        # order tags alphabetically
        tags.sort()

        existing_job = self._scheduler.get_job(job_id)
        if existing_job:
            raise HistoryHarvesterJobAlreadyExists(
                f"History loader Job {job_id} already exists."
            )

        conn = self.connection_manager.connection(conn_name, check_enabled=False)

        self._scheduler.add_job(
            func=self._delivery_job_func,
            trigger="date",
            # next_run_time=dt.datetime.now(),
            coalesce=True,  # Always run once
            id=job_id,
            max_instances=2,
            replace_existing=True,
            args=[
                job_id,
                conn,
                tags,
                first_timestamp,
                last_timestamp,
                time_frequency,
                batch_size,
                0,
            ],
        )
