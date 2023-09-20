import logging
import time
from datetime import timedelta

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers import interval

from .exceptions import DaqJobAlreadyExists

log = logging.getLogger(__name__)


class DAQScheduler(AsyncIOScheduler):
    def __init__(
        self,
        broker_conn,
        connection_manager,
        persistence,
        is_convert_dot_to_slash=True,
        **options,
    ):
        self._is_convert_dot_to_slash = is_convert_dot_to_slash
        self._broker_conn = broker_conn
        self._connection_manager = connection_manager
        self._job_state = {}
        self._persistence = persistence
        self._total_iterations_counter = 0

        super(DAQScheduler, self).__init__(gconfig={}, options=options)

        # Recreate jobs from config
        jobs = self._persistence.list_items()
        for job_id in jobs:
            log.debug(f'Starting preconfigured job "{job_id}"...')
            self._create_scan_job(
                job_id=job_id,
                conn_name=jobs[job_id]["conn_name"],
                tags=jobs[job_id]["tags"],
                seconds=jobs[job_id]["seconds"],
                from_cache=jobs[job_id]["from_cache"],
            )

    def reset(self, persist=False):
        jobs = self.list_jobs()
        for job_id in jobs:
            self.remove_job(job_id, persist=persist)

    async def _job_func(self, job_id, conn, broker, tags, from_cache, refresh_rate_ms):
        try:
            # Reconnect if needed
            if not conn.connected:
                log.info("Reconnecting to Target Server...")
                conn.connect()

            if not self._group_id(job_id) in conn.list_groups():
                log.info(f"Re-registering group {self._group_id(job_id)}...")
                conn.register_group(
                    group_name=self._group_id(job_id),
                    tags=tags,
                    refresh_rate_ms=refresh_rate_ms,
                )

            # Read data
            start_time = time.time()
            group_values = conn.read_group_values(
                self._group_id(job_id), from_cache=from_cache
            )
            # group_values = conn.read_tag_values(tags)
            read_time = time.time() - start_time

            msg = {
                "job_id": job_id,
                "sample_id": self._job_state[job_id]["iter_counter"],
                "data": group_values,
            }

            to_publish = [f'{t}={group_values[t]["Value"]}' for t in group_values]
            self._total_iterations_counter += 1
            self._job_state[job_id]["iter_counter"] += 1
            log.debug(
                f"(#{self._total_iterations_counter}): Job {job_id}: "
                f'Data publish (read time={read_time:.2f}s): {", ".join(to_publish[:120])}'
            )
            broker.publish_data(msg, headers={"job_id": job_id})

        except Exception as e:
            log.exception(f'Exception in job "{job_id}" - {e}')

    def list_jobs(self, conn_name=None):
        if conn_name:
            # Filter jobs by connection name
            jobs = [j.id for j in self.get_jobs() if j.args[1].name == conn_name]
        else:
            jobs = [j.id for j in self.get_jobs()]
        jobs.sort()
        return jobs

    def create_scan_job(
        self,
        job_id,
        conn_name,
        tags,
        seconds=1,
        update_on_conflict=False,
        from_cache=True,
    ):
        # order tags alphabetically
        tags.sort()

        job = self.get_job(job_id)
        if job:
            if not update_on_conflict:
                raise DaqJobAlreadyExists(f"DAQ Job {job_id} already exists.")

            job = self.get_job(job_id)

            # Modify interval
            if timedelta(seconds=seconds) != job.trigger.interval:
                job.reschedule(interval.IntervalTrigger(seconds=seconds))
                self._persistence.add_item(
                    job_id,
                    {
                        "conn_name": conn_name,
                        "tags": tags,
                        "seconds": seconds,
                        "from_cache": from_cache,
                    },
                )

            # Modify args
            if job.args[1].name != conn_name or job.args[3] != tags:
                self._create_scan_job(job_id, conn_name, tags, seconds, from_cache)
                self._persistence.add_item(
                    job_id,
                    {
                        "conn_name": conn_name,
                        "tags": tags,
                        "seconds": seconds,
                        "from_cache": from_cache,
                    },
                )
            log.info(
                f"Job  '{job_id}' modified (Connection: '{conn_name}', Seconds: {seconds}  "
                f"with tags: '{tags}'  from_cache: '{from_cache}')."
            )

        else:
            job = self._create_scan_job(job_id, conn_name, tags, seconds, from_cache)
            self._persistence.add_item(
                job_id,
                {
                    "conn_name": conn_name,
                    "tags": tags,
                    "seconds": seconds,
                    "from_cache": from_cache,
                },
            )
            log.info(
                f"Job  '{job_id}' created (Connection: '{conn_name}', Seconds: {seconds}  "
                f"with tags: '{tags}'  from_cache:'{from_cache}')."
            )

        return job

    @staticmethod
    def _group_id(job_id):
        return f"scan_job_{job_id}"

    def _create_scan_job(self, job_id, conn_name, tags, seconds, from_cache):
        refresh_rate_ms = seconds * 1000
        conn = self._connection_manager.connection(conn_name, check_enabled=False)

        if conn.connected:
            conn.register_group(
                group_name=self._group_id(job_id),
                tags=tags,
                refresh_rate_ms=refresh_rate_ms,
            )

        trigger = interval.IntervalTrigger(seconds=seconds)

        job = self.add_job(
            func=self._job_func,
            trigger=trigger,
            # seconds=seconds,
            coalesce=True,  # Always run once
            id=job_id,
            max_instances=1,
            replace_existing=True,
            args=[job_id, conn, self._broker_conn, tags, from_cache, refresh_rate_ms],
        )

        self._job_state[job_id] = {"iter_counter": 0}

        return job

    def remove_job(self, job_id, persist=True):
        if not isinstance(job_id, list):
            job_id = [job_id]

        for j in job_id:
            job = self.get_job(j)
            conn = job.args[1]

            super().remove_job(j)

            conn.unregister_group(self._group_id(j))

            if persist:
                self._persistence.remove_item(j)

    def list_tags(self, job_id):
        job = self.get_job(job_id)
        return job.args[3]

    def add_tags(self, job_id, tags):
        existing_tags = self.list_tags(job_id)

        # Add tags to list unless already exist
        for tag in tags:
            existing_tags.append(tag) if tag not in existing_tags else existing_tags

    def remove_tags(self, job_id, tags):
        existing_tags = self.list_tags(job_id)
        for tag in tags:
            existing_tags.remove(tag)


def create_daq_scheduler(
    broker, conn_manager, persistence, is_convert_dot_to_slash=True, **options
):
    scheduler = DAQScheduler(
        broker,
        conn_manager,
        persistence=persistence,
        is_convert_dot_to_slash=is_convert_dot_to_slash,
        **options,
    )
    scheduler.start()
    return scheduler
