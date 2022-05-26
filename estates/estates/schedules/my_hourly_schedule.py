from dagster import schedule

from estates.jobs.home_job import make_home_job


@schedule(cron_schedule="0 * * * *", job=make_home_job, execution_timezone="Europe/Copenhagen")
def my_hourly_schedule(_context):
    """
    A schedule definition. This example schedule runs once each hour.

    For more hints on running jobs with schedules in Dagster, see our documentation overview on
    schedules:
    https://docs.dagster.io/overview/schedules-sensors/schedules
    """
    run_config = {}
    return run_config
