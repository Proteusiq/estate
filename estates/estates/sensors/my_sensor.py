from dagster import RunRequest, sensor

from estates.jobs.say_hello import say_hello_job


@sensor(job=say_hello_job)
def my_sensor(_context):
    """
    A sensor definition. This example sensor always requests a run at each sensor tick.

    For more hints on running jobs with sensors in Dagster, see our documentation overview on
    sensors:
    https://docs.dagster.io/overview/schedules-sensors/sensors
    """
    should_run = True
    if should_run:
        yield RunRequest(run_key=None, run_config={})
