from dagster import repository
from estates.assets.analysis import analysis_assets
from estates.jobs.home_job import make_home_job
from estates.jobs.boliga_job import make_boliga_job
from estates.jobs.service_job import make_service_job
from estates.schedules.my_hourly_schedule import my_hourly_schedule
from estates.sensors.my_sensor import my_sensor


@repository
def estates():
    """
    ## Denmark Real Estates
    > Performing DataOps, ML and MLOps with real estates data
    Code: [advance scraping](https://github.com/Proteusiq/advance_scraping)
    """
    jobs = [make_service_job, make_home_job, make_boliga_job]
    schedules = [my_hourly_schedule]
    sensors = [my_sensor]
    assets = [analysis_assets]

    return jobs + schedules + sensors + assets
