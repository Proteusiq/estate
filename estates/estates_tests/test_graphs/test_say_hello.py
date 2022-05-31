import pytest
from estates.jobs.home_job import make_home_job


@pytest.mark.skip(reason="not complete")
def test_say_hello():
    """
    This is an example test for a Dagster job.

    For hints on how to test your Dagster graphs, see our documentation tutorial on Testing:
    https://docs.dagster.io/concepts/testing
    """
    result = make_home_job.execute_in_process()

    assert result.success
