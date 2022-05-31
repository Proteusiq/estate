import pytest
from pandas import DataFrame
from dagster import build_op_context
from estates.ops.home import get_home, prepare_home


fake_data = DataFrame({"Name": ["James", "Bond", "M"], "Score": [0.1, 0.9, 0.7]})


@pytest.mark.skip(reason="not complete")
def test_get_home():
    context = build_op_context(resources={"workers": 1, "pagesize": 5})
    assert type(get_home(context), DataFrame)


@pytest.mark.skip(reason="not complete")
def test_prepare_home():

    dataf = prepare_home(fake_data)
    assert [column for column in dataf.columns] == ["name", "score"]
