from typing import Literal
from pandas import DataFrame


def size_of_dataframe(
    dataframe: DataFrame,
    unit: Literal["B", "KB", "MB", "GB"] = "B",
) -> str:
    units_divider = {
        "B": float(1 << 0),
        "KB": float(1 << 10),
        "MB": float(1 << 20),
        "GB": float(1 << 30),
    }

    divider = units_divider.get(unit)
    data_bytes = dataframe.memory_usage(deep=True).sum()

    return f"{data_bytes/divider:,.2f} {unit}"
