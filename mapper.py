import numpy as np


def mapper(row):
    """Parses row of dataframe and returns price value."""

    _, df = row
    price = df["price"]
    return price
