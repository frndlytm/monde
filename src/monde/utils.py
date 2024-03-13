import datetime
import functools
import json
import logging
import traceback
import time
from typing import Iterator

import numpy as np
import pandas as pd


def timed(f):
    """Log the execution time of a decorated function"""

    @functools.wraps(f)
    def wrapped(*args, **kwargs):
        start = time.time()
        result = f(*args, **kwargs)
        stop = time.time()
        
        obs = {
            "type": "Timed",
            "name": f"{f.__module__}.{f.__qualname__}",
            "start": datetime.fromtimestamp(start).isoformat(),
            "stop": datetime.fromtimestamp(stop).isoformat(),
            "runtime": stop - start,
        }

        logging.info(json.dumps(obs))
        return result

    return wrapped


class chunk_statistics:
    def __init__(
        self,
        chunk: pd.DataFrame,
        logger: logging.Logger = logging.getLogger(),
    ):
        self.chunk = chunk
        self.last_observed = datetime.datetime.now()
        self.logger = logger

    def __enter__(self):
        return self

    def __exit__(self, type_, value, tb):
        # fmt:off
        obs = self.observe()

        if (type_, value, tb) == (None, None, None):
            self.logger.info(json.dumps(obs))

        else:
            obs.update({
                "status": "ERROR",
                "exception": traceback.print_exception(type_, value=value, tb=tb),
            })

            self.logger.error(json.dumps(obs))
        # fmt:on

    def observe(self):
        # OBSERVE
        now = datetime.datetime.now()
        stats = {
            "type": "ChunkStatistics",
            "start": self.last_observed.isoformat(),
            "stop": now.isoformat(),
            "runtime": now.timestamp() - self.last_observed.timestamp(),
            "offset": int(self.chunk.index.min()),
            "rows": len(self.chunk.index),
            "columns": len(self.chunk.columns),
            "memory": int(self.chunk.memory_usage().sum()),
            "status": "OK",
            "exception": None,
        }

        # Update the last_observed time
        self.last_observed = now

        # RETURN
        return stats

    def json(self, **kwargs):
        return json.dumps(self.observe(), **kwargs)


Quadrants = tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
]


def quadrantize(X: pd.DataFrame) -> Quadrants:
    """
    `quadrantize` a `DataFrame` $X$ by finding the a fully nan region at the
    top-left of the dataframe, and return the quadrants.

    Case: 4-quadrants

        In the `numpy.array` below, there are 4-quadrants, where each quadrant
        is labeled by a different unique value.

            np.array(
                [
                    [np.nan, np.nan, np.nan, 0, 0],
                    [np.nan, np.nan, np.nan, 0, 0],
                    [     1,      1,      1, 2, 2],
                    [     1,      1,      1, 2, 2],
                    [     1,      1,      1, 2, 2],
                    [     1,      1,      1, 2, 2],
                    [     1,      1,      1, 2, 2],
                ]
            )

        The following are the definitions of the 4 quadrants:

            * TOPLEFT contains all Nans. Raise an error if this isn't True
            * TOPRIGHT (label=0) contains the multi-index for the columns.
            * BOTTOMLEFT (label=1) contains the multi-index for the rows.
            * BOTTOMRIGHT (label=2) contains the data.

    Case: 1-quadrant

        In the `numpy.array` below, there is 1-quadrant.

            np.array(
                [
                    [0, 0, 0, 0, 0],
                    [0, 0, 0, 0, 0],
                    [0, 0, 0, 0, 0],
                    [0, 0, 0, 0, 0],
                    [0, 0, 0, 0, 0],
                    [0, 0, 0, 0, 0],
                    [0, 0, 0, 0, 0],
                ]
            )

        In the event of this, return everything as the bottomright with
        `None` for the other arrays.

            (None, None, None, bottomright)

    """
    # Get the location of the first column by finding the first non-null
    # value (`first_valid_index`) in the first row
    y_star = X.iloc[0].first_valid_index()

    # Get the `first_valid_index` from that column.
    x_star = X[y_star - 1].first_valid_index() if y_star > 0 else 0

    # Case: 1-quadrant
    if (x_star, y_star) == (0, 0):
        return (None, None, None, X)

    # Case: 4-quadrants
    topleft = X.loc[: x_star - 1, : y_star - 1]
    bottomleft = X.loc[x_star:, : y_star - 1]
    topright = X.loc[: x_star - 1, y_star:]
    bottomright = X.loc[x_star:, y_star:]

    if not np.array(topleft.isnull()).reshape(-1).all():
        raise ValueError("Topleft quadrant must all be `null`")

    # RETURN
    return (topleft, bottomleft, topright, bottomright)


def widths_to_slices(widths: Iterator[int]) -> Iterator[slice]:
    start, end = 0, 0
    for w in widths:
        end += w
        yield slice(start, end)
        start += w
