import pandas as pd


def coalesce(s: pd.Series, *ss: pd.Series) -> pd.Series:
    out = s.copy()
    for alt_s in ss:
        out.mask(pd.isnull, alt_s, inplace=True)  # type: ignore
    return out
