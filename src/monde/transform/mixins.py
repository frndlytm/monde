from functools import cached_property
from typing import Iterable

import pandas as pd
import pandera as pa

from monde import dataframe


class SchemaDriven:
    schema: pa.DataFrameSchema

    def __init__(self, schema: pa.DataFrameSchema):
        self.schema = schema

    @cached_property
    def meta(self) -> pd.DataFrame:
        return dataframe.empty(self.schema)


class Protector(SchemaDriven):
    @cached_property
    def protected_attributes(self) -> Iterable[str]:
        return [
            f.name
            for f in self.schema.columns.values()
            if (getattr(f, "metadata") or {}).get("protected")
        ]
