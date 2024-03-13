import hashlib

import pandas as pd
import pandera as pa

from monde.transform import abstract, mixins

__all__ = [
    "MaskProtectedAttributes",
]


class HashProtectedAttributes(abstract.Transform, mixins.Protector):
    def __init__(self, schema: pa.DataFrameSchema, algorithm: str = "md5"):
        self.schema = schema
        self.hasher = getattr(hashlib, algorithm)

    def transform(self, X: pd.DataFrame):
        for column in self.protected_attributes:
            X[column] = (
                X[column]
                .astype("string")
                .str.encode("utf-8")
                .apply(self.hasher)
                .apply(lambda h: h.hexdigest())
            )

        return X


class MaskProtectedAttributes(abstract.Transform, mixins.Protector):
    def __init__(self, schema: pa.DataFrameSchema):
        self.schema = schema

    @staticmethod
    def mask(x: str) -> str:
        return len(x) * "*"

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        for column in self.protected_attributes:
            X[column] = X[column].astype("string").apply(self.mask)
        return X
