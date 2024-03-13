import pandas as pd
import pandera as pa

from monde.transform import abstract, mixins

__all__ = ["MemoryOptimizer"]


class MemoryOptimizer(abstract.Transform, mixins.SchemaDriven):
    """
    MemoryOptimizer shrinks the memory usage of a DataFrame by downcasting
    all the columns it can according to the schema.

    See the following Medium post for optimization logic.

        https://medium.com/bigdatarepublic/
            advanced-pandas-optimize-speed-and-memory-a654b53be6c2

    Params

        :type schema: pa.DataFrameSchema
        :param schema:

            A Pandera schema to be used as the validator. Read the following
            docs. <https://pandera.readthedocs.io/en/stable/index.html>

        :type category_threshold: float
        :param category_threshold:

            The threshold of unique values versus size to shrink a string to a
            categorical column. Generally, you will gain memory back if at least
            half the values are not unique.

    """

    def __init__(self, schema: pa.DataFrameSchema, category_threshold: float = 0.5):
        self.category_threshold = category_threshold
        self.schema = schema
        self.datetimes = [
            f.name
            for _, f in self.schema.columns.items()
            if str(f.dtype).startswith("datetime")
        ]

    def optimize_floats(self, X: pd.DataFrame) -> pd.DataFrame:
        floats = self.meta.select_dtypes(include=["float64"]).columns.tolist()
        X[floats] = X[floats].transform(pd.to_numeric, downcast="float")
        return X

    def optimize_ints(self, X: pd.DataFrame) -> pd.DataFrame:
        ints = self.meta.select_dtypes(include=["int64"]).columns.tolist()
        X[ints] = X[ints].transform(pd.to_numeric, downcast="integer")
        return X

    def optimize_datetimes(self, X: pd.DataFrame) -> pd.DataFrame:
        X[self.datetimes] = X[self.datetimes].transform(pd.to_datetime)
        return X

    def optimize_categories(self, X: pd.DataFrame) -> pd.DataFrame:
        for col in X.select_dtypes(include=["object"]):
            # Exclude datetime columns
            if col in self.datetimes:
                continue

            # Assuming object columns that don't contain lists
            if not X[col].apply(isinstance, args=(list,)).any():
                # Measure a compresion_factor for the column
                N_unique, N_total = float(len(X[col].unique())), float(len(X[col]))
                compression_factor = N_unique / N_total

                # If we compress more than the threshold, make the column
                # categorical
                if compression_factor < self.threshold:
                    X[col] = X[col].astype("category")

        return X

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        # Pipe the dataframe through all the type optimizers
        return (
            X.pipe(self.optimize_floats)
            .pipe(self.optimize_ints)
            .pipe(self.optimize_datetimes)
            .pipe(self.optimize_categories)
        )
