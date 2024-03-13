import pandas as pd

from monde import utils
from monde.transform import abstract, mixins


class Identity(abstract.Transform):
    """
    IdentityTransform returns the DataFrame it receives, with no changes.

    This is useful for building abstract pipelines that need a default
    transform and helps to avoid a bunch of if/elif/else control flow
    and null checks.

    Usage

        >>> preprocessor = Identity()
        >>> X = pd.read_csv("s3://path/to/file.csv")
        >>> (X.pipe(preprocessor) == X).all()
        True

    """

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:  # type: ignore
        return X


class CleanBooleans(abstract.Transform, mixins.SchemaDriven):
    TRUTHY = ["TRUE", "T", "YES", "Y"]  # True
    FALSEY = ["FALSE", "F", "NO", "N"]  # False
    UNKNOWN = ["NA", "N/A", "NULL", "NONE", "(BLANK)", "U", "UNKNOWN", ""]  # pd.NA
    
    @utils.timed
    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        selection = self.meta.select_dtypes(include="boolean")

        for column in selection:
            if pd.api.types.is_object_dtype(X[column]):
                X[column] = X[column].str.strip().str.upper()
                X[column] = X[column].replace(self.TRUTHY, 1)
                X[column] = X[column].replace(self.FALSEY, 0)
                X[column] = X[column].replace(self.UNKNOWN, pd.NA)
                X[column] = X[column].astype("boolean")

        return X


class CleanStrings(abstract.Transform, mixins.SchemaDriven):
    @utils.timed
    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        for column in X.columns:
            X[column] = X[column].apply(
                lambda s: s.strip() if isinstance(s, str) else s
            )

        return X


class CleanIntegers(abstract.Transform, mixins.SchemaDriven):
    @utils.timed
    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        selection = self.meta.select_dtypes(include=["int", "uint"])

        for column in selection:
            x = X[column]

            if pd.api.types.is_string_dtype(x):
                nullable = self.schema.columns[column].nullable

                X[column] = (
                    x.str.strip()  # Always strip whitespace
                    .str.lstrip("0")  # Strip zeroes from the left
                    .str.rstrip("-.")  # Strip non-numerics from the right
                    .str.replace(",", "")  # ! ASSUME thousands separator is a ","
                    .replace({"": None if nullable else 0})  # Get pd.NA cells
                    .astype("Int64" if nullable else "int64")
                )

        return X


class CleanFloats(abstract.Transform, mixins.SchemaDriven):
    @utils.timed
    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        selection = self.meta.select_dtypes(include=["float"])

        for column in selection:
            x = X[column]

            if pd.api.types.is_string_dtype(x):
                nullable = self.schema.columns[column].nullable

                X[column] = (
                    x.str.strip()  # Always strip whitespace
                    .str.lstrip("0")  # Strip zeroes from the left
                    .str.rstrip("-.")  # Strip non-numerics from the right
                    .str.replace(",", "")  # ! ASSUME thousands separator is a ","
                    .replace({"": None if nullable else 0})  # Get pd.NA cells
                    .astype("Float64" if nullable else "float64")
                )

        return X


class RenameAliases(abstract.Transform, mixins.SchemaDriven):
    """
    RenameAliases reads column metadata from a `schema` and renames any
    `aliases` to the given column name in the schema.

    TODO: Test - Missing aliases should fail silently.
    TODO: Test - Duplicate alias matches should raise an error.

    Params

        :type schema: pa.DataFrameSchema
        :param schema:

            A Pandera schema to be used as the validator. Read the following
            docs. <https://pandera.readthedocs.io/en/stable/index.html>

    """

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        # Rename all the configured aliases back to the column name
        aliases = {
            alias: name
            for name, column in self.schema.columns.items()
            for alias in (getattr(column, "metadata") or {}).get("aliases", [])
            if alias in X.columns
        }

        # Rename the column title to the column name as well
        titles = {
            column.title: name
            for name, column in self.schema.columns.items()
            if column.title in X.columns
        }

        # fmt:off
        return X.rename(columns=aliases).rename(columns=titles)
        # fmt:on


class SetConst(abstract.Transform):
    def __init__(self, **constants):
        self.constants = constants

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        return X.assign(**self.constants)


__all__ = [
    "CleanBooleans",
    "CleanFloats",
    "CleanIntegers",
    "CleanStrings",
    "Identity",
    "RenameAliases",
    "SetConst",
]
