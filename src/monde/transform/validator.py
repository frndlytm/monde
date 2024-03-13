import json
import logging
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Callable, Dict

import pandas as pd
import pandera as pa
import pandera.errors

from monde.transform import mixins
from monde.transform import abstract


# fmt:off
def noop(*args, **kwargs): ...


def schema_errors(e: pa.errors.SchemaErrors) -> dict:
    errors = e.failure_cases

    return {
        "types": "ExcelSheet",
        "data": (
            errors[errors.schema_context == "DataFrameSchema"][
                ["check", "failure_case"]
            ].to_dict(orient="records")
        ),
    }


def column_errors(e: pa.errors.SchemaErrors) -> dict:
    errors = e.failure_cases

    return {
        "types": "ExcelSheet",
        "data": (
            errors[errors.schema_context == "Column"][["column", "check", "index"]]
            .groupby(["column", "check"], as_index=False)
            .count()
            .rename(columns={"index": "count"})
            .to_dict(orient="records")
        ),
    }


def data_errors(e: pa.errors.SchemaErrors) -> dict:
    errors = e.failure_cases

    index_errors = (
        errors[errors.schema_context == "Column"]
        .assign(errors=lambda x: x.to_dict(orient="records"))
        .groupby("index")["errors"]
        .apply(list)
    )

    return {
        "types": "ExcelSheet",
        "data": (
            pd.merge(index_errors, e.data, left_index=True, right_index=True)
            .to_dict(orient="records")
        ),
    }


def error_report(e: pa.errors.SchemaErrors) -> dict:
    return {
        "types": "ExcelWorkbook",
        "sheets": {
            "Schema Errors": schema_errors(e),
            "Column Errors": column_errors(e),
            "Data Errors": data_errors(e),
        },
    }


def log_error_report(e: pa.errors.SchemaErrors):
    def default(x):
        if isinstance(x, (date, datetime)):
            return x.isoformat()
        elif isinstance(x, Decimal):
            return float
        else:
            return x

    report = error_report(e)
    types_ = report["types"]
    message = json.dumps(report, default=default, indent=2)

    logging.error(f"{types_} {message}")


def raise_error_report(e: pa.errors.SchemaErrors):
    log_error_report(e)
    raise e

# fmt:on


class Validator(abstract.Transform, mixins.SchemaDriven):
    """
    Validator applies schema validations to the DataFrame and removes invalid
    rows from the result of the transform.

    See <https://pandera.readthedocs.io/en/stable/index.html>.

    Params

        :type schema: pa.DataFrameSchema
        :param schema:

            A Pandera schema to be used as the validator. Read the following
            docs. <https://pandera.readthedocs.io/en/stable/index.html>

        :type error_handler: Callable[[pd.DataFrame], None]
        :param error_handler:

            A function that takes a DataFrame and returns nothing, a pure
            sink function, like dumping the data to file or data table.

    Usage

        >>> schema = pa.DataFrameSchema(...)
        >>> validate = Validator(schema)
        >>> X = pd.read_csv("s3://path/to/file.csv")
        >>> good = validator(X)
        >>> bad = X.drop(index=good.index)

    """

    @classmethod
    def from_model(cls, model: pa.DataFrameModel):
        return cls(model.to_schema())

    @classmethod
    def from_file(cls, fp: str):
        return cls(pa.DataFrameSchema.from_json(fp))

    def __init__(
        self,
        schema: pa.DataFrameSchema,
        error_handler: Callable[[Exception], Any] = log_error_report,  # type: ignore
    ):
        self.schema = schema
        self.error_handler = error_handler
        self.fit_params: Dict[str, Any] = dict()

        # Default to an empty DataFrame with the panderas error_cases schema
        self.errors = pd.DataFrame(
            columns=[
                "schema_context",
                "column",
                "check",
                "check_number",
                "failure_case",
                "index",
            ],
        )

    @property
    def index_errors(self):
        return self.errors[self.errors.schema_context == "Column"]

    @property
    def extra_column_errors(self):
        return self.errors.query(
            "schema_context == 'DataFrameSchema'"
            " and check == 'column_in_dataframe'"
        )

    @property
    def missing_column_errors(self):
        return self.errors.query(
            "schema_context == 'DataFrameSchema'"
            " and check == 'column_in_schema'"
        )

    def fit_transform(self, X: pd.DataFrame, y=None, **fit_params) -> pd.DataFrame:
        # FIT
        # Validate the DataFrame lazily (i.e. run all validations
        # then raise errors together)
        fit_params.setdefault("lazy", True)
        fit_params.setdefault("inplace", False)
        self.fit_params = dict(fit_params)

        # fmt:off
        try:
            self.schema.validate(X, **fit_params)

        except pandera.errors.SchemaErrors as e:
            # Cache the error cases
            self.errors = e.failure_cases
            self.error_handler(e)
            
        # TRANSFORM
        return (
            # Remove invalid rows from the input
            X.drop(index=self.index_errors["index"])
            # Remove extra columns
            .drop(columns=self.extra_column_errors["failure_case"])
            # Insert extra columns in their right places
            .pipe(self.schema.validate, **fit_params)
        )
        # fmt:on
