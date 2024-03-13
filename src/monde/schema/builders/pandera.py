"""
================================
 `monde.schema.builders.pandera`
================================

Implement the IBuilder Interface so we can export this class to the root
of the project.

"""

from typing import Optional, Type, Union

import babel.numbers
import pandas as pd
import pandera as pa
from pandera import dtypes
from pandera.engines import pandas_engine
from pandera.engines.type_aliases import PandasObject

from monde.schema import spec
from monde.schema.builders.abstract import IBuilder

"""
Custom pandera.DataTypes
------------------------

Registering custom data types with the pandera pandas engine by example.

See https://pandera.readthedocs.io/en/stable/dtypes.html#example

"""
PanderaDtype = Union[pa.DataType, Type[pa.DataType], str]


@pandas_engine.Engine.register_dtype(
    equivalents=["boolean", pd.BooleanDtype, pd.BooleanDtype()]
)
@dtypes.immutable
class LiteralBool(pandas_engine.BOOL):
    truthy = ["TRUE", "T", "YES", "Y"]  # True
    falsey = ["FALSE", "F", "NO", "N"]  # False
    unknown = ["NA", "NULL", "NONE", "(BLANK)", "U", "UNKNOWN", ""]  # pd.NA

    def coerce(self, series: pd.Series) -> pd.Series:
        """Coerce a pandas.Series to boolean types."""
        if pd.api.types.is_object_dtype(series):
            series = series.str.strip().str.upper()
            series.replace(self.truthy, 1, inplace=True)
            series.replace(self.falsey, 0, inplace=True)
            series.replace(self.unknown, pd.NA, inplace=True)

        return series.astype("boolean")


@pandas_engine.Engine.register_dtype  # type: ignore
@dtypes.immutable
class Currency(pandas_engine.DataType):
    currency: str = "USD"
    locale: Optional[str] = "en_US"
    type = pd.Float64Dtype()

    @property
    def currency_symbol(self):
        return babel.numbers.get_currency_symbol(
            self.currency,
            locale=self.locale,
        )

    def coerce(self, s: PandasObject) -> PandasObject:
        """Pure coerce without catching exceptions."""
        return (
            s.astype("str")
            .str.replace(self.currency_symbol, "")
            .transform(babel.numbers.parse_decimal, locale=self.locale)
            .astype("Float64")
        )


@pandas_engine.Engine.register_dtype  # type: ignore
@dtypes.immutable
class SSN(pandas_engine.NpString):
    def coerce(self, s: PandasObject) -> PandasObject:
        """Coerce numeric series to a string pandas.Series."""
        # Coerce numerics to strings, this happens by default sometimes when
        # calling read_csv without dtypes
        return (
            s.astype("string")
            # Remove hyphens
            .str.replace("-", "")
            # Strip whitespace
            .str.lstrip()
            # Left justify with 0's
            .str.rjust(width=9, fillchar="0")
        )


@pandas_engine.Engine.register_dtype  # type: ignore
@dtypes.immutable
class ZipCode(pandas_engine.NpString):
    def coerce(self, s: PandasObject) -> PandasObject:
        """Coerce numeric series to a string pandas.Series."""
        return s.astype("string").str.rjust(5, "0")


class DataFrameSchemaBuilder(IBuilder[pa.DataFrameSchema]):
    """
    ``PanderaSchema``
    =================

    Implements the IBuilder[pa.DataFrameSchema] interface.

    """

    @classmethod
    def build_dtype(cls, field: spec.SchemaFieldModel) -> PanderaDtype:  # type: ignore
        # fmt:off
        date_types = (spec.field.DateSchemaField, spec.field.DatetimeSchemaField)
        if isinstance(field, date_types):
            return pandas_engine.DateTime(  # type: ignore
                unit=field.unit,
                tz=field.tz,
                to_datetime_kwargs={
                    "format": field.datefmt,
                    "errors": "coerce",
                },
            )

        elif isinstance(field, spec.field.BooleanSchemaField): return LiteralBool
        elif field.dtype == "currency": return Currency
        elif field.dtype == "ssn": return SSN
        elif field.dtype == "zipcode": return ZipCode
        else: return field.dtype
        # fmt:on

    @classmethod
    def build_field(cls, field: spec.SchemaFieldModel) -> pa.Column:
        return pa.Column(
            name=field.name,
            title=field.title,
            description=field.doc,
            dtype=cls.build_dtype(field),
            default=field.default,
            nullable=field.nullable,
            metadata={
                "start": field.start,
                "end": field.end,
                "length": field.length,
                "protect": field.protect,
            },
        )

    @classmethod
    def build(cls, schema: spec.SchemaModel, **kwargs) -> pa.DataFrameSchema:
        # BUILD a field for each field in the schema...
        fields = {field.name: cls.build_field(field) for field in schema.fields}

        # BUILD a pa.DataFrameSchema
        return pa.DataFrameSchema(  # type: ignore
            columns=fields,
            name=schema.name,
            metadata=schema.metadata,
            **kwargs,
        )


__all__ = ["Currency", "LiteralBool", "SSN", "ZipCode", "DataFrameSchemaBuilder"]
