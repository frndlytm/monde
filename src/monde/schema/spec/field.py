"""
===========================
 monde.schema.spec.field
===========================
-----------------------------------------------------
 Data Structures to Serialize Fields in Schema Files
-----------------------------------------------------

A ``SchemaField`` is a strongly-typed field on a flat data structure, like
a table on a flat storage medium like a relational database or file.

A ``Schema`` consists of zero or more ``SchemaFields``.

"""

import abc
import decimal
import enum
import string
import sys
import zoneinfo
from datetime import date, datetime
from functools import cached_property
from typing import Any, Dict, Generic, List, Optional, Set, Type, TypeVar

import babel.numbers
import pydantic

T = TypeVar("T")

RE_PYTHON_IDENTIFIER = r"\b[A-Za-z\_][\d\w]*\b"


class SchemaFieldModel(pydantic.BaseModel, Generic[T]):
    """
    SchemaFieldModel[T]
    ==================

    A ``SchemaFieldModel[T]`` is the base class for a row in ``fields`` worksheet
    or an Excel schema file.

    How it works?
    -------------

    A ``SchemaFieldModel[T]`` must implement ``get_python_type``, and *may* implement
    any number of additional checks and properties; however, any new properties
    must exist in schema files that reference the type.

    Examples::

        class IntSchemaField(SchemaFieldModel[int]):
            def get_python_type(self):
                return int


        class DatetimeSchemaField(SchemaFieldModel[datetime.datetime]):
            unit: str = "d"
            tz: str = "UTC"
            datefmt: str = pydantic.Field("%Y-%m-%d %X")

            @pydantic.field_validator("tz")
            def tz_in_available_timezones(cls, tz: str):
                if tz not in zoneinfo.available_timezones():
                    raise ValueError(f"'{tz}' not in zoneinfo.available_timezones()")
                return tz

            def get_python_type(self):
                return datetime.datetime

    """

    """
    ``model_config``
    ----------------

    See, https://docs.pydantic.dev/latest/api/config/#pydantic.config.ConfigDict
    for more information about these properties. Some things to note:

      `extra="ignore"`

          Parsing a SchemaFieldModel excludes properties that are not on the specific
          sub-class to ensure that we can't access properties that a type isn't
          supposed to have.

      `use_enum_values=True`

          Since we use enum.StrEnum to do validation, we want to ensure that
          the string is used always.

    """
    model_config = pydantic.ConfigDict(
        arbitrary_types_allowed=True,
        allow_inf_nan=True,
        extra="ignore",
        use_enum_values=True,
        str_strip_whitespace=True,
    )

    """
    Mapping Properties
    ------------------

    name:
        Target field name in the database.

    title:
        Source field name, or title_cased field name.

    aliases:
        Set of possible aliases, (automatically includes `title`).

    doc:
        Plain-text document describing the field.

    dtype:
        Data type alias supported by numpy / pandas, (like `int64`). See,
        https://pandas.pydata.org/docs/user_guide/basics.html#dtypes.

    default:
        The default value, parseable as the generic type-var of the  subclass.

    """
    index: int = pydantic.Field(
        ...,
        description="Sort order key of the fields in a schema.",
    )
    name: str = pydantic.Field(
        default=...,
        description="Target field name in the database.",
        pattern=RE_PYTHON_IDENTIFIER,
        max_length=63,
    )
    title: str = pydantic.Field(
        default=...,
        description="Source field name, or title_cased field name.",
    )
    aliases: Optional[Set[str]] = pydantic.Field(
        default_factory=set,
        description="Set of possible aliases, (automatically includes `title`).",
    )
    doc: Optional[str] = pydantic.Field(
        default=None,
        description="Plain-text document describing the field.",
    )
    dtype: str = pydantic.Field(
        default=...,
        description=(
            "Data type alias supported by numpy / pandas, (like `int64`). See, "
            "https://pandas.pydata.org/docs/user_guide/basics.html#dtypes."
        ),
    )
    default: Optional[T] = pydantic.Field(
        default=None,
        description=(
            "The default value, parseable as the generic type-var of the " "subclass."
        ),
    )

    """
    Flags
    -----

    Flags describe how the field should be validated in the various
    serialization frameworks supported by ``monde.schema.builders``.

    ``required:``
        Value must be provided during validation, even if it's None / NULL / NaN.

    ``nullable:``
        Value is allowed to be None / NULL / NaN.

    ``protect:``
        Value must be protected with higher security, like encryption at rest.

    ``exclude:``
        Value can be excluded, like FILLER and EOR fields.

    """
    required: bool = pydantic.Field(
        default=False,
        description=(
            "Value must be provided during validation, even if it's "
            "None / NULL / NaN."
        ),
    )
    nullable: bool = pydantic.Field(
        default=False,
        description="Value is allowed to be None / NULL / NaN.",
    )
    protect: bool = pydantic.Field(
        default=False,
        description=(
            "Value must be protected with higher security, like " "encryption at rest."
        ),
    )
    exclude: bool = pydantic.Field(
        default=False,
        description="Value can be excluded, like FILLER and EOR fields.",
    )

    """
    Fixed Width Properties
    -----------------------

    Fixed Width properties define the span of the value inside a single line in
    a data file. This should be purely for parsing, and should be guaranteed to
    exist if the schema is of type "fwf".

    ``start``:
        Start character position (inclusive) of the span containing the value.

    ``end``:
        End character position (exclusive) of the span containing the value.

    ``length``:
        Length of the span containing the value.

    """
    start: Optional[int] = pydantic.Field(None)
    end: Optional[int] = pydantic.Field(None)
    length: Optional[int] = pydantic.Field(None)

    """
    Validators
    ----------

    ``no_empty_aliases``:
        Assert that all aliases are stripped and contain information.

    ``pandas_upper_type_is_nullable``:
        pandas assumes that types starting with upper-cased letters are nullable.
        This applies when needing a nullable ``int``, which parses ``nan`` as
        ``float`` by default.

    """

    @pydantic.field_validator("aliases")
    @classmethod
    def no_empty_aliases(cls, value: Set[str]) -> Set[str]:
        stripped = [s.strip() for s in value]
        return set(filter(bool, stripped))

    @pydantic.model_validator(mode="before")
    @classmethod
    def pandas_upper_type_is_nullable(cls, data: dict) -> dict:
        dtype, nullable = data["dtype"], data["nullable"]
        if dtype[0].isupper() and not nullable:
            raise ValueError(f"'{dtype}' is nullable, but nullable={nullable}.")
        return data

    @abc.abstractmethod
    def get_python_type(self) -> type:
        return NotImplemented

    @abc.abstractmethod
    def get_pandas_type(self) -> str:
        return NotImplemented

    @abc.abstractmethod
    def get_pandas_safe_type(self) -> str:
        return NotImplemented


class BooleanSchemaField(SchemaFieldModel[bool]):
    def get_python_type(self):
        return bool

    def get_pandas_type(self):
        return "boolean"

    def get_pandas_safe_type(self):
        return "string"


class DateSchemaField(SchemaFieldModel[date]):
    """
    ``DateSchemaField``
    ===================

    Extra Properties
    ----------------
    ``unit``
        A pandas datetime unit, like ``"ns"``.

    ``tz``
        A timezone alias string, like ``"UTC"``.

    ``datefmt``
        A strftime / strptime date format string.

    """

    unit: str = pydantic.Field(
        default="s",
        description="A pandas datetime unit, like ``'ns'``.",
    )
    tz: str = pydantic.Field(
        default="UTC",
        description="A timezone alias string, like ``'UTC'``.",
    )
    datefmt: str = pydantic.Field(
        default="%Y-%m-%d",
        description="A strftime / strptime date format string.",
    )

    """
    Validators
    ----------
    """

    @pydantic.field_validator("tz")
    def tz_in_available_timezones(cls, tz: str) -> str:
        """
        ``tz_in_available_timzones`` asserts that the given ``tz`` is in
        ``zoneinfo.available_timezones()``
        """
        if tz not in zoneinfo.available_timezones():
            raise ValueError(f"'{tz}' not in zoneinfo.available_timezones()")
        return tz

    def get_python_type(self):
        return date

    def get_pandas_type(self):
        return f"datetime[{self.unit}, {self.tz}]"

    def get_pandas_safe_type(self):
        return "string"


class DatetimeSchemaField(SchemaFieldModel[datetime]):
    """
    ``DatetimeSchemaField``
    =======================

    Extra Properties
    ----------------

    ``unit``
        A pandas datetime unit, like ``"ns"``.

    ``tz``
        A timezone alias string, like ``"UTC"``.

    ``datefmt``
        A strftime / strptime date format string.

    """

    unit: str = pydantic.Field(
        default="ns",
        description="A pandas datetime unit, like ``'ns'``.",
    )
    tz: str = pydantic.Field(
        default="UTC",
        description="A timezone alias string, like ``'UTC'``.",
    )
    datefmt: str = pydantic.Field(
        default="%Y-%m-%d",
        description="A strftime / strptime date format string.",
    )

    """
    Validators
    ----------
    """

    @pydantic.field_validator("tz")
    def tz_in_available_timezones(cls, tz: str) -> str:
        """
        ``tz_in_available_timzones`` asserts that the given ``tz`` is in
        ``zoneinfo.available_timezones()``
        """
        if tz not in zoneinfo.available_timezones():
            raise ValueError(f"'{tz}' not in zoneinfo.available_timezones()")
        return tz

    def get_python_type(self):
        return datetime

    def get_pandas_type(self):
        return f"datetime[{self.unit}, {self.tz}]"

    def get_pandas_safe_type(self):
        return "string"


MAX_DECIMAL_SCALE = 131_072
MAX_DECIMAL_PRECISION = 16_383


class DecimalSchemaField(SchemaFieldModel[decimal.Decimal]):
    """
    ``DecimalSchemaField``
    ======================

    Extra Properties
    ----------------

    ``scale``
        How many digits make up the whole number?

    ``precision``
        How many digits are after the decimal point?

    """

    scale: Optional[float] = pydantic.Field(
        default=MAX_DECIMAL_SCALE,
        description="How many digits make up the whole decimal number?",
    )
    precision: Optional[float] = pydantic.Field(
        default=MAX_DECIMAL_PRECISION,
        description="How many digits are after the decimal point?",
    )

    def get_python_type(self):
        return float

    def get_pandas_type(self):
        return "Float64"

    def get_pandas_safe_type(self):
        return "Float64"


class CurrencySchemaField(SchemaFieldModel[decimal.Decimal]):
    """
    ``CurrencySchemaField``
    =======================

    Extra Properties
    ----------------

    ``currency``
        The alias for the currency symbol according to ``babel.numbers``.

    ``locale``
        The locale representing how a number parses at commas and periods
        (i.e.  '1,000.00' versus '1.000,00').

    """

    currency: str = pydantic.Field(
        default="USD",
        description="The alias for the currency symbol according to ``babel.numbers``.",
    )
    locale: str = pydantic.Field(
        default="en_US",
        description=(
            "The locale representing how a number parses at commas and periods "
            "(i.e.  '1,000.00' versus '1.000,00')."
        ),
    )

    @cached_property
    def currency_symbol(self) -> str:
        return babel.numbers.get_currency_symbol(self.currency, self.locale)

    def get_python_type(self):
        return float

    def get_pandas_type(self):
        return "string"

    def get_pandas_safe_type(self):
        return "string"


class CategorySchemaField(SchemaFieldModel[str]):
    """
    ``CategorySchemaField``
    =======================

    Extra Properties
    ----------------

    ``symbols``
        The complete list of valid symbols a value can be.

    """

    symbols: Set[str] = pydantic.Field(
        default_factory=set,
        description="The complete list of valid symbols a value can be.",
    )

    @pydantic.field_validator("symbols")
    @classmethod
    def no_empty_symbols(cls, value: List[str]):
        return set(filter(bool, value))

    def get_python_type(self):
        name = self.title.replace(" ", "")
        return enum.StrEnum(name, list(self.symbols))

    def get_pandas_type(self):
        return "category"

    def get_pandas_safe_type(self):
        return "string"


class IntSchemaField(SchemaFieldModel[int]):
    """
    ``IntSchemaField``
    ==================
    """

    def get_python_type(self):
        return int

    def get_pandas_type(self):
        return "Int64"

    def get_pandas_safe_type(self):
        return "Int64"


class ObjectSchemaField(SchemaFieldModel[object]):
    """
    ``ObjectSchemaField``
    =====================
    """

    def get_python_type(self):
        return object

    def get_pandas_type(self):
        return "object"

    def get_pandas_safe_type(self):
        return "object"


class StrSchemaField(SchemaFieldModel[str]):
    """
    ``StrSchemaField``
    ==================

    Extra Properties
    ----------------

    ``size``
        The maximum number of characters a value can contain.

    ``truncate``
        Should we trim excess characters from the right?


    """

    size: Optional[int] = pydantic.Field(
        default=sys.maxsize,
        description="The maximum number of characters a value can contain.",
    )
    truncate: Optional[bool] = pydantic.Field(
        default=False,
        description="Should we trim excess characters from the right?",
    )

    def get_python_type(self):
        return str

    def get_pandas_type(self):
        return "string"

    def get_pandas_safe_type(self):
        return "string"


"""
``SchemaFieldTypes``
====================

Map cleaned ``pandas`` string aliases to SchemaFieldModel implementers. This ensures
that our type mapping has special property validation built-in.

This ensures that we have structural validation on our schema files when we read
them.
"""
SchemaFieldTypes: Dict[str, Type[SchemaFieldModel]] = {
    "bool": BooleanSchemaField,
    "boolean": BooleanSchemaField,
    "category": CategorySchemaField,
    "currency": DecimalSchemaField,
    "date": DateSchemaField,
    "datetime": DatetimeSchemaField,
    "float": DecimalSchemaField,
    "decimal": DecimalSchemaField,
    "enum": CategorySchemaField,
    "int": IntSchemaField,
    "uint": IntSchemaField,
    "object": ObjectSchemaField,
    "string": StrSchemaField,
    "ssn": StrSchemaField,
    "zipcode": StrSchemaField,
}


def SchemaField(**definition: Dict[str, Any]) -> SchemaFieldModel:
    """
    ``SchemaField``
    ===============

    ``SchemaField`` is a factory function responsible for making instances of
    ``SchemaFieldModel`` depending on the given ``dtype`` in the field ``definition``.

    """
    # Get the dtype property (required or raises KeyError)
    dtype = str(definition["dtype"]) or "object"

    # Remove numbers from the type and lowercase it to get the SchemaFieldModel
    dtype = dtype.strip(string.digits + string.whitespace).lower()

    # Lookup the SchemaFieldModel and build it with the fielddef.
    return SchemaFieldTypes[dtype].model_validate(definition)


# export
__all__ = [
    "SchemaFieldModel",
    "SchemaField",
]
