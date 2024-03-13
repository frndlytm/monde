import sys
from functools import partial, singledispatchmethod
from typing import Optional, Set, Type, TypeVar, Union

import pydantic
from dateutil.parser import isoparse
from typing_extensions import Annotated

from monde.schema import spec
from monde.schema.builders.abstract import IBuilder

NumericT = TypeVar("NumericT", int, float)
DateLikeSchemaField = Union[
    spec.field.DateSchemaField,
    spec.field.DatetimeSchemaField,
]


def maxsize(
    s: Optional[str],
    size: int = sys.maxsize,
    truncate: bool = False,
) -> Optional[str]:
    """
    `maxsize` asserts the string is not larger than it is allowed to be.

    Parameters:
    -----------
        s (Optional[str]):
            The string we're clipping the size.

        size (int, optional):
            The max size of the field. Defaults to sys.maxsize.

        truncate (bool, optional):
            Should we truncate the string? Defaults to False.

    """

    # Handle the optional case
    # fmt:off
    if s is None: return s
    # fmt:on

    # Raise a validation error if we shouldn't truncate and the string is too long
    if not truncate and len(s) > size:
        raise ValueError(f"String {s} is longer than {size} characters.")

    # Truncate the string if needed
    return s[:size] if truncate else s


def rounded(
    x: Optional[float],
    precision: int = sys.maxsize,
    scale: int = sys.maxsize,
) -> Optional[float]:
    """
    Following from SQL Servers defintion of precision and scale for decimal types,

        https://learn.microsoft.com/en-us/sql/t-sql/data-types/
            precision-scale-and-length-transact-sql?view=sql-server-ver16

    The number 123.45 has precision = 5 digits and scale = 2 digits after the
    decimal.

    This function rounds the float to scale and then validates that it can fit
    into the precision number of places.

    Parameters:
    -----------
        x (float, optional):
            The value to validate

        precision (int):
            The number of digits in a number. Defaults to sys.maxsize.

        scale (int):
            The number of digits to the right of the decimal point in a number.
            Defaults to sys.maxsize.

    """

    # fmt:off
    if x is None: return x
    # fmt:on

    x = round(x, scale)
    return x


def oneOf(x: Optional[str], symbols: tuple[str] = ()) -> Optional[str]:  # type: ignore
    # fmt:off
    if x is None: return x
    # fmt:on

    if x not in symbols:
        raise ValueError(f"'{x}' is not one of {symbols}.")

    return x


class ModelBuilder(IBuilder[Type[pydantic.BaseModel]]):
    @singledispatchmethod
    @classmethod
    def build_dtype(cls, _: spec.field.SchemaFieldModel) -> type:  # type: ignore
        return object

    @build_dtype.register
    @classmethod
    def _(cls, field: spec.field.BooleanSchemaField):
        return field.get_python_type()

    @build_dtype.register
    @classmethod
    def _(cls, field: spec.field.StrSchemaField):
        return Annotated[
            field.get_python_type(),
            # truncate the string to its max size (raises ValueError if not truncate)
            pydantic.AfterValidator(
                partial(maxsize, size=field.size, truncate=field.truncate)
            ),
        ]

    @build_dtype.register
    @classmethod
    def _(cls, field: spec.field.IntSchemaField):
        return Annotated[
            field.get_python_type(),
            pydantic.BeforeValidator(
                lambda x: (
                    (int(x.strip().lstrip("0")) or 0) if isinstance(x, str) else x
                )
            ),
        ]

    @build_dtype.register(spec.field.DateSchemaField)
    @build_dtype.register(spec.field.DatetimeSchemaField)
    @classmethod
    def _(cls, field: DateLikeSchemaField):
        # TODO: Defaulting to isoparse for now, but that doesn't rely on the field
        # properties for ``unit``, ``tz``, and ``datefmt``.
        return Annotated[field.get_python_type(), pydantic.BeforeValidator(isoparse)]

    @build_dtype.register
    @classmethod
    def _(cls, field: spec.field.DecimalSchemaField):
        return Annotated[
            # Round the float to the configured scale and precision
            field.get_python_type(),
            pydantic.AfterValidator(
                partial(rounded, scale=field.scale, precision=field.precision)
            ),
        ]

    @build_dtype.register
    @classmethod
    def _(cls, field: spec.field.CategorySchemaField):
        symbols: Set[str] = {f.lower() for f in field.symbols}

        return Annotated[
            field.get_python_type(),
            pydantic.BeforeValidator(lambda x: str(x).lower()),
            pydantic.AfterValidator(partial(oneOf, symbols=symbols)),
        ]

    @classmethod
    def build_field(
        cls, field: spec.field.SchemaFieldModel
    ) -> pydantic.fields.FieldInfo:
        return pydantic.Field(  # type:ignore
            default=field.default or (... if field.required else None),
            alias=pydantic.AliasChoices(field.title, *field.aliases),  # type: ignore
            serialization_alias=field.name,
            description=field.doc,
            exclude=field.exclude,
        )

    @classmethod
    def build(
        cls, schema: spec.SchemaModel, *args, **kwargs
    ) -> Type[pydantic.BaseModel]:
        # Make a pydantic.SchemaField and its type annotation
        __fields__ = {
            field.name: (
                (
                    Optional[cls.build_dtype(field)]
                    if field.nullable
                    else cls.build_dtype(field)
                ),
                cls.build_field(field),
            )
            for field in schema.fields
        }

        # Dynamic model creation function from the pydantic module
        return pydantic.create_model(  # type: ignore
            schema.name, *args, __module__=schema.namespace, **__fields__, **kwargs
        )


__all__ = ["ModelBuilder"]
