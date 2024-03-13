"""
============================
 monde.schema.spec.schema
============================
-------------------------------------------
 Data Structures to Serialize Schema Files
-------------------------------------------

A ``Schema`` consists of zero or more ``SchemaFields``, ``Checks``,
and ``Constraints``.

A ``Schema`` can contain additional metadata.

"""
import json
import string

from typing import Any, Dict, Iterable, List, Literal, Optional, Set, Tuple

import pydantic

from .field import SchemaFieldModel, SchemaField

__all__ = ["Schema"]

CheckMode = Literal["after", "before", "wrap"]


class CheckModel(pydantic.BaseModel):
    """
    ``Check``
    =========

    A ``Check`` is a named validation step the occurs somewhere during data
    validation and type coercion in a given validation pipeline.

    """
    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)

    """
    Properties
    ----------
    
    ``name``:
        The name of the Check to uniquely identify validation errors.

    ``field``:
        The name of the field on whose to apply the ``Check``.

    ``mode``:
        The processing order of the Check, i.e. before, after, or wrapped
        around the rest of type validation.

    ``kwargs``:        
        A mapping containing fixed kwargs for the check. See also
        https://docs.python.org/3/library/functools.html#functools.partial
    )
    
    """
    name: str = pydantic.Field(
        default=...,
        description="The name of the Check to uniquely identify validation errors."
    )
    field: str = pydantic.Field(
        default=...,
        description="The name of the field on whose to apply the ``Check``.",
    )
    mode: CheckMode = pydantic.Field(
        default="after",
        description=(
            "The processing order of the Check, i.e. before, after, or wrapped"
            "around the rest of type validation."
        ),
    )
    kwargs: Dict[str, Any] = pydantic.Field(
        default_factory=dict,
        description=(
            "A mapping containing fixed kwargs for the check. See also "
            "https://docs.python.org/3/library/functools.html#functools.partial"
        ),
    )
    
    @pydantic.field_validator("kwargs")
    @classmethod
    def coerce_kwargs(cls, kwargs: str) -> Dict[str, Any]:
        return json.loads(kwargs)


ConstraintType = Literal["index", "primary_key", "unique"]


class ConstraintModel(pydantic.BaseModel):
    """
    ``Constraint``
    ==============

    A ``Constraint`` is a named validation step that occurs after value
    validation and type coercion.
    
    A ``Constraint`` checks 

    """
    model_config= pydantic.ConfigDict(populate_by_name=True)

    name: str = pydantic.Field(
        default=...,
        description="The name of the constraint in the target type system.",
    )
    type_: ConstraintType = pydantic.Field(
        default=...,
        alias="type",
        description=(
            "The type of constraint to apply specifically within the file "
            "(one of 'index', 'primary_key', 'unique')."
        )
    )
    fields: Set[str] = pydantic.Field(
        default_factory=set,
        description="A set of fields over which the constraint applies.",
    )


class SchemaModel(pydantic.BaseModel):
    """
    Schema
    ======

    A ``Schema`` consists of zero or more ``SchemaFields``, ``Checks``, and
    ``Constraints``.

    A ``Schema`` can contain additional metadata.
    
    """
    model_config= pydantic.ConfigDict(arbitrary_types_allowed=True)

    """
    Properties
    ----------

    name:
        The Python ``__class__.__name__`` of the generated validator.

    namespace:
        The Python ``__module__.__name__`` of the generated validator.

    fields:
        The ``SchemaFieldModels`` that define how to type-coerce fields.

    checks:
        Special ``Checks`` that run on specific fields in specific places of
        the processing pipeline.

    constraints:
        Special ``Constraints`` that run on the entire dataset after processing
        and type coercion.

    metadata:
        Schemaless customizable metadata properties.

    """
    name: str = pydantic.Field(
        default=...,
        description="The Python ``__class__.__name__`` of the generated validator.",
    )
    namespace: Optional[str] = pydantic.Field(
        default=None,
        description="The Python ``__module__.__name__`` of the generated validator.",
    )
    fields: List[SchemaFieldModel] = pydantic.Field(
        default_factory=list,
        description="The ``SchemaFieldModels`` that define how to type-coerce fields.",
    )
    checks: List[Check] = pydantic.Field(
        default_factory=list,
        description=(
            "Special ``Checks`` that run on specific fields in specific places of "
            "the processing pipeline."
        ),
    )
    constraints: List[Constraint] = pydantic.Field(
        default_factory=list,
        description=(
            "Special ``Constraints`` that run on the entire dataset after processing "
            "and type coercion."
        ),
    )
    metadata: Dict[str, Any] = pydantic.Field(
        default_factory=dict,
        description="Schemaless customizable metadata properties.",
    )

    """
    Computed
    --------
    """
    @property
    def names(self) -> List[str]:
        """
        ``names``:

            A sequence of names of the fields in the schema, for easily making
            readers. Should be passable to ``pd.read_*``

        """
        return [field.name for field in self.fields]

    @property
    def dtype(self) -> Dict[str, str]:
        """
        ``dtype``:

            Should be passable to ``pd.read_*`` as the ``dtype`` kwarg.

        """
        return {field.name: field.get_pandas_safe_type() for field in self.fields}
    
    @property
    def colspecs(self) -> List[Tuple[Optional[int], Optional[int]]]:
        """
        ``colspecs``:

            See ``pd.read_fwf`` for the ``colspecs`` kwarg.

        """
        return [(field.start, field.end) for field in self.fields]

    """
    Validators
    ----------
    """
    @pydantic.field_validator("fields", mode="before")
    @classmethod
    def coerce_fields(cls, fields: List[Dict[str, Any]]) -> List[SchemaFieldModel]:
        """
        ``coerce_fields`` via the ``SchemaField`` factory function prior to
        validating that the types are satisfied.
        """
        return list(
            sorted(
                [SchemaField(**field) for field in fields],  # type: ignore
                key=lambda f: f.index
            )
        )

    """
    Methods
    -------
    """
    def select_dtypes(
        self,
        include: Optional[Set[str]] = None,
        exclude: Optional[Set[str]] = None,
    ) -> Dict[str, SchemaFieldModel]:

        assert (include, exclude) != (None, None)
        include = set(include or [])
        exclude = set(exclude or [])

        def normalized_dtype(d: str) -> str:
            return d.strip().rstrip(string.digits).lower()

        def iterator() -> Iterable[Tuple[str, SchemaFieldModel]]:
            # * Prioritize exclusions over inclusions *
            for f in self.fields:
                ndtype = normalized_dtype(f.dtype)

                # Filter anything explicitly listed in ``exclude``.
                if f.dtype in exclude: continue
                elif ndtype in exclude: continue

                # Yield anything explicitly listed in ``include``.
                elif f.dtype in include: yield (f.name, f)
                elif ndtype in include: yield (f.name, f)

        return dict(iterator())


def Schema(**definition: Dict[str, Any]) -> "SchemaModel":
    """
    ``Schema``
    ==========

    ``Schema`` is a factory function responsible for making instances of 
    ``SchemaModels`` by additionally making ``SchemaFieldModels``,
    ``Checks``, and ``Constraints`` from the schema ``definition``.
    
    """
    return SchemaModel.model_validate(definition)  # type: ignore


# export
__all__ = [
    "CheckModel",
    "CheckMode",
    "ConstraintModel",
    "ConstraintType",
    "Schema",
    "SchemaModel",
]
