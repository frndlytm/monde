import string

import sqlalchemy as sql

from monde.schema import spec
from monde.schema.builders.abstract import IBuilder

SqlDtypes = {
    # Base dtypes
    "bool": sql.BOOLEAN,
    "boolean": sql.BOOLEAN,
    "bytes": sql.BINARY,
    "category": sql.TEXT,
    "decimal": sql.FLOAT,
    "float": sql.FLOAT,
    "uint": sql.BIGINT,
    "int": sql.BIGINT,
    "integer": sql.BIGINT,
    "datetime": sql.DATETIME,
    "date": sql.DATE,
    "period": sql.DATE,
    "string": sql.TEXT,
    "timedelta": sql.TIME,
    "time": sql.TIME,
    # Extended dtypes (see schema.dtypes sub-module).
    "currency": sql.FLOAT,
    "ssn": sql.TEXT,
    "zipcode": sql.TEXT,
}


class TableBuilder(IBuilder[sql.Table]):
    @classmethod
    def build_dtype(cls, field: spec.field.SchemaFieldModel) -> type:
        # Standardize the type name down to lowers with no digits
        type_name = field.dtype.lower().strip(string.digits)
        # Lookup
        return SqlDtypes[type_name]

    @classmethod
    def build_field(cls, field: spec.field.SchemaFieldModel) -> sql.Column:
        return sql.Column(
            field.name,
            type_=cls.build_dtype(field),
            nullable=field.nullable,
            default=field.default,
            doc=field.doc,
        )

    @classmethod
    def build_constraint(
        cls,
        constraint: spec.schema.ConstraintModel,
    ) -> sql.Constraint | sql.Index:

        if constraint.type_ == "index":
            return sql.Index(constraint.name, *constraint.fields)

        elif constraint.type_ == "primary_key":
            return sql.PrimaryKeyConstraint(*constraint.fields, name=constraint.name)

        elif constraint.type_ == "unique":
            return sql.UniqueConstraint(*constraint.fields, name=constraint.name)

        else:
            raise ValueError(f"'{constraint.type_}' is not a valid ConstraintType.")

    @classmethod
    def build(cls, schema: spec.SchemaModel, *args, **kwargs) -> sql.Table:
        """
        TODO: Investigate sql.Metadata Caching

            Does sqlalchemy cache singleton instances of Metadata objects,
            or if I should implement caching myself?

        """
        # Map the builder over the schema.fields...
        __columns__ = list(map(cls.build_field, schema.fields))

        # Map the builder over the schema.constraints...
        __constraints__ = list(map(cls.build_constraint, schema.constraints))

        # Make a Table
        return sql.Table(*args, *__columns__, *__constraints__, **kwargs)


__all__ = ["TableBuilder"]
