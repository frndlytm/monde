import abc

from typing import Generic, TypeVar

from monde.schema import spec

P = TypeVar("P")


class IBuilder(abc.ABC, Generic[P]):
    @classmethod
    @abc.abstractmethod
    def build_dtype(cls, field: spec.field.SchemaFieldModel) -> type:
        raise NotImplementedError()

    @classmethod
    @abc.abstractmethod
    def build_constraint(cls, field: spec.schema.Constraint) -> type:
        raise NotImplementedError()

    @classmethod
    @abc.abstractmethod
    def build_check(cls, field: spec.schema.Check) -> type:
        raise NotImplementedError()

    @classmethod
    @abc.abstractmethod
    def build_field(cls, field: spec.field.SchemaFieldModel):
        raise NotImplementedError()

    @classmethod
    @abc.abstractmethod
    def build(cls, schema: spec.schema.SchemaModel, *args, **kwargs) -> P:
        raise NotImplementedError()

    def __call__(self, schema: spec.schema.SchemaModel, *args, **kwargs) -> P:
        return self.build(schema, *args, **kwargs)
