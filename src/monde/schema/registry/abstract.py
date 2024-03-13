import abc

from monde.schema.io import IReader
from monde.schema.spec import SchemaModel


class ISchemaRegistry:
    def __init__(self, reader: IReader):
        self.reader = reader

    def __iter__(self):
        return iter(self.keys())

    @abc.abstractmethod
    def exists(self, key: str) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def keys(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def get(self, name: str) -> SchemaModel:
        raise NotImplementedError()
