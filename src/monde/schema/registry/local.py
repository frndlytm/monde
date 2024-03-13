import pathlib
from functools import lru_cache
from typing import Iterable

from monde.schema import io
from monde.schema.registry.abstract import ISchemaRegistry
from monde.schema.registry.exceptions import RegistryKeyError
from monde.schema.spec import SchemaModel


class LocalSchemaRegistry(ISchemaRegistry):
    """
    ``LocalSchemaRegistry``
    =======================

    A ``LocalSchemaRegistry`` implements ``ISchemaRegistry`` by pointing to
    a path on the local file-system as the root for listing and reading
    schemas.

    Arguments
    ---------

        ``root``:
            The root directory form which we rglob schema file.

        ``suffix``:
            The suffix of the files to rglob, controls which reader to use.

        ``storage_options``:
            Extra file-system options for the reader. (See fsspec and s3fs).

    """

    # fmt:off
    def __init__(
        self,
        root: str,
        suffix: str = "xlsx",
        **storage_options
    ):
        super().__init__(reader=io.reader(suffix, **storage_options))

        self.root = pathlib.Path(root)
        self.suffix = suffix

    def exists(self, key: str) -> bool:
        """Does the key ``exists`` where the registry is stored?"""
        fullpath = self.root.joinpath(key)
        return fullpath.exists() and fullpath.is_file()

    def keys(self) -> Iterable[str]:
        """List all the ``keys`` stored in the registry."""
        for child in self.root.glob(f"**/*.{self.suffix}"):
            if child.is_file() and not child.stem.startswith("~$"):
                yield str(child.relative_to(self.root))

    @lru_cache(maxsize=None)
    def get(self, key: str) -> SchemaModel:
        """``get`` a ``SchemaModel`` for a particular key in the registry."""
        # Red the contents of the schema file from the local file path
        if not self.exists(key): raise RegistryKeyError(key)
        fullpath = self.root.joinpath(key)

        # Parse the specification into a pydantic.BaseModel
        return self.reader(str(fullpath))

    # fmt:on
