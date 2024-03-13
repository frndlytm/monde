import os
from functools import lru_cache
from typing import Iterable

import s3fs

from monde.schema import io
from monde.schema.registry.abstract import ISchemaRegistry
from monde.schema.registry.exceptions import RegistryKeyError
from monde.schema.spec import SchemaModel


class S3SchemaRegistry(ISchemaRegistry):
    """
    ``S3SchemaRegistry``
    =======================

    A ``S3SchemaRegistry`` is a versioned ``ISchemaRegistry`` by pointing to
    a (Bucket, Prefix, Version) in S3.

    Arguments
    ---------

        ``bucket``:
            The S3 bucket containing the schema files.

        ``prefix``:
            The root folder inside the bucket containing the schema files.

        ``version``:
            A version sub-folder in the root ``prefix``. Equivalently, you could
            set `prefix={prefix}/{version}` and `version=""`.

        ``suffix``:
            The suffix of the files to rglob, controls which reader to use.

        ``storage_options``:
            Extra file-system options for the reader. (See fsspec and s3fs).

    """

    # fmt:off
    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        version: str = "latest",
        suffix: str = "xlsx",
        **storage_options,
    ):
        super().__init__(reader=io.reader(suffix, **storage_options))

        self.fs = s3fs.S3FileSystem(**storage_options)
        self.Bucket = bucket
        self.Prefix = prefix
        self.Version = version
        self.Suffix = suffix

    @property
    def root(self):
        return os.path.join(f"s3://{self.Bucket}", self.Prefix, self.Version)

    def __get_s3_path(self, key: str) -> str:
        if key.startswith("./"): key = key.lstrip("./")
        return os.path.join(self.root, key)

    def exists(self, key: str):
        """Does the key ``exists`` where the registry is stored?"""
        return self.fs.exists(self.__get_s3_path(key))

    def keys(self) -> Iterable[str]:
        """List all the ``keys`` stored in the registry."""
        objects = self.fs.glob(f"{self.root}/**/*.{self.Suffix}")
        maxsplit = len(list(filter(bool, [self.Bucket, self.Prefix, self.Version])))
        yield from (obj.split("/", maxsplit=maxsplit)[-1] for obj in objects)

    @lru_cache(maxsize=None)
    def get(self, key: str) -> SchemaModel:
        """``get`` a ``SchemaModel`` for a particular key in the registry."""
        if not self.exists(key): raise RegistryKeyError(key)
        return self.reader(self.__get_s3_path(key))

    # fmt:on
