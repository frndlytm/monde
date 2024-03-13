from functools import partial
from typing import Callable

from monde.schema import models
from monde.schema.io import xlsx, yaml

IReader = Callable[..., models.SchemaModel]


def reader(type_: str, **storage_options) -> IReader:
    # fmt:off
    if type_ == "xlsx": return partial(xlsx.read, **storage_options)
    elif type_ == "yaml": return partial(yaml.read, **storage_options)
    else: raise ValueError(f"Unsupported schema file type: '{type_}'")
    # fmt:on
