import pytest

from monde.schema import LocalSchemaRegistry


@pytest.fixture
def registry(cwd):
    return LocalSchemaRegistry(
        root=str(cwd.joinpath("schemas")),
        suffix="xlsx",
    )
