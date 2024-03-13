import pytest

from monde.schema.io import reader
from monde.schema.spec import SchemaModel


@pytest.mark.parametrize(
    "type_, schemapath", [
        ("xlsx", "example/FinancialSample.xlsx"),
    ]
)
def test_read(cwd, type_: str, schemapath: str):
    schema = reader(type_)(str(cwd.joinpath("schemas", schemapath)))
    assert isinstance(schema, SchemaModel)
