import pytest

from monde.schema.io import reader
from monde.schema.spec import SchemaModel


@pytest.mark.parametrize(
    "type_, schemapath", [
        ("xlsx", "./schemas/FinancialSample.xlsx"),
    ]
)
def test_read(assets, type_: str, schemapath: str):
    schema = reader(type_)(str(assets.joinpath(schemapath)))
    assert isinstance(schema, SchemaModel)
