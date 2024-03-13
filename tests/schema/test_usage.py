import pandas as pd
import pytest
import sqlalchemy as sql

from pydantic import TypeAdapter

from monde.schema import (
    DataFrameSchemaBuilder,
    ModelBuilder,
    TableBuilder,
)


@pytest.mark.parametrize(
    "schema, datapath", [
        (
            "example/FinancialSample.xlsx",
            "./data/FinancialSample.xlsx",
        ),
    ]
)
def test_builders(assets, registry, subtests, schema: str, datapath: str):
    # fmt:off
    definition = registry.get(schema)
    data = pd.read_excel(
        str(assets.joinpath(datapath)),
        names=definition.names,
        dtype=definition.dtype,
        header=0,
    )
    
    with subtests.test(msg="pandera"):
        Schema = DataFrameSchemaBuilder.build(schema, coerce=True)
        Schema.validate(data)

    with subtests.test(msg="pydantic"):
        Model = TypeAdapter(list[ModelBuilder.build(schema)])  # type: ignore
        Model.validate_json(data.to_json(orient="records", date_format="iso"))

    with subtests.test(msg="sqlalchemy"):
        Table = TableBuilder.build(schema, "test_table", sql.MetaData(schema="test"))
        for row in data.to_dict(orient="records"):
            assert set(row.keys()) == set(Table.columns.keys())
    # fmt:on
