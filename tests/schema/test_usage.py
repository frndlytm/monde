import pandas as pd
import pytest
import sqlalchemy as sql
from pydantic import TypeAdapter

from monde.schema import DataFrameSchemaBuilder, ModelBuilder, TableBuilder


@pytest.mark.parametrize(
    "schema, datapath",
    [
        ("example/FinancialSample.xlsx", "FinancialSample.xlsx"),
    ],
)
def test_builders(data, registry, subtests, schema: str, datapath: str):
    # fmt:off
    definition = registry.get(schema)
    X = pd.read_excel(
        str(data.joinpath(datapath)),
        names=definition.names,
        dtype=definition.dtype,
        header=0,
    )

    with subtests.test(msg="pandera"):
        Schema = DataFrameSchemaBuilder.build(definition, coerce=True)
        Schema.validate(X)

    with subtests.test(msg="pydantic"):
        Model = TypeAdapter(list[ModelBuilder.build(definition)])  # type: ignore
        Model.validate_json(X.to_json(orient="records", date_format="iso"))

    with subtests.test(msg="sqlalchemy"):
        Table = TableBuilder.build(
            definition, "test_table", sql.MetaData(schema="test")
        )

        for row in X.to_dict(orient="records"):
            assert set(row.keys()) == set(Table.columns.keys())
    # fmt:on
