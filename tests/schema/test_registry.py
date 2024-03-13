from monde.schema import LocalSchemaRegistry


def test_LocalSchemaRegistry(assets):
    registry = LocalSchemaRegistry(
        root=assets.joinpath("schemas"),
        suffix="xlsx",
    )

    for key in list(registry):
        assert (schema := registry.get(key))
        assert isinstance(schema, schema.SchemaModel)
    