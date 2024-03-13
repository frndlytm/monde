from monde.schema import LocalSchemaRegistry, SchemaModel


def test_LocalSchemaRegistry(cwd):
    registry = LocalSchemaRegistry(root=cwd.joinpath("schemas"), suffix="xlsx")

    for key in list(registry):
        assert (schema := registry.get(key))
        assert isinstance(schema, SchemaModel)
    