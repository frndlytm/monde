# Monde

Easy, flat-file transformation tools to help data engineers get to data engineering
well-documented pipelines faster.

## Usage

### `Transform`

``abstract.Transform`` adds some extra safety around implementing ``sklearn``
Estimators, according to the [sklearn Developer Docs][0]

[0]: https://scikit-learn.org/stable/developers/develop.html

`monde.Transfom` implements a default noop `fit` function, so that all we _need_
to implement is

> transform: pd.DataFrame -> pd.DataFrame

```python
# %%
import pandas as pd

from monde.transform import abstract

# %%
class LowercaseColumnNames(abstract.Transform):
    """Simple transform that renames columns to lowercase"""
    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        return X.rename(columns={c: c.lower() for c in X.columns})

# %%
source_uri = "s3://bucket/prefix/key.csv"
transform = LowercaseColumnNames()
X = pd.read_csv(source_uri).pipe(transform.fit_transform)
print(X)
```

### `Pipeline`

`Pipeline` works similarly to `sklearn.pipeline.Pipeline`, except that `sklearn`
doesn't allow their `Pipeline` to end on a pure `Transformer`.

A `monde.Pipeline` takes a sequence of steps (that are valid `Transformers`),
and chains them together in order.

```python
# %%
import pandas as pd
import pandera as pa

from monde import transform

# %%
constants = {"name": "frndlytm"}
source_uri = "s3://bucket/prefix/key.csv"
schema = pa.DataFrameSchema(...)

pipeline = transform.Pipeline(
    ("preprocess", transform.EasyPreprocess(schema)),
    ("rename", transform.RenameAliases(schema)),
    ("constants", transform.SetConst(**constants)),
)

# %%
X = pd.read_csv(source_uri)
X = pipeline.fit_transform(X, lazy=True)
print(X)
```

### Schema Registry

`monde` uses Excel Workbooks to manage schemas because flat-files 

Putting it all together, we can create a custom ``TransformerPipeline``
with a custom ``abstract.Transform`` and a nested ``TransformerPipeline``!

```python
# %%
import json

import pandas as pd
import pandera as pa

from monde import schema

# %% Initialize a schema registry located on the local file-system backend.
registry = schema.LocalSchemaRegistry(root="./schemas")

# %% Read a schema definition
definition = registry.get("example/FinancialSample.xlsx")

# %% Build a pandera.DataFrameSchema
Schema = schema.DataFrameSchemaBuilder.build(definition)
```

### `EasyValidate` Pipeline

The `EasyValidate` pipeline is a standard `monde.Pipeline` that include the
following steps.

* preprocess: `EasyPreprocess(schema)`
* rename: `RenameAliases(schema)`
* optimize: `MemoryOptimizer(schema)`
* validate: `Validator(schema, **kwargs)`
* protect: `HashProtectedAttributes(schema)`

Note: `EasyValidate`, and many other `Transforms` rely on schemas to help decide
which properties of the data to transform.

```python
# %%
import json

import pandas as pd

from monde import transform
from monde.transform.validator import error_report

# %% Establish an EasyValidate pipeline using the schema
pipeline = transform.EasyValidate(schema, error_handler=noop)

# %% Chunked dataframe pipeline
with pd.read_csv("s3://bucket/prefix/key.csv", iterator=True) as chunks:
    for chunk in chunks:
        with utils.chunk_statistics(chunk):
            try:
                X = pipeline.fit_transform(chunk, lazy=True)
                print(X)

            except pa.errors.SchemaErrors as e:
                print(json.dumps(error_report(e), indent=2))

```
