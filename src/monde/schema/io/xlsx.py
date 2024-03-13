import json
import warnings
from typing import Any, Dict, List

import pandas as pd

from monde.schema import spec, utils

warnings.filterwarnings("ignore")


def read_metadata(filepath: str) -> Dict[str, Any]:
    # Read the metadata sheet from the ``filepath``
    # fmt:off
    metadata = (
        pd.read_excel(filepath, sheet_name="metadata", index_col=0, header=None)
        .T.to_dict(orient="records")[0]
    )
    # fmt:on
    
    # Pop the required props
    name = metadata.pop("name")
    namespace = metadata.pop("namespace")
    
    # Everything else is a schemaless ``metadata`` prop.
    return {
        "name": name,
        "namespace": namespace,
        "metadata": metadata,
    }


def read_constraints(filepath: str) -> List[Dict[str, Any]]:
    # Read the constraints sheet
    # fmt:off
    return (
        pd.read_excel(
            filepath, sheet_name="constraints", header=0,
            converters={"kwargs": json.loads},
        )
        .to_dict(orient="records")
    )
    # fmt:on


FieldDtypes: Dict[str, str] = {
    "name": "string",
    "title": "string",
    "doc": "string",
    "aliases": "string",
    "default": "string",
    "primary_key": "bool",
    "required": "bool",
    "nullable": "bool",
    "protected": "bool",
    "exclude": "bool",
    "dtype": "string",
    "symbols": "string",
    "start": "Int16",
    "end": "Int16",
    "length": "Int16",
    "datefmt": "string",
    "scale": "Int16",
    "precision": "Int16",
}


def read_fields(filepath: str) -> List[Dict[str, Any]]:
    # Read the field definitions from the `fields` sheet
    # fmt:off
    fields = (
        pd.read_excel(
            filepath, sheet_name="fields", header=0, dtype=FieldDtypes,
            converters={
                "aliases": lambda x: tuple(filter(bool, (x or "").split(","))),
                "symbols": lambda x: tuple(filter(bool, (x or "").split(","))),
            }
        )
        .set_index("name", drop=False)
        .to_dict(orient="records")
    )
    
    return list(map(utils.drop_nulls, fields))
    # fmt:on


def read(filepath: str) -> spec.SchemaModel:
    # Build a schema dictionary from the Excel sheets
    schema = {
        **read_metadata(filepath),
        "fields": read_fields(filepath),
        "constraints": read_constraints(filepath),
    }
    return spec.Schema(**schema)
