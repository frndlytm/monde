import io

import pandas as pd
import pandera as pa
import pytest

from monde.transform import EasyPreprocess, RenameAliases, SetConst


@pytest.fixture
def schema() -> pa.DataFrameSchema:
    return pa.DataFrameSchema(
        columns={
            "i": pa.Column(int, title="I"),
            "integers": pa.Column(int, title="Ints"),
            "floats": pa.Column(float, title="Floats"),
            "letters": pa.Column(str, title="Alphas"),
        },
        coerce=True,
    )


@pytest.fixture
def data() -> pd.DataFrame:
    fwf = io.StringIO("", newline="\n")
    fwf.writelines(
        [
            "i\t    Ints\t  Floats\tAlphas\n",
            "0\t0001,500\t0001,500\t  a   \n",
            "1\t0000-350\t00-350.2\t    bc\n",
        ]
    )
    fwf.seek(0)

    return pd.read_csv(fwf, header=0, skipinitialspace=True, sep="\t")


def test_EasyPreprocess(data, schema):
    transform = EasyPreprocess(schema)

    columns = {
        "I": "i",
        "Ints": "integers",
        "Floats": "floats",
        "Alphas": "letters",
    }

    X = data.rename(columns=columns).pipe(transform).pipe(schema.validate)
    strings = X.select_dtypes(include="object")

    for column in strings:
        assert (~X[column].str.startswith(" ")).all()
        assert (~X[column].str.endswith(" ")).all()


def test_RenameAliases(data, schema):
    transform = RenameAliases(schema)

    columns = {
        "I": "i",
        "Ints": "integers",
        "Floats": "floats",
        "Alphas": "letters",
    }

    assert data.pipe(transform).columns.tolist() == list(columns.values())


def test_SetConst(data, schema):
    transform = SetConst(hello="world", indicator=0)
    X = data.pipe(transform)

    assert (X.hello == "world").all()
    assert (X.indicator == 0).all()
