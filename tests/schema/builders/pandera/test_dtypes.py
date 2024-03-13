import pandas as pd
import pandera as pa

from monde.schema.builders.pandera import SSN, Currency, LiteralBool, ZipCode


def test_Currency():
    data = pd.DataFrame({"money": ["-$12.50"]})
    Schema = pa.DataFrameSchema(
        columns={"money": pa.Column(Currency)},
        coerce=True,
    )

    valid = Schema.validate(data)
    assert valid.iloc[0, 0] == -12.50


def test_LiteralBool():
    data = pd.DataFrame(
        {
            "truthy": [True, "Y", "yes", "YES", "YeS", "T", "True"],
            "falsey": [False, "N", "no", "NO", "nO", "F", "False"],
            "unknown": [pd.NA, "(blank)", "U", "None", "NULL", "null", None],
        }
    )

    Schema = pa.DataFrameSchema(
        columns={
            "truthy": pa.Column(LiteralBool, nullable=True),
            "falsey": pa.Column(LiteralBool, nullable=True),
            "unknown": pa.Column(LiteralBool, nullable=True),
        },
        coerce=True,
    )

    valid = Schema.validate(data, lazy=True)

    assert valid.truthy.all()
    assert not valid.falsey.all()
    assert valid.unknown.apply(pd.isnull).all()


def test_SSN():
    data = pd.DataFrame({"member_ssn": ["000000012", 12, "123456789"]})

    Schema = pa.DataFrameSchema(
        columns={"member_ssn": pa.Column(SSN)},
        coerce=True,
    )

    valid = Schema.validate(data, lazy=True)

    assert (
        valid.member_ssn == pd.Series(["000000012", "000000012", "123456789"])
    ).all()


def test_ZipCode():
    data = pd.DataFrame({"member_zipcode": ["02128", 0, "12345"]})

    Schema = pa.DataFrameSchema(
        columns={"member_zipcode": pa.Column(ZipCode)},
        coerce=True,
    )

    valid = Schema.validate(data, lazy=True)

    assert (valid.member_zipcode == pd.Series(["02128", "00000", "12345"])).all()
