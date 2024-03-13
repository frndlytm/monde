from datetime import date

import pandas as pd
import pandera as pa
import pytest

from monde.transform.validator import Validator, noop


@pytest.fixture
def calendar() -> pd.DataFrame:
    # Construct a date index
    idx = pd.date_range(date(2012, 1, 1), date(2050, 12, 31), name="calendar_date")

    calendar: pd.DataFrame = (
        idx.to_frame()
        # Extract the various datetime components from the date into a table
        .assign(
            calendar_year=lambda x: x.calendar_date.dt.year,
            calendar_quarter=lambda x: x.calendar_date.dt.quarter,
            calendar_month=lambda x: x.calendar_date.dt.month,
            calendar_day=lambda x: x.calendar_date.dt.day,
            calendar_day_name=lambda x: x.calendar_date.dt.day_name(),
            calendar_day_of_week=lambda x: x.calendar_date.dt.weekday,
            calendar_day_of_year=lambda x: x.calendar_date.dt.day_of_year,
            calendar_is_month_start=lambda x: x.calendar_date.dt.is_month_start,
            calendar_is_month_end=lambda x: x.calendar_date.dt.is_month_end,
        )
    )

    return calendar


@pytest.fixture
def schema() -> pa.DataFrameSchema:
    return pa.DataFrameSchema(
        columns={
            "calendar_date": pa.Column("date"),
            "calendar_year": pa.Column("uint16"),
            "calendar_quarter": pa.Column("uint8"),
            "calendar_month": pa.Column("uint8"),
            "calendar_day": pa.Column("uint8"),
            "calendar_day_name": pa.Column("string"),
            "calendar_day_of_week": pa.Column("uint8"),
            "calendar_day_of_year": pa.Column("uint16"),
            "calendar_is_month_start": pa.Column("boolean"),
            "calendar_is_month_end": pa.Column("boolean"),
        },
        coerce=True,
        strict=True,
    )


def test_validate_calendar_with_Validator(
    calendar: pd.DataFrame, schema: pa.DataFrameSchema
):
    validator = Validator(schema, error_handler=noop)
    valids = validator.fit_transform(calendar)
    checks = (valids == calendar)
    assert checks.to_numpy().all()


def test_drop_invalids_in_calendar_with_Validator(
    calendar: pd.DataFrame, schema: pa.DataFrameSchema
):
    # Update the schema to raise invalid errors on calendar_years
    schema = schema.update_column(
        "calendar_year", checks=[pa.Check.between(2015, 2022)]
    )

    # Use the validator to transform the dataframe
    validator = Validator(schema, error_handler=noop)
    valids = validator.fit_transform(calendar, lazy=True)

    # The validation should filter valids to a smaller set of records
    assert calendar.size > valids.size
