import pathlib

import pytest


@pytest.fixture
def here():
    return pathlib.Path(__file__).parent


@pytest.fixture
def cwd(here):
    return here.parent


@pytest.fixture
def data(here):
    return here.joinpath("data")
