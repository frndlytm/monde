import pathlib

import pytest


@pytest.fixture
def here():
    return pathlib.Path(__file__).parent


@pytest.fixture
def cwd(here):
    return here.parent


@pytest.fixture
def assets(here):
    return here.joinpath("assets")
