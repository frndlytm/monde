import os
import pathlib
import re

from invoke import Context, task

# Project Constants
PROJECT_HOME = pathlib.Path(__file__).parent
PROJECT_SLUG = PROJECT_HOME.stem
PROJECT_VERSION = PROJECT_HOME.joinpath("VERSION").read_text().strip()
PIP_INDEX_URL = os.getenv("PIP_INDEX_URL")


def single_line(s: str) -> str:
    return re.subn(r"\s+", " ", s)[0].strip()


def is_valid_version_tag(t: str) -> bool:
    # The core semantic version component is 3 dot-separated numbers.
    re_version_core = r"(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)"

    # A pre-release is like alpha, alpha.beta, etc.
    re_pre_release = r"(?P<pre_release>\w[\w\d\.]+[\w\d])"

    # Combine the version with the pre-release
    re_tag = r"^v" + re_version_core + "(-" + re_pre_release + r")?$"

    # Test for a match, or latest.
    return re.match(re_tag, t) is not None or t == "latest"


@task
def clean(c: Context):
    """Remove (.venv/, node_modules/, build/ and dist/)"""
    for directory in (".venv", "build", "dist"):
        c.run(f"rm -rf ./{directory}")


@task(pre=[clean])
def init(c: Context, editable: bool = False):
    """Build a .venv and install prettier with npm"""
    # Install the requirements into a new .venv
    # fmt:off
    with c.cd(PROJECT_HOME):
        # Build a virtual environment
        c.run("python -m venv .venv")

        # Using the virtual environment, install python requirements
        with c.prefix("source .venv/bin/activate"):
            c.run("pip install --upgrade pip setuptools wheel invoke")
            c.run("pip install -r requirements-test.txt -r requirements.txt")

            if editable:
                c.run("pip install -e .")

            else:
                c.run("python -m build")
                c.run(single_line("""
                    find ./dist -name "*.whl" -exec
                        pip install --force-reinstall {} +
                """))
    # fmt:on


@task
def version(c: Context, tag: bool = False):
    """Print the version"""
    print(f"v{PROJECT_VERSION}" if tag else PROJECT_VERSION)


@task
def lint(c: Context):
    """Lint `./src` using (black, isort, flake8, mypy, bandit)"""
    with c.cd(PROJECT_HOME):
        with c.prefix("source .venv/bin/activate"):
            c.run("isort . --check-only")
            c.run("black . --check --diff")
            c.run("flake8 .")
            c.run("mypy .")
            c.run("bandit .")


@task
def format(c: Context):
    """Format `./src` using (black, isort)"""
    with c.cd(PROJECT_HOME):
        with c.prefix("source .venv/bin/activate"):
            c.run("isort .")
            c.run("black .")


@task(pre=[clean])
def build(c: Context):
    """Build using setuptools"""
    with c.cd(PROJECT_HOME):
        with c.prefix("source .venv/bin/activate"):
            c.run("python -m build")


@task
def release(c: Context, replace: bool = False):
    """Release dist/ to pypi."""
    # CONTINUE
    if is_valid_version_tag(PROJECT_VERSION):
        raise ValueError(f"'{PROJECT_VERSION}' is not a valid version tag.")

    with c.cd(PROJECT_HOME):
        with c.prefix("source .venv/bin/activate"):
            # TODO: Implement this to target pypi
            ...


@task
def test(c: Context):
    """Run pytests."""
    with c.cd(PROJECT_HOME):
        with c.prefix("source .venv/bin/activate"):
            c.run("pytest ./tests -vvv --capture=sys")
