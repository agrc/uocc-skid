"""UOCC Skid package."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("uocc-skid")
except PackageNotFoundError:
    __version__ = "0.0.0-dev"
