import pytest

from hyperion import exceptions


def test_dynamic_exception_is_subclass_and_cached() -> None:
    first = exceptions.CustomError
    second = exceptions.CustomError
    another = exceptions.AnotherError

    assert issubclass(first, exceptions.HyperionError)
    assert first is second
    assert issubclass(another, exceptions.HyperionError)
    assert another is not first
    with pytest.raises(first):
        raise first("boom")


def test_from_import_creates_exception() -> None:
    from hyperion.exceptions import SimulationFailure

    assert issubclass(SimulationFailure, exceptions.HyperionError)
    assert SimulationFailure is exceptions.SimulationFailure
