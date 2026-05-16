"""Exception types for Hyperion."""

from __future__ import annotations


class HyperionError(Exception):
    """Base class for Hyperion errors."""


_DYNAMIC_EXCEPTIONS: dict[str, type[HyperionError]] = {}


def _get_or_create_exception(name: str) -> type[HyperionError]:
    existing = _DYNAMIC_EXCEPTIONS.get(name)
    if existing is not None:
        return existing

    new_exception = type(
        name,
        (HyperionError,),
        {"__doc__": f"Dynamically created {name} exception."},
    )
    _DYNAMIC_EXCEPTIONS[name] = new_exception
    return new_exception


def __getattr__(name: str) -> type[HyperionError]:
    if name.startswith("_"):
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    return _get_or_create_exception(name)


__all__ = ["HyperionError"]


def __dir__() -> list[str]:
    public_names = set(__all__)
    public_names.update(_DYNAMIC_EXCEPTIONS.keys())
    return sorted(public_names)
