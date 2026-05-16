"""Exception types for Hyperion."""

from __future__ import annotations

from typing import Dict, Type


class HyperionError(Exception):
    """Base class for Hyperion errors."""


_DYNAMIC_EXCEPTIONS: Dict[str, Type[HyperionError]] = {}


def _get_or_create_exception(name: str) -> Type[HyperionError]:
    existing = _DYNAMIC_EXCEPTIONS.get(name)
    if existing is not None:
        return existing

    new_exception = type(name, (HyperionError,), {})
    _DYNAMIC_EXCEPTIONS[name] = new_exception
    globals()[name] = new_exception
    return new_exception


def __getattr__(name: str) -> Type[HyperionError]:
    if name.startswith("_"):
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    return _get_or_create_exception(name)


__all__ = ["HyperionError"]
