from typing import Callable, Any, Optional, Type
from functools import wraps

__all__ = [
    'acceptNone',
    'classproperty'
]


def acceptNone(func: Callable[[Any], Any]) -> Callable[[Optional[Any]], Any]:
    @wraps(func)
    def wrapper(x: Optional[Any]) -> Any:
        if x is None:
            return None

        return func(x)
    return wrapper


class classproperty:
    def __init__(self, func: Callable[[Any], Any]) -> None:
        self.func = func

    def __get__(self, _: Any, owner: Type[Any]) -> Any:
        return self.func(owner)
