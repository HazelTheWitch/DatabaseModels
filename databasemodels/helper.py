from typing import Callable, Any, Optional
from functools import wraps

__all__ = [
    'acceptNone',
]


def acceptNone(func: Callable[[Any], Any]) -> Callable[[Optional[Any]], Any]:
    @wraps(func)
    def wrapper(x: Optional[Any]) -> Any:
        if x is None:
            return None

        return func(x)
    return wrapper
