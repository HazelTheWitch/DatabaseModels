import ast
from typing import Callable, Any, Optional, Type, TypeVar, List
from functools import wraps

__all__ = [
    'acceptNone',
    'classproperty',
    'identity',
    'splitArrayString'
]


T = TypeVar('T')


def acceptNone(func: Callable[[Any], Any]) -> Callable[[Optional[Any]], Any]:
    @wraps(func)
    def wrapper(x: Optional[Any]) -> Any:
        if x is None:
            return None

        return func(x)
    return wrapper


def identity(x: T) -> T:
    return x


class classproperty:
    def __init__(self, func: Callable[[Type[Any]], Any]) -> None:
        self.func = func

    def __get__(self, _: Any, owner: Type[Any]) -> Any:
        return self.func(owner)


def splitArrayString(arraystring: str) -> List[str]:
    """Parses a string array from psycopg into a proper array"""
    itemstring = arraystring[1:-1]  # Cut off {}

    escaped = False

    inString = False

    depth = 0

    items = []
    startingIndex = 0

    for i, c in enumerate(itemstring):
        if c == ',' and not inString and depth == 0:
            items.append(itemstring[startingIndex:i])
            startingIndex = i + 1

        if escaped:
            escaped = False
        else:
            if c == '"':
                inString = not inString
            elif c == '\\':
                escaped = True
            elif c == '{':
                depth += 1
            elif c == '}':
                depth -= 1

    items.append(itemstring[startingIndex:])

    for i, item in enumerate(items):
        if item[0] != '"':
            items[i] = f'"{item}"'

    return list(map(ast.literal_eval, items))  # Safely eval strings

