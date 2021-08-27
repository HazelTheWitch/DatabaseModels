import pickle

from typing import Union, TYPE_CHECKING, TypedDict, Optional, Callable
from pathlib import Path
from psycopg import connect

from functools import wraps
from os import PathLike

if TYPE_CHECKING:
    from psycopg import connection


__all__ = [
    'Login',
    'connectWithLogin',
    'createLogin',
    'loadLogin',
    'createOrLoadLogin',
    'createConnection',
    'loadConnection',
    'createOrLoadConnection'
]


PATH_LIKE = Union[str, bytes, PathLike, 'Path']


class Login(TypedDict):
    dbname: str
    user: str
    password: str
    host: str
    port: int


def prompt(prompt: str, default: Optional[str] = None) -> str:
    if default is not None:
        value = input(f'{prompt} [{default}]: ')

        if value:
            return value
        return default
    else:
        return input(f'{prompt}: ')


def connectWithLogin(login: Login) -> 'connection.Connection':
    return connect(**login)


def connectUsingLoginFunc(func: Callable[[PATH_LIKE], Login]) -> Callable[[PATH_LIKE], 'connection.Connection']:
    @wraps(func)
    def wrapper(filePath: PATH_LIKE) -> 'connection.Connection':
        return connectWithLogin(func(filePath))

    return wrapper


def createLogin(filePath: PATH_LIKE) -> Login:
    path = Path(filePath)

    login = dict(
        dbname=prompt('Database Name'),
        user=prompt('User Name'),
        password=prompt('Password'),
        host=prompt('Hostname'),
        port=int(prompt('Port', '5432'))
    )

    with path.open('wb') as loginFile:
        pickle.dump(login, loginFile)

    return login


def loadLogin(filePath: PATH_LIKE) -> Login:
    path = Path(filePath)

    with path.open('rb') as loginFile:
        return pickle.load(loginFile)


def createOrLoadLogin(filePath: PATH_LIKE) -> Login:
    path = Path(filePath)

    if path.exists():
        return loadLogin(filePath)
    else:
        return createLogin(filePath)


createConnection = connectUsingLoginFunc(createLogin)
loadConnection = connectUsingLoginFunc(loadLogin)
createOrLoadConnection = connectUsingLoginFunc(createOrLoadLogin)
