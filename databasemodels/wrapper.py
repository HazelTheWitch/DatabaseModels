import types
from collections import OrderedDict as OD
from dataclasses import fields, MISSING
from typing import Callable, Any, List, Type, Optional, OrderedDict, Dict

from psycopg import connection, sql

__all__ = [
    'model',
]

from .datatypes import Column, DatabaseModel, Dataclass, NO_DEFAULT, AUTO_FILLED


def model(_schema: Optional[str] = None, _table: Optional[str] = None) -> Callable[[Type['Dataclass']], Type['DatabaseModel']]:
    def wrapped(cls: Type['Dataclass']) -> Type['DatabaseModel']:
        if _table is None:
            tableName = cls.__name__.lower()
        else:
            tableName = _table

        if _schema is None:
            schemaName = 'public'
        else:
            schemaName = _schema

        columnDefinitions: OrderedDict[str, 'Column'] = OD()
        _primaryKey: Optional['Column'] = None

        argsNames: List[str] = []

        for field in fields(cls):
            definition = Column.fromField(field)
            columnDefinitions[definition.name] = definition

            if definition.type.primary:
                if _primaryKey is not None:
                    raise TypeError(f'{schemaName}.{tableName} ({cls.__name__}) Has two primary keys defined')
                _primaryKey = definition

            if not (field.default is MISSING or field.default is NO_DEFAULT or field.default is AUTO_FILLED):
                raise TypeError(f'{field.name} does not declare default type of MISSING, NO_DEFAULT, or AUTO_FILLED')

            if field.default is not AUTO_FILLED:
                argsNames.append(field.name)

        argsString = ', '.join(argsNames)
        settersString = '\n'.join(f'    self.{a} = {a}' for a in argsNames)

        funcString = f"def __init__(self, {argsString}):\n{settersString}"

        # mypy doesn't support this yet so have to silence the error
        class WrappedClass(cls):  # type: ignore
            __column_definitions__: OrderedDict[str, 'Column'] = columnDefinitions
            __primary_key__: Optional['Column'] = _primaryKey

            __schema_name__: str = schemaName
            __table_name__: str = tableName

            def _create(self, *args, **kwargs) -> None:
                super().__init__(*args, **kwargs)

            def __str__(self) -> str:
                dictlike = ', '.join(f'{a}={getattr(self, a)}' for a in self.__column_definitions__.keys())
                return f'{self.__schema_name__}.{self.__table_name__}({dictlike})'

            @classmethod
            def getColumn(cls, name: str) -> 'Column':
                return cls.__column_definitions__[name]

            @property
            def primaryKey(self) -> Optional['Column']:
                return WrappedClass.__primary_key__

            @property
            def schema(self) -> str:
                return WrappedClass.__schema_name__

            @property
            def table(self) -> str:
                return WrappedClass.__table_name__

            @staticmethod
            def createTable(conn: 'connection.Connection[Any]') -> None:
                for defini in WrappedClass.__column_definitions__.values():
                    defini.initialize(conn)

                createSchema = sql.SQL(
                    'CREATE SCHEMA IF NOT EXISTS {};'
                ).format(
                    sql.Identifier(schemaName)
                )

                createTable = sql.SQL(
                    'CREATE TABLE IF NOT EXISTS {}.{} ({});'
                ).format(
                    sql.Identifier(schemaName),
                    sql.Identifier(tableName),
                    sql.SQL(', ').join(
                        [d.columnDefinition for d in WrappedClass.__column_definitions__.values()]
                    )
                )

                with conn.cursor() as cur:
                    cur.execute(createSchema)
                    cur.execute(createTable)

            @staticmethod
            def instatiate(conn: 'connection.Connection[Any]', query: 'sql.ABC') -> List['WrappedClass']:
                ...

            def insert(self, conn: 'connection.Connection[Any]') -> None:
                ...

        miniLocals: Dict[str, Callable] = {}

        # Builds init method
        exec(funcString, {}, miniLocals)

        # Ignored because this must be done to set init method properly
        WrappedClass.__init__ = miniLocals['__init__']  # type: ignore

        # Transfer wrapped class data over
        WrappedClass.__module__ = cls.__module__
        WrappedClass.__name__ = cls.__name__
        WrappedClass.__qualname__ = cls.__qualname__
        WrappedClass.__annotations__ = cls.__annotations__
        WrappedClass.__doc__ = cls.__doc__

        return WrappedClass
    return wrapped
