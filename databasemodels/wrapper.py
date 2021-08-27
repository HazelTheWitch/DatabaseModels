from collections import OrderedDict as OD
from dataclasses import fields
from typing import Callable, Any, List, Type, Optional, OrderedDict

from psycopg import connection, sql

__all__ = [
    'model',
]

from .datatypes import Column, DatabaseModel, Dataclass


def model(_schema: str, _table: str) -> Callable[[Type['Dataclass']], Type['DatabaseModel']]:
    def wrapped(cls: Type['Dataclass']) -> Type['DatabaseModel']:
        columnDefinitions: OrderedDict[str, 'Column'] = OD()
        _primaryKey: Optional['Column'] = None

        for field in fields(cls):
            definition = Column.fromField(field)
            columnDefinitions[definition.name] = definition

            if definition.type.primary:
                if _primaryKey is not None:
                    raise ValueError(f'{_schema}.{_table} ({cls.__name__}) Has two primary keys defined')
                _primaryKey = definition

        # mypy doesn't support this yet so have to silence the error
        class WrappedClass(cls):  # type: ignore
            __column_definitions__: OrderedDict[str, 'Column'] = columnDefinitions
            __primary_key__: Optional['Column'] = _primaryKey

            __schema__: str = _schema
            __table_name__: str = _table

            def __init__(self, conn: 'connection.Connection[Any]', *args, **kwargs) -> None:
                super().__init__(*args, **kwargs)

                self.conn = conn

            def __str__(self) -> str:
                return f'{self.__schema__}.{self.__table_name__}({super().__repr__()})'

            @classmethod
            def getColumn(cls, name: str) -> 'Column':
                return cls.__column_definitions__[name]

            @property
            def primaryKey(self) -> Optional['Column']:
                return WrappedClass.__primary_key__

            @property
            def schema(self) -> str:
                return WrappedClass.__schema__

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
                    sql.Identifier(_schema)
                )

                createTable = sql.SQL(
                    'CREATE TABLE IF NOT EXISTS {}.{} ({});'
                ).format(
                    sql.Identifier(_schema),
                    sql.Identifier(_table),
                    sql.SQL(', ').join(
                        [d.columnDefinition for d in WrappedClass.__column_definitions__.values()]
                    )
                )

                print(createTable.as_string(conn))

                with conn.cursor() as cur:
                    cur.execute(createSchema)
                    cur.execute(createTable)

            @staticmethod
            def instatiate(conn: 'connection.Connection[Any]', query: 'sql.ABC') -> List['WrappedClass']:
                ...

            def insert(self, conn: 'connection.Connection[Any]') -> None:
                ...

        # Transfer wrapped class data over
        WrappedClass.__module__ = cls.__module__
        WrappedClass.__name__ = cls.__name__
        WrappedClass.__qualname__ = cls.__qualname__
        WrappedClass.__annotations__ = cls.__annotations__
        WrappedClass.__doc__ = cls.__doc__

        return WrappedClass
    return wrapped
