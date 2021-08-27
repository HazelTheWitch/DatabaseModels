import re

from dataclasses import Field

from psycopg import sql
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, runtime_checkable, Protocol, Dict, List, Union, Tuple, Optional, OrderedDict

if TYPE_CHECKING:
    from psycopg import connection


__all__ = [
    'DatabaseModel',
    'Dataclass',
    'Column',
    'ForeignKey',
    'PrimaryKey',
    'Unique',
    'ColumnType',
    'NoInsert',
    'NotNull',
    'EnumType',
    'INTEGER',
    'SERIAL',
    'REAL',
    'TEXT',
    'TIMESTAMP',
    'TIMESTAMP_WITH_TIMEZONE',
    'DATE',
    'TIME',
    'TIME_WITH_TIMEZONE',
    'INTERVAL',
    'BOOL',
    'VARCHAR',
    'CHAR',
    'NUMERIC',
]


@runtime_checkable
class Dataclass(Protocol):
    __dataclass_fields__: Dict


@runtime_checkable
class DatabaseModel(Dataclass, Protocol):
    __column_definitions__: OrderedDict[str, 'Column']
    __primary_key__: Optional['Column']

    __schema__: str
    __table_name__: str

    @staticmethod
    def createTable(conn: 'connection.Connection[Any]') -> None:
        ...

    @staticmethod
    def instatiate(conn: 'connection.Connection[Any]', query: 'sql.ABC') -> List['DatabaseModel']:
        ...

    def insert(self, conn: 'connection.Connection[Any]') -> None:
        ...

    @classmethod
    def getColumn(cls, name: str) -> 'Column':
        ...

    @property
    def primaryKey(self) -> 'Column':
        ...

    @property
    def schema(self) -> str:
        ...

    @property
    def table(self) -> str:
        ...


COLUMN_NAME = re.compile(r'[a-zA-Z_][a-zA-Z0-9_]{0,58}')


class Column:
    def __init__(self, name: str, type: 'ColumnType') -> None:
        self.name = name
        self.type = type

    def initialize(self, conn: 'connection.Connection[Any]'):
        self.type.initializeType(conn)

    @property
    def columnDefinition(self) -> 'sql.Composable':
        return sql.SQL('{} {}').format(sql.Identifier(self.name), self.type.typeStatement)

    @classmethod
    def fromField(cls, field: 'Field') -> 'Column':
        assert COLUMN_NAME.match(field.name) is not None, f'{field.name} is not a valid column name'
        assert field.name != 'conn', 'Column name must not be "conn"'

        assert isinstance(field.type, ColumnType), 'Fields must be annotated with a type deriving ColumnType'

        return cls(field.name, field.type)

    @property
    def rawType(self) -> str:
        return self.type.rawType

    def __str__(self) -> str:
        return f'"{self.name}" {self.type}'


class ColumnType(ABC):
    @property
    @abstractmethod
    def typeStatement(self) -> 'sql.Composable':
        ...

    @property
    def includeInsert(self) -> bool:
        return True

    @property
    def primary(self) -> bool:
        return False

    @property
    @abstractmethod
    def rawType(self) -> str:
        ...

    @abstractmethod
    def initializeType(self, conn: 'connection.Connection[Any]') -> None:
        ...


TABLE_OR_TABLE_COLUMN = Union['DatabaseModel', Tuple['DatabaseModel', str]]


class ForeignKey(ColumnType):
    def __init__(self, schema: str, table: str, column: 'Column') -> None:
        self.schema = schema
        self.table = table
        self.column = column
        self._rawType = column.rawType

    def __class_getitem__(cls, key: TABLE_OR_TABLE_COLUMN) -> 'ForeignKey':
        if isinstance(key, DatabaseModel):
            if key.__primary_key__ is None:
                raise ValueError(f'{key} contains no primary key')
            return cls(key.__schema__, key.__table_name__, key.__primary_key__)
        return cls(key[0].__schema__, key[0].__table_name__, key[0].getColumn(key[1]))

    def initializeType(self, conn: 'connection.Connection[Any]') -> None:
        pass

    @property
    def rawType(self) -> str:
        return self._rawType

    @property
    def typeStatement(self) -> 'sql.Composable':
        return sql.SQL('{} REFERENCES {}.{} ({})').format(
            sql.SQL(self._rawType),
            sql.Identifier(self.schema),
            sql.Identifier(self.table),
            sql.SQL(self.column.name)
        )

    def __str__(self) -> str:
        return f'{self.rawType} REFERENCES "{self.schema}"."{self.table}" ({self.column.name})'


class ModifiedColumnType(ColumnType, ABC):
    def __init__(self, type: 'ColumnType') -> None:
        self.type = type

    def __class_getitem__(cls, type: 'ColumnType') -> 'ModifiedColumnType':
        return cls(type)

    @property
    def typeStatement(self) -> 'sql.Composable':
        return self.type.typeStatement

    def initializeType(self, conn: 'connection.Connection[Any]') -> None:
        self.type.initializeType(conn)

    @property
    def rawType(self) -> str:
        return self.type.rawType

    def __str__(self) -> str:
        return str(self.type)


class NotNull(ModifiedColumnType):
    @property
    def typeStatement(self) -> 'sql.Composable':
        return sql.SQL('{} NOT NULL').format(self.type.typeStatement)

    def __str__(self) -> str:
        return str(self.type) + ' NOT NULL'


class NoInsert(ModifiedColumnType):
    @property
    def includeInsert(self) -> bool:
        return False

    def __str__(self) -> str:
        return '(Not inserted) ' + str(self.type)


class PrimaryKey(ModifiedColumnType):
    @property
    def typeStatement(self) -> 'sql.Composable':
        return sql.SQL('{} PRIMARY KEY').format(self.type.typeStatement)

    @property
    def primary(self) -> bool:
        return True

    def __str__(self) -> str:
        return str(self.type) + ' PRIMARY KEY'


class Unique(ModifiedColumnType):
    @property
    def typeStatement(self) -> 'sql.Composable':
        return sql.SQL('{} UNIQUE').format(self.type.typeStatement)

    def __str__(self) -> str:
        return str(self.type) + ' UNIQUE'


class LiteralType(ColumnType):
    def __init__(self, literal: str) -> None:
        self.type = literal

    @property
    def typeStatement(self) -> 'sql.Composable':
        return sql.SQL(self.type)

    def initializeType(self, conn: 'connection.Connection[Any]') -> None:
        pass

    @property
    def rawType(self) -> str:
        return self.type

    def __str__(self) -> str:
        return self.type


class EnumType(ColumnType):
    def __init__(self, type: str, *enums: str) -> None:
        self.type = type
        self.enums = enums

    @property
    def typeStatement(self) -> 'sql.Composable':
        return sql.SQL(self.type)

    def initializeType(self, conn: 'connection.Connection[Any]') -> None:
        with conn.cursor() as cur:
            statement = sql.SQL('''
            DO $$ BEGIN
                CREATE TYPE {} AS ENUM ({});
            EXCEPTION
                WHEN duplicate_object THEN null;
            END $$;
            ''').format(
                sql.Identifier(self.type),
                sql.SQL(', ').join(list(map(sql.Literal, self.enums)))
            )
            cur.execute(statement)

    @property
    def rawType(self) -> str:
        return self.type

    def __str__(self) -> str:
        return self.type


class Serial(LiteralType):
    def __init__(self) -> None:
        super().__init__('SERIAL')

    @property
    def rawType(self) -> str:
        return 'INTEGER'


INTEGER = LiteralType('INTEGER')
SERIAL = Serial()
REAL = LiteralType('DOUBLE PRECISION')

TEXT = LiteralType('TEXT')

TIMESTAMP = LiteralType('TIMESTAMP')
TIMESTAMP_WITH_TIMEZONE = LiteralType('TIMESTAMP WITH TIME ZONE')
DATE = LiteralType('DATE')
TIME = LiteralType('TIME')
TIME_WITH_TIMEZONE = LiteralType('TIME WITH TIME ZONE')
INTERVAL = LiteralType('INTERVAL')

BOOL = LiteralType('BOOLEAN')


def VARCHAR(n: int) -> 'LiteralType':
    assert n > 0

    return LiteralType(f'VARCHAR({n})')


def CHAR(n: int) -> 'LiteralType':
    assert n > 0

    return LiteralType(f'CHAR({n})')


def NUMERIC(precision: int, scale: int) -> 'LiteralType':
    assert precision > 0
    assert scale >= 0

    return LiteralType(f'NUMERIC({precision}, {scale})')
