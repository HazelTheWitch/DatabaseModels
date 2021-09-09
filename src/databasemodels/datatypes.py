import datetime
import json
import warnings
from abc import ABC
from typing import TYPE_CHECKING, Any, Union, Tuple, Optional, Callable, cast

from iso8601 import parse_date
from psycopg import sql

from .columns import ColumnType, Column
from .exceptions import PrimaryKeyError, NullValueError, EnumValueError, ArrayLengthWarning
from .helper import acceptNone, splitNestedString
from .protocols import DatabaseModel

if TYPE_CHECKING:
    from psycopg import connection


__all__ = [
    'ForeignKey',
    'Composite',
    'PrimaryKey',
    'Unique',
    'NotNull',
    'Array',

    'EnumType',
    'INTEGER',
    'SERIAL',
    'REAL',
    'TEXT',
    'TIMESTAMP',
    'TIMESTAMP_WITH_TIMEZONE',
    'DATE',
    'TIME',
    # 'INTERVAL',

    'JSON',
    'JSONB',

    'BOOL',
    'VARCHAR',
    'CHAR',
    'NUMERIC',

    'datatypeModifiers',
    'constantDatatypes',
    'customizableDatatypes',

    'AUTO_FILLED',
    'NO_DEFAULT',
]

TABLE_OR_TABLE_COLUMN = Union['DatabaseModel', Tuple['DatabaseModel', str]]


class ForeignKey(ColumnType):
    """
    Defines a column to be a foreign key to a different model.
    """

    def __init__(self, model: 'DatabaseModel', schema: str, table: str, column: 'Column') -> None:
        self.model = model

        self.schema = schema
        self.table = table
        self.column = column
        self._rawType = column.rawType

    def __class_getitem__(cls, key: TABLE_OR_TABLE_COLUMN) -> 'ForeignKey':
        # I really want match statements
        if isinstance(key, DatabaseModel):
            if key.__primary_key__ is None:
                raise PrimaryKeyError(f'{key} contains no primary key')
            return cls(key, key.__schema_name__, key.__table_name__, key.__primary_key__)
        return cls(key[0], key[0].__schema_name__, key[0].__table_name__, key[0].getColumn(key[1]))

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

    def convertDataFromString(self, conn: 'connection.Connection[Any]', string: Optional[str]) -> Any:
        if self.model.__primary_key__ is None:
            raise PrimaryKeyError(f'{self.model} contains no primary key')

        return self.model.instantiateFromPrimaryKey(conn, string)

    def convertInsertableFromData(self, conn: 'connection.Connection[Any]', data: Any) -> Any:
        if self.model.__primary_key__ is None:
            raise PrimaryKeyError(f'{self.model} contains no primary key')

        # Data will be of type self.model so we can get the primary key of data and return it
        data.insertOrUpdate(conn)
        
        return getattr(data, self.model.__primary_key__.name)

    def __str__(self) -> str:
        return f'{self.rawType} REFERENCES "{self.schema}"."{self.table}" ({self.column.name})'


class Composite(ColumnType):
    """
    Creates a composite postgres type.
    """

    def __init__(self, name: str, fields: Tuple[Tuple[str, 'ColumnType'], ...]) -> None:
        self.name = name
        self.fields = fields

    def __class_getitem__(cls, definition: Tuple[str, Tuple[Tuple[str, 'ColumnType'], ...]]) -> 'Composite':
        name, fields = definition
        return cls(name, fields)

    @property
    def typeStatement(self) -> 'sql.Composable':
        return sql.SQL(self.name)

    @property
    def rawType(self) -> str:
        return self.name

    def initializeType(self, conn: 'connection.Connection[Any]') -> None:
        conn.execute(
            sql.SQL("""
            DO $$ BEGIN
                CREATE TYPE {} AS ({});
            EXCEPTION
                WHEN duplicate_object THEN null;
            END $$;
            """).format(
                sql.Identifier(self.name),
                sql.SQL(', ').join([
                    sql.SQL('{} {}').format(
                        sql.Identifier(name),
                        column.typeStatement
                    ) for name, column in self.fields
                ])
            )
        )

    def convertDataFromString(self, conn: 'connection.Connection[Any]', string: Optional[str]) -> Any:
        columns = (c for _, c in self.fields)

        return tuple(c.convertDataFromString(conn, i) for c, i in zip(columns, splitNestedString(string)))

    def convertInsertableFromData(self, conn: 'connection.Connection[Any]', data: Any) -> Any:
        return data


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

    @property
    def primary(self) -> bool:
        return self.type.primary

    def convertDataFromString(self, conn: 'connection.Connection[Any]', string: Optional[str]) -> Any:
        return self.type.convertDataFromString(conn, string)

    def convertInsertableFromData(self, conn: 'connection.Connection[Any]', data: Any) -> Any:
        return self.type.convertInsertableFromData(conn, data)

    def __str__(self) -> str:
        return str(self.type)


class Array(ModifiedColumnType):
    """
    Turns the given collumn into an array, must be used first in any chain of modified types.
    """

    def __init__(self, type: 'ColumnType', length: Optional[int] = None) -> None:
        super().__init__(type)
        self.length = length

    def __class_getitem__(cls, items: Union['ColumnType', Tuple['ColumnType', int]]) -> 'ModifiedColumnType':
        # I really want match statements
        if type(items) == tuple:
            return cls(*items)

        return cls(cast('ColumnType', items))

    @property
    def typeStatement(self) -> 'sql.Composable':
        if self.length is not None:
            return sql.SQL('{}[' + str(self.length) + ']').format(self.type.typeStatement)
        else:
            return sql.SQL('{}[]').format(self.type.typeStatement)

    def convertDataFromString(self, conn: 'connection.Connection[Any]', string: Optional[str]) -> Any:
        if string is None:
            return None

        items = splitNestedString(string)
        
        if self.length is not None and len(items) != self.length:
            warnings.warn(f'Expected {self.length} items, got {len(items)} ({string})', ArrayLengthWarning)
        
        return list(self.type.convertDataFromString(conn, None if item == 'NULL' else item) for item in items)

    def convertInsertableFromData(self, conn: 'connection.Connection[Any]', data: Any) -> Any:
        if data is None:
            return None
        return list(self.type.convertInsertableFromData(conn, d) for d in data)

    def __str__(self) -> str:
        if self.length is not None:
            return str(self.type) + f'[{self.length}]'
        else:
            return str(self.type) + '[]'


class NotNull(ModifiedColumnType):
    """
    Requires a column can not be null. Should be applied to most columns.
    """

    @property
    def typeStatement(self) -> 'sql.Composable':
        return sql.SQL('{} NOT NULL').format(self.type.typeStatement)

    def convertInsertableFromData(self, conn: 'connection.Connection[Any]', data: Any) -> Any:
        if data is None:
            raise NullValueError('Attempted to fill NOT NULL field with null')

        return super().convertInsertableFromData(conn, data)

    def __str__(self) -> str:
        return str(self.type) + ' NOT NULL'


class PrimaryKey(ModifiedColumnType):
    """
    Defines this column as the primary key for a table. There can only be one defined for each table.
    """

    @property
    def typeStatement(self) -> 'sql.Composable':
        return sql.SQL('{} PRIMARY KEY').format(self.type.typeStatement)

    @property
    def primary(self) -> bool:
        return True

    def __str__(self) -> str:
        return str(self.type) + ' PRIMARY KEY'


class Unique(ModifiedColumnType):
    """
    Requires that each value entered into the database is unique for this field.
    """

    @property
    def typeStatement(self) -> 'sql.Composable':
        return sql.SQL('{} UNIQUE').format(self.type.typeStatement)

    def __str__(self) -> str:
        return str(self.type) + ' UNIQUE'


class LiteralType(ColumnType):
    """
    A basic type. The type name and raw name are the same and there are customizable converter functions.
    """

    def __init__(self, literal: str, converter: Callable[[str], Any], inverse: Callable[[Any], Any]) -> None:
        self.type = literal
        self.converter = acceptNone(converter)
        self.inverse = acceptNone(inverse)

    @property
    def typeStatement(self) -> 'sql.Composable':
        return sql.SQL(self.type)

    def initializeType(self, conn: 'connection.Connection[Any]') -> None:
        pass

    @property
    def rawType(self) -> str:
        return self.type

    def convertDataFromString(self, conn: 'connection.Connection[Any]', string: Optional[str]) -> Any:
        return self.converter(string)

    def convertInsertableFromData(self, conn: 'connection.Connection[Any]', data: Any) -> Any:
        return self.inverse(data)

    def __str__(self) -> str:
        return self.type


class EnumType(ColumnType):
    """
    A constructed type that can only be one of a few values.
    """

    def __init__(self, type: str, enums: Tuple[str, ...]) -> None:
        self.type = type
        self.enums = enums

    def __class_getitem__(cls, args: Tuple[str, Tuple[str, ...]]) -> 'EnumType':
        return cls(*args)

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

    def convertDataFromString(self, conn: 'connection.Connection[Any]', string: Optional[str]) -> Any:
        return string

    def convertInsertableFromData(self, conn: 'connection.Connection[Any]', data: Any) -> Any:
        if data is None:
            return None

        strData = str(data)

        if strData is not None and strData not in self.enums:
            raise EnumValueError(f'Attempted to insert {strData} into enum {self.type} which only accepts {self.enums}')

        return strData

    def __str__(self) -> str:
        return self.type


class PseudoType(LiteralType):
    """
    A type with which its name is different than its raw name.
    """

    def __init__(self, name: str, rawName: str, converter: Callable[[str], Any], inverse: Callable[[Any], Any]) -> None:
        super().__init__(name, converter, inverse)

        self.rawName = rawName

    @property
    def rawType(self) -> str:
        return self.rawName


INTEGER = LiteralType('INTEGER', int, int)
SERIAL = PseudoType('SERIAL', 'INTEGER', int, int)
REAL = LiteralType('DOUBLE PRECISION', float, float)

TEXT = LiteralType('TEXT', str, str)


TIMESTAMP = LiteralType('TIMESTAMP', parse_date, lambda t: t.isoformat())
TIMESTAMP_WITH_TIMEZONE = LiteralType('TIMESTAMP WITH TIME ZONE', parse_date, lambda t: t.isoformat())
DATE = LiteralType('DATE', datetime.date.fromisoformat, lambda t: t.isoformat())
TIME = LiteralType('TIME', datetime.time.fromisoformat, lambda t: t.isoformat())
# INTERVAL = LiteralType('INTERVAL', str, str)

JSON = LiteralType('JSON', json.loads, json.dumps)
JSONB = LiteralType('JSONB', json.loads, json.dumps)

BOOL = LiteralType('BOOLEAN', lambda s: s == 't', bool)


def VARCHAR(n: int) -> 'LiteralType':
    assert n > 0

    return LiteralType(f'VARCHAR({n})', str, str)


def CHAR(n: int) -> 'LiteralType':
    assert n > 0

    return LiteralType(f'CHAR({n})', str, str)


def NUMERIC(precision: int, scale: int) -> 'LiteralType':
    assert precision > 0
    assert scale >= 0

    return LiteralType(f'NUMERIC({precision}, {scale})', float, float)


datatypeModifiers = [
    ForeignKey,
    Composite,
    Array,
    NotNull,
    PrimaryKey,
    Unique
]

constantDatatypes = [
    INTEGER,
    SERIAL,
    REAL,
    TEXT,
    TIMESTAMP,
    TIMESTAMP_WITH_TIMEZONE,
    DATE,
    TIME,
    JSON,
    JSONB,
    BOOL
]

customizableDatatypes = [
    VARCHAR,
    CHAR,
    NUMERIC
]


class SentinelValue:
    def __init__(self, name: str) -> None:
        self.name = name

    def __repr__(self) -> str:
        return f'SentinelValue({self.name})'

    def __str__(self) -> str:
        return self.name


AUTO_FILLED = SentinelValue('AUTO_FILLED')
NO_DEFAULT = SentinelValue('NO_DEFAULT')
