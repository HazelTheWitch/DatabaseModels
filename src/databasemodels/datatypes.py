import ast
import datetime
import json
import re
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import Field
from typing import TYPE_CHECKING, Any, runtime_checkable, Protocol, Dict, Union, Tuple, Optional, OrderedDict, \
    Callable, Generator, Type

from iso8601 import parse_date

from psycopg import sql

from .helper import acceptNone, classproperty, identity

if TYPE_CHECKING:
    from psycopg import connection


__all__ = [
    'DatabaseModel',
    'Dataclass',

    'Column',

    'ColumnType',

    'ForeignKey',
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

    'AUTO_FILLED',
    'NO_DEFAULT',
]


@runtime_checkable
class Dataclass(Protocol):
    __dataclass_fields__: Dict[str, 'Field[Any]']


# Properties gave warnings in PyCharm, this disables checking that inspection
# noinspection PyPropertyDefinition
@runtime_checkable
class DatabaseModel(Dataclass, Protocol):
    __column_definitions__: OrderedDict[str, 'Column']
    __primary_key__: Optional['Column']

    __schema_name__: str
    __table_name__: str

    @classmethod
    def createTable(cls, conn: 'connection.Connection[Any]', *, recreateSchema: bool = False, recreateTable: bool = False) -> None:
        """
        Create a table representing this class.

        :param conn: the connection to use
        :type conn: psycopg.connection.Connection
        :param recreateSchema: if true it will drop the schema before recreating it. This will drop any other tables on the schema too
        :type recreateSchema: bool
        :param recreateTable: if true it will drop the table before recreating it. This will drop any other tables that depend on it
        :type recreateTable: bool
        """
        ...

    @classmethod
    def instatiateAll(cls, conn: 'connection.Connection[Any]', query: Union[str, 'sql.Composable'] = '') -> Tuple['DatabaseModel', ...]:
        """
        Instantiate all models of this type with the given query.

        :param conn: the connection to use
        :type conn: connection.Connection[Any]
        :param query: the additional query to use after the select statement
        :type query: Union[str, sql.Composable]
        :return: a tuple of every model returned from the query
        :rtype: Tuple[DatabaseModel, ...]
        """
        ...

    @classmethod
    def instatiate(cls, conn: 'connection.Connection[Any]', query: Union[str, 'sql.Composable'] = '') -> Generator['DatabaseModel', None, None]:
        """
        Instantiate each models of this type with the given query as a generator.

        :param conn: the connection to use
        :type conn: connection.Connection[Any]
        :param query: the additional query to use after the select statement
        :type query: Union[str, sql.Composable]
        :return: a tuple of every model returned from the query
        :rtype: Tuple[DatabaseModel, ...]
        """
        ...

    def insert(self, conn: 'connection.Connection[Any]', *, doTypeConversion: bool = True) -> None:
        """
        Insert this model into the database.

        :param conn: the connection to use
        :type conn: connection.Connection[Any]
        :param doTypeConversion: if true a conversion function will be called on each field
        :type doTypeConversion: bool
        """
        ...

    def update(self, conn: 'connection.Connection[Any]', *, doTypeConversion: bool = True) -> None:
        """
        Update this model in the database, will replace any model currently in the database with the updated values.
        If there was not a row with the primary key this model has it will raise an error.

        :param conn: the connection to use
        :type conn: connection.Connection[Any]
        :param doTypeConversion: if true a conversion function will be called on each field
        :type doTypeConversion: bool
        """
        ...

    def insertOrUpdate(self, conn: 'connection.Connection[Any]', *, doTypeConversion: bool = True) -> None:
        """
        Intelligently either updates or inserts this model into the database. If there was not a row with the primary
        key this model has it will insert it.

        :param conn: the connection to use
        :type conn: connection.Connection[Any]
        :param doTypeConversion: if true a conversion function will be called on each field
        :type doTypeConversion: bool
        """
        ...

    @classmethod
    def getColumn(cls, name: str) -> 'Column':
        """
        Get a Column from a given name from this model type.

        :param name: the name of the column
        :type name: str
        :return: the column with the given name
        :rtype: Column
        """
        ...

    @classproperty
    def primaryKey(cls: Type['DatabaseModel']) -> Optional['Column']:
        """
        Get the primary key for this model or model type.

        :return: either the primary key column or None
        :rtype: Optional[Column]
        """
        ...

    @property
    def primaryKeyValue(self) -> Optional[Any]:
        """
        Get this model's primary key value or None if there is no primary key.

        :return: the primary key value
        :rtype: Optional[Any]
        """
        ...

    @classproperty
    def schema(cls: Type['DatabaseModel']) -> str:
        """
        Get the schema for this model.

        :return: the schema this model uses
        :rtype: str
        """
        ...

    @classproperty
    def table(cls: Type['DatabaseModel']) -> str:
        """
        Get the table name for this model.

        :return: the table name this model uses
        :rtype: str
        """
        ...


COLUMN_NAME = re.compile(r'[a-zA-Z_][a-zA-Z0-9_]{0,58}')


class Column:
    def __init__(self, name: str, type: 'ColumnType') -> None:
        self.name = name
        self.type = type

    def initialize(self, conn: 'connection.Connection[Any]') -> None:
        self.type.initializeType(conn)

    @property
    def columnDefinition(self) -> 'sql.Composable':
        return sql.SQL('{} {}').format(sql.Identifier(self.name), self.type.typeStatement)

    @classmethod
    def fromField(cls, field: 'Field[Any]') -> 'Column':
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
    """
    Defines the types of data that is allowed in a column of a model.
    """

    @property
    @abstractmethod
    def typeStatement(self) -> 'sql.Composable':
        ...

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

    @abstractmethod
    def convertDataFromString(self, conn: 'connection.Connection[Any]', string: str) -> Any:
        """
        Convert string retrieved from the database to a Python object representation.
        Should be the inverse of convertInsertableFromData.

        :param conn: the connection to use
        :type conn: psycopg.connection.Connection
        :param string: the string to convert
        :type string: str
        :return: the Python object
        :rtype: Any
        """
        ...

    def convertInsertableFromData(self, conn: 'connection.Connection[Any]', data: Any) -> Any:
        """
        Convert a Python object representation to something insertable by psycopg.
        Should be the inverse of convertDataFromString.

        :param conn: the connection to use
        :type conn: psycopg.connection.Connection
        :param data: the object to convert
        :type data: Any
        :return: the insertable object
        :rtype: Any
        """
        return data


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
        if isinstance(key, DatabaseModel):
            if key.__primary_key__ is None:
                raise TypeError(f'{key} contains no primary key')
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

    def convertDataFromString(self, conn: 'connection.Connection[Any]', string: str) -> Any:
        if self.model.__primary_key__ is None:
            raise TypeError(f'{self.model} contains no primary key')

        return self.model.instatiateAll(conn, sql.SQL('WHERE {} = {}').format(
            sql.Identifier(self.model.__primary_key__.name),
            sql.Literal(string)
        ))[0]

    def convertInsertableFromData(self, conn: 'connection.Connection[Any]', data: Any) -> Any:
        if self.model.__primary_key__ is None:
            raise TypeError(f'{self.model} contains no primary key')

        # Data will be of type self.model so we can get the primary key of data and return it
        data.insertOrUpdate(conn)
        
        return getattr(data, self.model.__primary_key__.name)

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

    @property
    def primary(self) -> bool:
        return self.type.primary

    def convertDataFromString(self, conn: 'connection.Connection[Any]', string: str) -> Any:
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
        if type(items) == tuple:
            return cls(*items)
        return cls(items)

    @property
    def typeStatement(self) -> 'sql.Composable':
        if self.length is not None:
            return sql.SQL('{}[' + str(self.length) + ']').format(self.type.typeStatement)
        else:
            return sql.SQL('{}[]').format(self.type.typeStatement)

    def convertDataFromString(self, conn: 'connection.Connection[Any]', string: str) -> Any:
        items = [None if i == 'NULL' else i for i in string[1:-1].split(',')]
        if self.length is not None and len(items) != self.length:
            raise ValueError(f'Expected {self.length} items, got {len(items)} ({string})')
        return list(self.type.convertDataFromString(conn, item) for item in items)

    def convertInsertableFromData(self, conn: 'connection.Connection[Any]', data: Any) -> Any:
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
            raise TypeError('Attempted to fill not null field with null')
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

    def convertDataFromString(self, conn: 'connection.Connection[Any]', string: str) -> Any:
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

    def convertDataFromString(self, conn: 'connection.Connection[Any]', string: str) -> Any:
        return string

    def convertInsertableFromData(self, conn: 'connection.Connection[Any]', data: Any) -> Any:
        strData = str(data)

        if strData is not None and strData not in self.enums:
            raise TypeError(f'Attempted to insert {strData} into enum {self.type} which only accepts {self.enums}')

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


class SentinelValue:
    def __init__(self, name: str) -> None:
        self.name = name

    def __repr__(self) -> str:
        return self.name


AUTO_FILLED = SentinelValue('AUTO_FILLED')
NO_DEFAULT = SentinelValue('NO_DEFAULT')
