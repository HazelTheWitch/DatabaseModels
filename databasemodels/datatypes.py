import re

from dataclasses import Field

from psycopg import sql
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, runtime_checkable, Protocol, Dict, List, Union, Tuple, Optional, OrderedDict, \
    Callable, cast

from .helper import acceptNone

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

    @staticmethod
    def createTable(conn: 'connection.Connection[Any]', *, recreateSchema: bool = False, recreateTable: bool = False) -> None:
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

    @staticmethod
    def instatiateAll(conn: 'connection.Connection[Any]', query: Union[str, 'sql.Composable'] = '') -> Tuple['WrappedClass', ...]:
        ...

    @staticmethod
    def instatiate(conn: 'connection.Connection[Any]', query: Union[str, 'sql.Composable'] = '') -> List['DatabaseModel']:
        ...

    def insert(self, conn: 'connection.Connection[Any]', *, doTypeConversion: bool = True) -> None:
        ...

    def update(self, conn: 'connection.Connection[Any]', *, doTypeConversion: bool = True) -> None:
        ...

    def insertOrUpdate(self, conn: 'connection.Connection[Any]', *, doTypeConversion: bool = True) -> None:
        ...

    @classmethod
    def getColumn(cls, name: str) -> 'Column':
        ...

    @property
    def primaryKey(self) -> 'Column':
        ...

    @property
    def primaryKeyValue(self) -> Any:
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
    def convertDataFromString(self, conn: 'connection.Connection', string: str) -> Any:
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

    def convertInsertableFromData(self, conn: 'connection.Connection', data: Any) -> Any:
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

    def convertDataFromString(self, conn: 'connection.Connection', string: str) -> Any:
        return self.model.instatiateAll(conn, sql.SQL('WHERE {} = {}').format(
            sql.Identifier(self.model.__primary_key__.name),
            sql.Literal(string)
        ))[0]

    def convertInsertableFromData(self, conn: 'connection.Connection', data: Any) -> Any:
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

    def convertDataFromString(self, conn: 'connection.Connection', string: str) -> Any:
        return self.type.convertDataFromString(conn, string)

    def convertInsertableFromData(self, conn: 'connection.Connection', data: Any) -> Any:
        return self.type.convertInsertableFromData(conn, data)

    def __str__(self) -> str:
        return str(self.type)


class NotNull(ModifiedColumnType):
    @property
    def typeStatement(self) -> 'sql.Composable':
        return sql.SQL('{} NOT NULL').format(self.type.typeStatement)

    def convertInsertableFromData(self, conn: 'connection.Connection', data: Any) -> Any:
        if data is None:
            raise TypeError('Attempted to fill not null field with null')
        return super().convertInsertableFromData(conn, data)

    def __str__(self) -> str:
        return str(self.type) + ' NOT NULL'


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

    def convertDataFromString(self, conn: 'connection.Connection', string: str) -> Any:
        return self.converter(string)

    def convertInsertableFromData(self, conn: 'connection.Connection', data: Any) -> Any:
        return self.inverse(data)

    def __str__(self) -> str:
        return self.type


class EnumType(ColumnType):
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

    def convertDataFromString(self, conn: 'connection.Connection', string: str) -> Any:
        return string

    def convertInsertableFromData(self, conn: 'connection.Connection', data: Any) -> Any:
        strData = str(data)

        if strData is not None and strData not in self.enums:
            raise TypeError(f'Attempted to insert {strData} into enum {self.type} which only accepts {self.enums}')

        return strData

    def __str__(self) -> str:
        return self.type


class Serial(LiteralType):
    def __init__(self) -> None:
        super().__init__('SERIAL', int, int)

    @property
    def rawType(self) -> str:
        return 'INTEGER'


INTEGER = LiteralType('INTEGER', int, int)
SERIAL = Serial()
REAL = LiteralType('DOUBLE PRECISION', float, float)

TEXT = LiteralType('TEXT', str, str)

# TODO: Fill out placeholder converters
TIMESTAMP = LiteralType('TIMESTAMP', str, str)
TIMESTAMP_WITH_TIMEZONE = LiteralType('TIMESTAMP WITH TIME ZONE', str, str)
DATE = LiteralType('DATE', str, str)
TIME = LiteralType('TIME', str, str)
TIME_WITH_TIMEZONE = LiteralType('TIME WITH TIME ZONE', str, str)
INTERVAL = LiteralType('INTERVAL', str, str)

BOOL = LiteralType('BOOLEAN', bool, bool)


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
