from collections import OrderedDict as OD
from dataclasses import fields, MISSING
from typing import Callable, Any, List, Type, Optional, OrderedDict, Dict, Generator, cast, Tuple, Union

from psycopg import connection, sql

__all__ = [
    'model',
]

from .datatypes import NO_DEFAULT, AUTO_FILLED
from .columns import Column
from .protocols import Dataclass, DatabaseModel
from .helper import classproperty


def model(_schema: Optional[str] = None, _table: Optional[str] = None) -> \
        Callable[[Type['Dataclass']], Type['DatabaseModel']]:
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
        allFieldsName: List[str] = []

        # PyCharm does not recognize cls as a Dataclass despite being type hinted as one
        # noinspection PyDataclass
        for i, field in enumerate(fields(cls)):
            definition = Column.fromField(field)

            allFieldsName.append(field.name)
            if field.default is not AUTO_FILLED:
                argsNames.append(field.name)

            columnDefinitions[definition.name] = definition

            if definition.type.primary:
                if _primaryKey is not None:
                    raise TypeError(f'{schemaName}.{tableName} ({cls.__name__}) Has two primary keys defined')
                _primaryKey = definition

            if not (field.default is MISSING or field.default is NO_DEFAULT or field.default is AUTO_FILLED):
                raise TypeError(f'{field.name} does not declare default type of MISSING, NO_DEFAULT, or AUTO_FILLED')

        argsString = ', '.join(argsNames)
        settersString = '\n'.join(f'    self.{a} = {a}' for a in argsNames)

        replaceAutofilledString = '    for a in dir(self):\n'\
                                  '        if getattr(self, a) is AUTO_FILLED:\n'\
                                  '            setattr(self, a, None) '

        funcString = f"def __init__(self, {argsString}):\n{settersString}\n{replaceAutofilledString}"

        # mypy doesn't support this yet so have to silence the error
        class WrappedClass(cls):  # type: ignore
            __column_definitions__: OrderedDict[str, 'Column'] = columnDefinitions
            __primary_key__: Optional['Column'] = _primaryKey

            __schema_name__: str = schemaName
            __table_name__: str = tableName

            def _create(self, conn: 'connection.Connection[Any]', record: Union[Any, Tuple[Any, ...]]) -> None:
                kwargs = {}

                if type(record) != tuple:
                    record = (record,)

                record = cast(Tuple[str, ...], record)

                for kc, v in zip(WrappedClass.__column_definitions__.items(), record):
                    k, c = kc
                    kwargs[k] = c.type.convertDataFromString(conn, v)

                for k, v in kwargs.items():
                    setattr(self, k, v)

            def __str__(self) -> str:
                dictlike = ', '.join(f'{a}={getattr(self, a)}' for a in self.__column_definitions__.keys())
                return f'{self.__schema_name__}.{self.__table_name__}({dictlike})'

            def __dir__(self) -> List[str]:
                return list(set(dir(type(self)) + list(self.__dict__.keys())))

            @classmethod
            def getColumn(cls, name: str) -> 'Column':
                return cls.__column_definitions__[name]

            @classproperty
            def primaryKey(cls: Type['DatabaseModel']) -> Optional['Column']:
                return cls.__primary_key__

            @property
            def primaryKeyValue(self) -> Optional[Any]:
                primary = self.primaryKey
                if primary is None:
                    return None
                return getattr(self, primary.name)

            @classproperty
            def schema(cls: Type['DatabaseModel']) -> str:
                return cls.__schema_name__

            @classproperty
            def table(cls: Type['DatabaseModel']) -> str:
                return cls.__table_name__

            @classmethod
            def createTable(cls, conn: 'connection.Connection[Any]', *, recreateSchema: bool = False,
                            recreateTable: bool = False) -> None:
                for defini in WrappedClass.__column_definitions__.values():
                    defini.initialize(conn)

                createSchema = sql.SQL(
                    'CREATE SCHEMA IF NOT EXISTS {};'
                ).format(
                    sql.Identifier(schemaName)
                )

                createTable = sql.SQL(
                    'CREATE TABLE IF NOT EXISTS {} ({});'
                ).format(
                    sql.Identifier(schemaName, tableName),
                    sql.SQL(', ').join(
                        [d.columnDefinition for d in WrappedClass.__column_definitions__.values()]
                    )
                )

                with conn.cursor() as cur:
                    if recreateSchema:
                        cur.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(
                            sql.Identifier(schemaName)
                        ))

                    cur.execute(createSchema)

                    if recreateTable:
                        cur.execute(sql.SQL('DROP TABLE IF EXISTS {} CASCADE').format(
                            sql.Identifier(schemaName, tableName)
                        ))

                    cur.execute(createTable)

            @classmethod
            def instatiateAll(cls, conn: 'connection.Connection[Any]', query: Union[str, 'sql.Composable'] = '') -> Tuple['WrappedClass', ...]:
                return tuple(cls.instatiate(conn, query))

            @classmethod
            def instatiate(cls, conn: 'connection.Connection[Any]', query: Union[str, 'sql.Composable'] = '') -> \
                    Generator['WrappedClass', None, None]:
                if isinstance(query, sql.Composable):
                    additionalQuery = query
                else:
                    additionalQuery = sql.SQL(query)

                queryStatement = sql.SQL('SELECT ({}) FROM {} {};').format(
                    sql.SQL(', ').join(
                        [sql.Identifier(c.name) for c in WrappedClass.__column_definitions__.values()]
                    ),
                    sql.Identifier(schemaName, tableName),
                    additionalQuery
                )

                with conn.cursor() as cur:
                    cur.execute(queryStatement)

                    record = cur.fetchone()

                    while record is not None:
                        # Abuse duck-typing to get "2 init methods" sort of
                        obj = cls(*argsNames)

                        obj._create(conn, record[0])

                        yield obj

                        record = cur.fetchone()

            @classmethod
            def instantiateFromPrimaryKey(cls, conn: 'connection.Connection[Any]', primaryKey: Any) -> 'DatabaseModel':
                if cls.__primary_key__ is None:
                    raise TypeError(f'Model {cls.__name__} has no primary key to instantiate from')

                return cls.instatiateAll(conn, sql.SQL('WHERE {} = {}').format(
                    sql.Identifier(cls.__primary_key__.name),
                    sql.Literal(primaryKey)
                ))[0]

            def insert(self, conn: 'connection.Connection[Any]', *, doTypeConversion: bool = True) -> None:
                if doTypeConversion:
                    data = [c.type.convertInsertableFromData(conn, getattr(self, c.name)) for c in
                            self.__column_definitions__.values() if c.name in argsNames]
                else:
                    data = [getattr(self, c.name) for c in self.__column_definitions__.values() if c.name in argsNames]

                allColumns = [sql.Identifier(c.name) for c in WrappedClass.__column_definitions__.values()]

                insertStatement = sql.SQL('INSERT INTO {} ({}) VALUES ({}) RETURNING ({});').format(
                    sql.Identifier(schemaName, tableName),
                    sql.SQL(', ').join(
                        [sql.Identifier(c.name) for c in self.__column_definitions__.values() if c.name in argsNames]
                    ),
                    sql.SQL(', ').join(
                        list(map(sql.Literal, data))
                    ),
                    sql.SQL(', ').join(
                        allColumns
                    )
                )

                with conn.cursor() as cur:
                    cur.execute(insertStatement)

                    # After insertion of this object go back and fill in any defaulted fields
                    record = cur.fetchone()

                    self._create(conn, cast(Tuple[Tuple[Any, ...]], record)[0])

            def update(self, conn: 'connection.Connection[Any]', *, doTypeConversion: bool = True) -> None:
                primary = self.primaryKey

                if primary is None:
                    raise TypeError('Can not update a database model without a primary key.')

                if doTypeConversion:
                    data = [c.type.convertInsertableFromData(conn, getattr(self, c.name)) for c in
                            self.__column_definitions__.values()]
                else:
                    data = [getattr(self, c.name) for c in self.__column_definitions__.values()]

                updateStatement = sql.SQL('UPDATE {} SET ({}) = ({}) WHERE {} = {};').format(
                    sql.Identifier(schemaName, tableName),
                    sql.SQL(', ').join(
                        [sql.Identifier(c.name) for c in WrappedClass.__column_definitions__.values()]
                    ),
                    sql.SQL(', ').join(
                        list(map(sql.Literal, data))
                    ),
                    sql.Identifier(primary.name),
                    sql.Literal(self.primaryKeyValue)
                )

                with conn.cursor() as cur:
                    cur.execute(updateStatement)

            def insertOrUpdate(self, conn: 'connection.Connection[Any]', *, doTypeConversion: bool = True) -> None:
                if self.primaryKey is None:
                    raise TypeError(f'{self} does not contain a primary key')

                if self.primaryKeyValue is None:
                    self.insert(conn, doTypeConversion=doTypeConversion)

                instances = WrappedClass.instatiateAll(conn, sql.SQL('WHERE {} = {}').format(
                    sql.Identifier(self.primaryKey.name),
                    sql.Literal(self.primaryKeyValue)
                ))

                if len(instances) == 0:
                    self.insert(conn, doTypeConversion=doTypeConversion)
                else:
                    self.update(conn, doTypeConversion=doTypeConversion)

        miniLocals: Dict[str, Callable[..., None]] = {}

        # Builds init method
        exec(funcString, {'AUTO_FILLED': AUTO_FILLED}, miniLocals)

        # Ignored because this must be done to set init method properly
        WrappedClass.__init__ = miniLocals['__init__']  # type: ignore

        # Transfer wrapped class data over
        WrappedClass.__module__ = cls.__module__
        WrappedClass.__name__ = cls.__name__
        WrappedClass.__qualname__ = cls.__qualname__
        WrappedClass.__annotations__.update(cls.__annotations__)
        WrappedClass.__doc__ = cls.__doc__

        # WrappedClass is of type Type['DatabaseModel'] but type checker does not see that in PyCharm
        return WrappedClass

    return wrapped
