from src import databasemodels as dbm
from src.databasemodels.datatypes import *
from datetime import datetime


from enum import Enum


class Gender(Enum):
    Male = 1
    Female = 2
    Nonbinary = 3


@dbm.model('example', 'people')
class Person:
    name: NotNull[TEXT] = NO_DEFAULT
    id: PrimaryKey[SERIAL] = AUTO_FILLED
    gender: NotNull[EnumType[Gender]] = NO_DEFAULT
    age: INTEGER = NO_DEFAULT
    favoriteNumbers: Array[INTEGER] = NO_DEFAULT
    data: Array[Array[TEXT]] = NO_DEFAULT


@dbm.model('example', 'orders')
class Order:
    id: PrimaryKey[SERIAL] = AUTO_FILLED
    customerID: ForeignKey[Person, 'id'] = NO_DEFAULT
    quantity: INTEGER = NO_DEFAULT
    orderedAt: TIMESTAMP = NO_DEFAULT
    fulfilled: BOOL = NO_DEFAULT
    additionalData: JSONB = NO_DEFAULT


conn = dbm.createOrLoadConnection('login.pkl')

with conn:
    with conn.transaction() as tx:
        Person.createTable(conn, recreateTable=True)
        Order.createTable(conn, recreateTable=True)

        p0 = Person('Hazel', 'female', 20, [1, 2, 3], [['{{a', 'b"""'], ['c', 'd']])
        p1 = Person('Hunter', 'male', 20, ['3', None], None)
        p2 = Person('Dacota', Gender.Nonbinary, 19, [32], [['a', 'b'], ['c', 'd']])

        o0 = Order(p0, 3, datetime.now(), False, {'a': True, 'b': [1.2, 3.4]})

        print('Original Objects')
        print(p0, p1, p2, o0, sep='\n')
        print()

        p0.insertOrUpdate(conn)
        p1.insertOrUpdate(conn)
        p2.insertOrUpdate(conn)

        o0.insertOrUpdate(conn)

        print('Retrieved from Database')
        print(*Person.instantiateAll(conn), sep='\n')
        print(*Order.instantiateAll(conn))
        print()

        print('Original Objects')
        print(p0, p1, p2, o0, sep='\n')
        print()

        with p0.mutate(conn):
            p0.favoriteNumbers = [17]

        with p1.mutate(conn):
            p1.age = 21

        with p2.mutate(conn):
            p2.age = 20

        with o0.mutate(conn):
            o0.fulfilled = True

        print('Retrieved from Database')
        print(*Person.instantiateAll(conn), sep='\n')
        print(*Order.instantiateAll(conn))
        print()
