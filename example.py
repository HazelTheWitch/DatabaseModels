from src import databasemodels as dbm
from src.databasemodels.datatypes import *
from dataclasses import dataclass
from datetime import datetime


@dbm.model('example', 'people')
@dataclass()
class Person:
    name: NotNull[TEXT] = NO_DEFAULT
    id: PrimaryKey[SERIAL] = AUTO_FILLED
    gender: NotNull[EnumType('gender', ('male', 'female', 'nonbinary'))] = NO_DEFAULT
    age: INTEGER = NO_DEFAULT
    favoriteNumber: INTEGER = NO_DEFAULT


@dbm.model('example', 'orders')
@dataclass()
class Order:
    id: PrimaryKey[SERIAL] = AUTO_FILLED
    customerID: ForeignKey[Person, 'id'] = NO_DEFAULT
    quantity: INTEGER = NO_DEFAULT
    orderedAt: TIMESTAMP = NO_DEFAULT


conn = dbm.createOrLoadConnection('login.pkl')

with conn:
    Person.createTable(conn, recreateTable=True)
    Order.createTable(conn, recreateTable=True)

    p0 = Person('Hazel', 'female', 20, None)
    p1 = Person('Hunter', 'male', 20, '3')
    p2 = Person('Dacota', 'nonbinary', 19, 32)

    o0 = Order(p0, 3, datetime.now())

    print('Original Objects')
    print(p0, p1, p2, o0, sep='\n')
    print()

    o0.insertOrUpdate(conn)

    p0.insertOrUpdate(conn)
    p1.insertOrUpdate(conn)
    p2.insertOrUpdate(conn)

    print('Retrieved from Database')
    print(*Person.instatiateAll(conn), sep='\n')
    print(*Order.instatiateAll(conn))
    print()

    print('Original Objects')
    print(p0, p1, p2, o0, sep='\n')
    print()

    p0.favoriteNumber = 17
    p1.age = 21

    p2.age = 20

    o0.quantity = 5

    p0.update(conn)
    p1.update(conn)
    p2.update(conn)

    o0.update(conn)

    print('Retrieved from Database')
    print(*Person.instatiateAll(conn), sep='\n')
    print(*Order.instatiateAll(conn))
    print()

conn.close()
