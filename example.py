import pickle

import databasemodels as dbm
from databasemodels.datatypes import *
from dataclasses import dataclass
from psycopg import connect


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


conn = dbm.createOrLoadConnection('login.pkl')

with conn:
    Person.createTable(conn)
    Order.createTable(conn)

    p0 = Person('Hazel', 'female', 20, None)
    p1 = Person('Hunter', 'male', 20, '3')

    o0 = Order(p0, 3)

    print('Original Objects')
    print(p0, p1, o0, sep='\n')
    print()

    with conn.cursor() as cur:
        cur.execute('DELETE FROM example.orders;')
        cur.execute('DELETE FROM example.people;')

    o0.insert(conn)

    # p0.insert(conn)
    p1.insert(conn)

    print('Retrieved from Database')
    print(*Person.instatiateAll(conn), sep='\n')
    print(*Order.instatiateAll(conn))
    print()

    print('Original Objects')
    print(p0, p1, o0, sep='\n')
    print()

    p0.favoriteNumber = 17
    p1.age = 21

    o0.quantity = 5

    p0.update(conn)
    p1.update(conn)

    o0.update(conn)

    print('Retrieved from Database')
    print(*Person.instatiateAll(conn), sep='\n')
    print(*Order.instatiateAll(conn))
    print()

conn.close()
