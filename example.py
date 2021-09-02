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
    favoriteNumbers: Array[INTEGER] = NO_DEFAULT
    data: Array[Array[TEXT]] = NO_DEFAULT


@dbm.model('example', 'orders')
@dataclass()
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
        p2 = Person('Dacota', 'nonbinary', 19, [32], [['a', 'b'], ['c', 'd']])

        o0 = Order(p0, 3, datetime.now(), False, {'a': True, 'b': [1.2, 3.4]})

        print('Original Objects')
        print(p0, p1, p2, o0, sep='\n')
        print()

        p0.insertOrUpdate(conn)
        p1.insertOrUpdate(conn)
        p2.insertOrUpdate(conn)

        o0.insertOrUpdate(conn)

        print('Retrieved from Database')
        print(*Person.instatiateAll(conn), sep='\n')
        print(*Order.instatiateAll(conn))
        print()

        print('Original Objects')
        print(p0, p1, p2, o0, sep='\n')
        print()

        # Update within the object because no reference exists to it within p0
        # A bit weird but works
        o0.customerID.favoriteNumbers = [17]
        p1.age = 21

        p2.age = 20

        o0.fulfilled = True

        # p0.update is implicitly called within o0.update so no need to update here
        # p0.update(conn)

        p1.update(conn)
        p2.update(conn)

        o0.update(conn)

        print('Retrieved from Database')
        print(*Person.instatiateAll(conn), sep='\n')
        print(*Order.instatiateAll(conn))
        print()
