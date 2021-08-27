import pickle

import databasemodels as dbms
from databasemodels.datatypes import *
from dataclasses import dataclass
from psycopg import connect

import inspect


@dbms.model('example', 'people')
@dataclass()
class Person:
    name: NotNull[TEXT]
    id: PrimaryKey[SERIAL] = AUTO_FILLED
    gender: NotNull[EnumType('gender', 'male', 'female', 'nonbinary')] = NO_DEFAULT
    age: NoInsert[INTEGER] = NO_DEFAULT
    favoriteNumber: INTEGER = NO_DEFAULT


@dbms.model('example', 'orders')
@dataclass()
class Order:
    id: PrimaryKey[SERIAL] = AUTO_FILLED
    customerID: ForeignKey[Person, 'id'] = NO_DEFAULT
    quantity: INTEGER = NO_DEFAULT


with open('login.pkl', 'rb') as f:
    login = pickle.load(f)

conn = connect(**login)

with conn:
    print(inspect.signature(Person))
    print(inspect.signature(Order))

    Person.createTable(conn)
    Order.createTable(conn)

    p0 = Person('Hazel', 'female', 20, None)
    p1 = Person('Hunter', 'male', 20, 3)

    print(p0, p1)

conn.close()
