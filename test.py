import pickle

import databasemodels as dbms
from databasemodels.datatypes import *
from dataclasses import dataclass
from psycopg import connect


@dbms.model('versiontwo', 'people')
@dataclass()
class Person:
    name: NotNull[TEXT]
    # gender: NotNull[EnumType('gender', 'male', 'female', 'nonbinary')]
    # age: NoInsert[INTEGER]
    # favoriteNumber: INTEGER
    id: PrimaryKey[SERIAL] = None


@dbms.model('versiontwo', 'orders')
@dataclass()
class Orders:
    # id: PrimaryKey[SERIAL]
    customerID: ForeignKey[Person, 'id']
    quantity: INTEGER


with open('login.pkl', 'rb') as f:
    login = pickle.load(f)

conn = connect(**login)

with conn:
    # p = Person(conn, 'Hazel')

    Person.createTable(conn)
    Orders.createTable(conn)

conn.close()
