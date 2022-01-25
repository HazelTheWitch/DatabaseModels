import contextlib
import unittest
from dataclasses import dataclass

from src import databasemodels as dbm
from src.databasemodels.datatypes import *

from helper import ConnectionUnitTest

from enum import Enum


class Color(Enum):
    RED = 'red',
    ORANGE = 'orange',
    YELLOW = 'yellow',
    GREEN = 'green',
    BLUE = 'blue',
    INDIGO = 'indigo',
    VIOLET = 'violet'


class TestDecorator(ConnectionUnitTest):
    def setUp(self) -> None:
        super().setUp()

        @dbm.model('unittests', 'fruits', useInstanceCache=False)
        @dataclass
        class Fruit:
            id: PrimaryKey[SERIAL] = AUTO_FILLED
            name: TEXT = NO_DEFAULT
            weight: REAL = NO_DEFAULT
            color: EnumType[Color] = NO_DEFAULT

        self.Fruit = Fruit

        Fruit.createTable(self.conn, recreateTable=True)

    def test_creation(self) -> None:
        pear = self.Fruit('Pear', 3, 'yellow')

        self.assertEqual(pear.name, 'Pear')
        self.assertEqual(pear.weight, 3)
        self.assertEqual(pear.color, 'yellow')

    def test_mutate(self) -> None:
        pear = self.Fruit('Pear', 3, 'yellow')

        with pear.mutate(self.conn, True):
            pear.weight = 4

        self.assertEqual(pear.weight, 4)
        self.assertEqual(self.Fruit.instantiateFromPrimaryKey(self.conn, pear.id), pear)

        with self.assertRaises(ValueError):
            with pear.mutate(self.conn, True):
                pear.weight = 5
                raise ValueError

        self.assertEqual(pear.weight, 4)
        self.assertEqual(self.Fruit.instantiateFromPrimaryKey(self.conn, pear.id), pear)

        with pear.mutate(self.conn, False):
            pear.weight = 3

        self.assertEqual(pear.weight, 3)
        self.assertEqual(self.Fruit.instantiateFromPrimaryKey(self.conn, pear.id).weight, 4)

        with pear.mutate(self.conn, True):
            pear.weight = 3

        with self.assertRaises(ValueError):
            with pear.mutate(self.conn, False):
                pear.weight = 5
                raise ValueError

        self.assertEqual(pear.weight, 3)
        self.assertEqual(self.Fruit.instantiateFromPrimaryKey(self.conn, pear.id), pear)

    def test_delete(self) -> None:
        apple = self.Fruit('Apple', 3, 'yellow')

        apple.insert(self.conn)

        self.assertTrue(apple.delete(self.conn))
        self.assertFalse(apple.delete(self.conn))


class TestGetters(unittest.TestCase):
    def setUp(self) -> None:
        @dbm.model('unittests', 'fruits')
        @dataclass
        class Fruit:
            id: PrimaryKey[SERIAL] = AUTO_FILLED
            name: TEXT = NO_DEFAULT
            weight: REAL = NO_DEFAULT
            color: EnumType[Color] = NO_DEFAULT

        self.pear = Fruit('Pear', 3, 'yellow')
        self.Fruit = Fruit

    def test_getColumn(self) -> None:
        self.assertIsInstance(self.pear.getColumn('id'), dbm.Column)
        self.assertIsInstance(self.pear.getColumn('name'), dbm.Column)
        self.assertIsInstance(self.pear.getColumn('weight'), dbm.Column)
        self.assertIsInstance(self.pear.getColumn('color'), dbm.Column)

        with self.assertRaises(KeyError):
            self.pear.getColumn('non_existant')

    def test_primaryKey(self) -> None:
        self.assertEqual(self.pear.getColumn('id'), self.pear.primaryKeyColumn)
        self.assertEqual(self.pear.getColumn('id'), self.Fruit.primaryKeyColumn)

    def test_schema(self) -> None:
        self.assertEqual(self.pear.schema, 'unittests')
        self.assertEqual(self.Fruit.schema, 'unittests')

    def test_table(self) -> None:
        self.assertEqual(self.pear.table, 'fruits')
        self.assertEqual(self.Fruit.table, 'fruits')


class TestCreation(ConnectionUnitTest):
    def setUp(self) -> None:
        super().setUp()

        @dbm.model('unittests', 'fruits')
        @dataclass
        class Fruit:
            id: PrimaryKey[SERIAL] = AUTO_FILLED
            name: TEXT = NO_DEFAULT
            weight: REAL = NO_DEFAULT
            color: EnumType[Color] = NO_DEFAULT

        @dbm.model('unittests', 'fruitbaskets')
        @dataclass
        class FruitBasket:
            id: PrimaryKey[SERIAL] = AUTO_FILLED
            fruit: ForeignKey[Fruit] = NO_DEFAULT
            quantity: INTEGER = NO_DEFAULT

        self.Fruit = Fruit
        self.FruitBasket = FruitBasket

        Fruit.createTable(self.conn, recreateTable=True)
        FruitBasket.createTable(self.conn, recreateTable=True)

    def test_createTable(self) -> None:
        self.Fruit.createTable(self.conn, recreateTable=True)
        self.FruitBasket.createTable(self.conn, recreateTable=True)


class TestInstantiation(ConnectionUnitTest):
    def setUp(self) -> None:
        super().setUp()

        @dbm.model('unittests', 'products')
        @dataclass
        class Product:
            id: PrimaryKey[SERIAL] = AUTO_FILLED
            name: TEXT = NO_DEFAULT
            weight: REAL = NO_DEFAULT
            color: EnumType[Color] = NO_DEFAULT

        @dbm.model('unittests', 'users')
        @dataclass
        class User:
            id: PrimaryKey[SERIAL] = AUTO_FILLED
            favoriteProduct: ForeignKey[Product] = NO_DEFAULT
            name: TEXT = NO_DEFAULT
            address: TEXT = NO_DEFAULT

        @dbm.model('unittests', 'orders')
        @dataclass
        class Order:
            id: PrimaryKey[SERIAL] = AUTO_FILLED
            product: ForeignKey[Product] = NO_DEFAULT
            user: ForeignKey[User, 'id'] = NO_DEFAULT
            quantity: INTEGER = NO_DEFAULT

        self.Product = Product
        self.User = User
        self.Order = Order

        Product.createTable(self.conn, recreateTable=True)
        User.createTable(self.conn, recreateTable=True)
        Order.createTable(self.conn, recreateTable=True)

    def test_creationAndRetrieval(self) -> None:
        order = self.Order(
            self.Product(
                'Shovel',
                1.5,
                Color.RED
            ),
            self.User(
                self.Product(
                    'Hammer',
                    2.3,
                    Color.YELLOW
                ),
                'Hazel',
                'localhost'  # :)
            ),
            3
        )

        order.insert(self.conn)

        self.assertEqual(self.Order.instantiateAll(self.conn)[0], order)


if __name__ == '__main__':
    unittest.main()
