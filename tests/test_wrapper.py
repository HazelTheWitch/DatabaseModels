import unittest
from dataclasses import dataclass

from src import databasemodels as dbm
from src.databasemodels.datatypes import *


class ConnectionUnitTest(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = dbm.createOrLoadConnection('../login.pkl')

    def tearDown(self) -> None:
        self.conn.commit()
        self.conn.close()


class TestDecorator(unittest.TestCase):
    def test_creation(self) -> None:
        @dbm.model('unittests', 'fruits')
        @dataclass
        class Fruit:
            id: PrimaryKey[SERIAL] = AUTO_FILLED
            name: TEXT = NO_DEFAULT
            weight: REAL = NO_DEFAULT
            color: EnumType('color', ('red', 'orange', 'yellow', 'green', 'blue', 'indigo', 'violet')) = NO_DEFAULT

        pear = Fruit('Pear', 3, 'yellow')

        self.assertEqual(pear.name, 'Pear')
        self.assertEqual(pear.weight, 3)
        self.assertEqual(pear.color, 'yellow')


class TestGetters(unittest.TestCase):
    def setUp(self) -> None:
        @dbm.model('unittests', 'fruits')
        @dataclass
        class Fruit:
            id: PrimaryKey[SERIAL] = AUTO_FILLED
            name: TEXT = NO_DEFAULT
            weight: REAL = NO_DEFAULT
            color: EnumType('color', ('red', 'orange', 'yellow', 'green', 'blue', 'indigo', 'violet')) = NO_DEFAULT

        self.pear = Fruit('Pear', 3, 'yellow')

    def test_getColumn(self) -> None:
        self.assertIsInstance(self.pear.getColumn('id'), Column)
        self.assertIsInstance(self.pear.getColumn('name'), Column)
        self.assertIsInstance(self.pear.getColumn('weight'), Column)
        self.assertIsInstance(self.pear.getColumn('color'), Column)

        with self.assertRaises(KeyError):
            self.pear.getColumn('non_existant')

    def test_primaryKey(self) -> None:
        self.assertEqual(self.pear.getColumn('id'), self.pear.primaryKey)

    def test_schema(self) -> None:
        self.assertEqual(self.pear.schema, 'unittests')

    def test_table(self) -> None:
        self.assertEqual(self.pear.table, 'fruits')


class TestCreation(ConnectionUnitTest):
    def setUp(self) -> None:
        super().setUp()

        @dbm.model('unittests', 'fruits')
        @dataclass
        class Fruit:
            id: PrimaryKey[SERIAL] = AUTO_FILLED
            name: TEXT = NO_DEFAULT
            weight: REAL = NO_DEFAULT
            color: EnumType('color', ('red', 'orange', 'yellow', 'green', 'blue', 'indigo', 'violet')) = NO_DEFAULT

        @dbm.model('unittests', 'fruitbaskets')
        @dataclass
        class FruitBasket:
            id: PrimaryKey[SERIAL] = AUTO_FILLED
            fruit: ForeignKey[Fruit] = NO_DEFAULT
            quantity: INTEGER = NO_DEFAULT

        self.Fruit = Fruit
        self.FruitBasket = FruitBasket

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
            color: EnumType('color', ('red', 'orange', 'yellow', 'green', 'blue', 'indigo', 'violet')) = NO_DEFAULT

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
                'red'
            ),
            self.User(
                self.Product(
                    'Hammer',
                    2.3,
                    'yellow'
                ),
                'Hazel',
                'localhost'  # :)
            ),
            3
        )

        order.insert(self.conn)

        self.assertEqual(self.Order.instatiateAll(self.conn)[0], order)
        self.assertFalse(self.Order.instatiateAll(self.conn)[0] is order)


if __name__ == '__main__':
    unittest.main()
