import unittest
from dataclasses import dataclass

import databasemodels as dbm
from databasemodels.datatypes import *


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

        self.Fruit = Fruit

    def test_createTable(self) -> None:
        self.Fruit.createTable(self.conn, recreateSchema=True, recreateTable=True)


# class TestInstantiation


if __name__ == '__main__':
    unittest.main()
