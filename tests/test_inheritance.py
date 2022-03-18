import unittest

from dataclasses import dataclass

from helper import ConnectionUnitTest

from src import databasemodels as dbm
from src.databasemodels.datatypes import *


class TestInheritance(ConnectionUnitTest):
    def test_singleInheritance(self):
        @dbm.model('unittests', 'basesingle')
        @dataclass
        class BaseSingle:
            id: PrimaryKey[SERIAL] = AUTO_FILLED
            a: NotNull[INTEGER] = NO_DEFAULT

        @dbm.model('unittests', 'subsingle')
        @dataclass
        class SubSingle(BaseSingle):
            b: NotNull[INTEGER] = NO_DEFAULT

        SubSingle.createTable(self.conn, recreateTable=True)
        BaseSingle.createTable(self.conn, recreateTable=True)

        sub = SubSingle(1, 2)

        self.assertEqual(sub.a, 1)
        self.assertEqual(sub.b, 2)

        sub.insert(self.conn)

        subNew = SubSingle.instantiateOne(self.conn)

        self.assertEqual(sub, subNew)

        base = BaseSingle(1)

        self.assertEqual(base.a, 1)

        base.insert(self.conn)

        baseNew = BaseSingle.instantiateOne(self.conn)

        self.assertEqual(base, baseNew)


if __name__ == '__main__':
    unittest.main()
