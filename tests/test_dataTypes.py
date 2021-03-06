import unittest

from dataclasses import dataclass

from src import databasemodels as dbm
from src.databasemodels.datatypes import *

from helper import ConnectionUnitTest

import datetime as dt
import pytz

from enum import Enum, auto


class EnumExample(Enum):
    A = auto()
    B = auto()
    C = auto()
    D = auto()


class TestDatatypes(ConnectionUnitTest):
    def setUp(self) -> None:
        super().setUp()

        @dbm.model('unittests', 'numerictypes', useInstanceCache=False)
        @dataclass
        class Numeric:
            id: PrimaryKey[SERIAL] = AUTO_FILLED
            integer: NotNull[INTEGER] = NO_DEFAULT
            real: NotNull[REAL] = NO_DEFAULT
            numeric1: NotNull[NUMERIC[6, 3]] = NO_DEFAULT
            numeric2: NotNull[NUMERIC(6, 3)] = NO_DEFAULT

        self.Numeric = Numeric

        @dbm.model('unittests', 'strings')
        @dataclass
        class String:
            type: NotNull[EnumType[EnumExample]] = NO_DEFAULT
            text: NotNull[TEXT] = NO_DEFAULT
            varchar: NotNull[VARCHAR[16]] = NO_DEFAULT
            char: NotNull[CHAR[16]] = NO_DEFAULT

        self.String = String

        @dbm.model('unittests', 'times')
        @dataclass
        class Time:
            timestamp: NotNull[TIMESTAMP] = NO_DEFAULT
            timestamp_tz: NotNull[TIMESTAMP_WITH_TIMEZONE] = NO_DEFAULT
            date: NotNull[DATE] = NO_DEFAULT
            time: NotNull[TIME] = NO_DEFAULT

        self.Time = Time

        @dbm.model('unittests', 'miscs')
        @dataclass
        class Misc:
            boolean: NotNull[BOOL] = NO_DEFAULT

        self.Misc = Misc

        Numeric.createTable(self.conn, recreateTable=True)
        String.createTable(self.conn, recreateTable=True, recreateColumns=True)
        Time.createTable(self.conn, recreateTable=True)
        Misc.createTable(self.conn, recreateTable=True)

    def test_numerics(self) -> None:
        n0 = self.Numeric(1, 1.5, 123.456, 789.012)

        n0.insert(self.conn)

        n1 = self.Numeric.instantiateOne(self.conn)

        self.assertEqual(n0, n1)

    def test_strings(self) -> None:
        s0 = self.String(EnumExample.A, 'This is some text', 'this is short', 'padded')

        s0.insert(self.conn)

        s1 = self.String.instantiateOne(self.conn)

        self.assertEqual(s0, s1)

    def test_times(self) -> None:
        tz = pytz.timezone('Pacific/Auckland')
        timestamp = dt.datetime(2003, 10, 21, 20, 8, 47)
        t0 = self.Time(
            timestamp,
            tz.localize(timestamp),
            timestamp.date(),
            timestamp.time(),
        )

        t0.insert(self.conn)

        t1 = self.Time.instantiateOne(self.conn)

        self.assertEqual(t0, t1)

    def test_miscs(self) -> None:
        m0 = self.Misc(False)

        m0.insert(self.conn)

        m1 = self.Misc.instantiateOne(self.conn)

        self.assertEqual(m0, m1)


class TestArrays(ConnectionUnitTest):
    def setUp(self) -> None:
        super().setUp()

        @dbm.model('unittests', 'numericarrays')
        @dataclass
        class Numeric:
            integer: NotNull[Array[INTEGER]] = NO_DEFAULT
            real: NotNull[Array[REAL]] = NO_DEFAULT
            numeric: NotNull[Array[NUMERIC[6, 3]]] = NO_DEFAULT

        self.Numeric = Numeric

        @dbm.model('unittests', 'stringarrays')
        @dataclass
        class String:
            type: NotNull[Array[EnumType[EnumExample]]] = NO_DEFAULT
            text: NotNull[Array[TEXT]] = NO_DEFAULT
            varchar: NotNull[Array[VARCHAR[16]]] = NO_DEFAULT
            char: NotNull[Array[CHAR[16]]] = NO_DEFAULT

        self.String = String

        @dbm.model('unittests', 'timearrays')
        @dataclass
        class Time:
            timestamp: NotNull[Array[TIMESTAMP]] = NO_DEFAULT
            timestamp_tz: NotNull[Array[TIMESTAMP_WITH_TIMEZONE]] = NO_DEFAULT
            date: NotNull[Array[DATE]] = NO_DEFAULT
            time: NotNull[Array[TIME]] = NO_DEFAULT

        self.Time = Time

        @dbm.model('unittests', 'miscarrays')
        @dataclass
        class Misc:
            boolean: NotNull[Array[BOOL]] = NO_DEFAULT

        self.Misc = Misc

        Numeric.createTable(self.conn, recreateTable=True)
        String.createTable(self.conn, recreateTable=True)
        Time.createTable(self.conn, recreateTable=True)
        Misc.createTable(self.conn, recreateTable=True)

    def test_numericarrays(self) -> None:
        n0 = self.Numeric([1, 2], [1.5, 3.25], [123.456, 789.012])

        n0.insert(self.conn)

        n1 = self.Numeric.instantiateOne(self.conn)

        self.assertEqual(n0, n1)

    def test_stringarrays(self) -> None:
        s0 = self.String([EnumExample.A, EnumExample.B], ['This is some text', 'som,,,""\'\'{}{}}}{{e more'], ['this is short'], ['padded'])

        s0.insert(self.conn)

        s1 = self.String.instantiateOne(self.conn)

        self.assertEqual(s0, s1)

    def test_timearrays(self) -> None:
        tz = pytz.timezone('Pacific/Auckland')
        timestamp = dt.datetime(2003, 10, 21, 20, 8, 47)
        t0 = self.Time(
            [timestamp],
            [tz.localize(timestamp)],
            [timestamp.date()],
            [timestamp.time()],
        )

        t0.insert(self.conn)

        t1 = self.Time.instantiateOne(self.conn)

        self.assertEqual(t0, t1)

    def test_miscarrays(self) -> None:
        m0 = self.Misc([False, True, True])

        m0.insert(self.conn)

        m1 = self.Misc.instantiateOne(self.conn)

        self.assertEqual(m0, m1)

    def test_emptyarrays(self) -> None:
        m0 = self.Misc([])

        m0.insert(self.conn)

        m1 = self.Misc.instantiateOne(self.conn)

        self.assertEqual(m0, m1)


class TestMultiArrays(ConnectionUnitTest):
    def setUp(self) -> None:
        super().setUp()

        @dbm.model('unittests', 'multiarray')
        @dataclass
        class MultiArray:
            none: INTEGER = NO_DEFAULT
            one: Array[INTEGER] = NO_DEFAULT
            two: Array[Array[INTEGER]] = NO_DEFAULT
            three: Array[Array[Array[INTEGER]]] = NO_DEFAULT
            four: Array[Array[Array[Array[INTEGER]]]] = NO_DEFAULT
            five: Array[Array[Array[Array[Array[INTEGER]]]]] = NO_DEFAULT

        MultiArray.createTable(self.conn, recreateTable=True)

        self.MultiArray = MultiArray

    def test_multiArrays(self) -> None:
        multi = self.MultiArray(
            1,
            [2, 3],
            [[4, 5], [6, 7]],
            [[[8, 9], [10, 11]], [[12, 13], [14, 15]]],
            [[[[16, 17], [18, 19]], [[20, 21], [22, 23]]]],
            [[[[[24, 25], [26, 27]], [[28, 29], [30, 31]]]]]
        )

        multi.insert(self.conn)

        other = self.MultiArray.instantiateOne(self.conn)

        self.assertEqual(multi, other)


class TestComposites(ConnectionUnitTest):
    def setUp(self) -> None:
        super().setUp()

        @dbm.model('unittests', 'compositeforeign')
        @dataclass
        class CompositeForeign:
            id: PrimaryKey[SERIAL] = AUTO_FILLED
            boolean: NotNull[BOOL] = NO_DEFAULT

        @dbm.model('unittests', 'multiarray')
        @dataclass
        class CompositeTypes:
            complexNumber: Composite['complex', (('r', REAL), ('i', REAL))] = NO_DEFAULT
            arrayOfComplex: Array[Composite['complex', (('r', REAL), ('i', REAL))]] = NO_DEFAULT
            foreign: Composite['example', (('f', FalseForeignKey[CompositeForeign]), ('x', REAL))] = NO_DEFAULT

        self.CompositeTypes = CompositeTypes
        self.CompositeForeign = CompositeForeign

        CompositeForeign.createTable(self.conn, recreateTable=True)
        CompositeTypes.createTable(self.conn, recreateTable=True, recreateColumns=True)

    def test_composites(self) -> None:
        cf = self.CompositeForeign(False)

        c0 = self.CompositeTypes((1.0, 2.0), [(1.0, 2.0), (3.0, 4.0)], (cf, 1.0))

        c0.insert(self.conn)

        c1 = self.CompositeTypes.instantiateOne(self.conn)

        self.assertEqual(c0, c1)
        self.assertNotEqual(c0, self.CompositeTypes((2.0, 3.0), [(1.0, 2.0), (3.0, 4.0)], (cf, 1.0)))


class TestForeignKeys(ConnectionUnitTest):
    def setUp(self) -> None:
        super().setUp()

        @dbm.model('unittests', 'A')
        @dataclass
        class A:
            id: PrimaryKey[SERIAL] = AUTO_FILLED
            a: INTEGER = NO_DEFAULT

        @dbm.model('unittests', 'B')
        @dataclass
        class B:
            id: PrimaryKey[SERIAL] = AUTO_FILLED
            b: INTEGER = NO_DEFAULT

        @dbm.model('unittests', 'C')
        @dataclass
        class C:
            id: PrimaryKey[SERIAL] = AUTO_FILLED
            a: ForeignKey[A] = NO_DEFAULT
            b: FalseForeignKey[B] = NO_DEFAULT
            bs: Array[FalseForeignKey[B]] = NO_DEFAULT

        self.A = A
        self.B = B
        self.C = C

        A.createTable(self.conn, recreateTable=True)
        B.createTable(self.conn, recreateTable=True)
        C.createTable(self.conn, recreateTable=True)

    def test_foreignKey(self) -> None:
        a, b0, b1 = self.A(1), self.B(2), self.B(3)
        c0 = self.C(a, b0, [b0, b1])

        c0.insert(self.conn)

        c1 = self.C.instantiateOne(self.conn)

        self.assertEqual(c0, c1)

    def test_noneForeignKey(self) -> None:
        c0 = self.C(None, None, [None, None])

        c0.insert(self.conn)

        c1 = self.C.instantiateOne(self.conn)

        self.assertEqual(c0, c1)


if __name__ == '__main__':
    unittest.main()
