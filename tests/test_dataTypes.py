import unittest

from dataclasses import dataclass

from src import databasemodels as dbm
from src.databasemodels.datatypes import *

from helper import ConnectionUnitTest

import datetime as dt
import pytz


class TestDatatypes(ConnectionUnitTest):
    def setUp(self) -> None:
        super().setUp()

        @dbm.model('unittests', 'numerictypes')
        @dataclass
        class Numeric:
            id: PrimaryKey[SERIAL] = AUTO_FILLED
            integer: NotNull[INTEGER] = NO_DEFAULT
            real: NotNull[REAL] = NO_DEFAULT
            numeric: NotNull[NUMERIC(6, 3)] = NO_DEFAULT

        self.Numeric = Numeric

        @dbm.model('unittests', 'strings')
        @dataclass
        class String:
            type: NotNull[EnumType('type', ('A', 'B', 'C'))] = NO_DEFAULT
            text: NotNull[TEXT] = NO_DEFAULT
            varchar: NotNull[VARCHAR(16)] = NO_DEFAULT
            char: NotNull[CHAR(16)] = NO_DEFAULT

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
        String.createTable(self.conn, recreateTable=True)
        Time.createTable(self.conn, recreateTable=True)
        Misc.createTable(self.conn, recreateTable=True)

    def test_numerics(self) -> None:
        n0 = self.Numeric(1, 1.5, 123.456)

        n0.insert(self.conn)

        n1 = self.Numeric.instatiateAll(self.conn)[0]

        self.assertEqual(n0, n1)

    def test_strings(self) -> None:
        s0 = self.String('A', 'This is some text', 'this is short', 'padded')

        s0.insert(self.conn)

        s1 = self.String.instatiateAll(self.conn)[0]

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

        t1 = self.Time.instatiateAll(self.conn)[0]

        self.assertEqual(t0, t1)

    def test_miscs(self) -> None:
        m0 = self.Misc(False)

        m0.insert(self.conn)

        m1 = self.Misc.instatiateAll(self.conn)[0]

        self.assertEqual(m0, m1)


if __name__ == '__main__':
    unittest.main()
