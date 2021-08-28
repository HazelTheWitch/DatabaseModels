import unittest
from src import databasemodels as dbm


class ConnectionUnitTest(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = dbm.createOrLoadConnection('../login.pkl')

    def tearDown(self) -> None:
        self.conn.commit()
        self.conn.close()
