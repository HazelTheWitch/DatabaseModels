import unittest

from src import databasemodels as dbm


class TestFixedPoint(unittest.TestCase):
    def test_init(self):
        f0 = dbm.FixedPointValue('123.456')

        self.assertEqual(f0.precision, 6)
        self.assertEqual(f0.scale, 3)
        self.assertEqual(f0._value, 123456)
        self.assertEqual(f0._scaleFactor, 0.001)
        self.assertEqual(f0._minValue, -1000000)
        self.assertEqual(f0._maxValue, 1000000)

        f1 = dbm.FixedPointValue('123.456', 16, 8)

        self.assertEqual(f1.precision, 16)
        self.assertEqual(f1.scale, 8)
        self.assertEqual(f1._value, 12345600000)
        self.assertEqual(f1._scaleFactor, 1 / 10 ** 8)
        self.assertEqual(f1._minValue, -(10 ** 16))
        self.assertEqual(f1._maxValue, 10 ** 16)

        f2 = dbm.FixedPointValue('-123.456')

        self.assertEqual(f2.precision, 6)
        self.assertEqual(f2.scale, 3)
        self.assertEqual(f2._value, -123456)
        self.assertEqual(f2._scaleFactor, 0.001)
        self.assertEqual(f2._minValue, -1000000)
        self.assertEqual(f2._maxValue, 1000000)

        f3 = dbm.FixedPointValue(123.456)

        self.assertEqual(f3.precision, 6)
        self.assertEqual(f3.scale, 3)
        self.assertEqual(f3._value, 123456)
        self.assertEqual(f3._scaleFactor, 0.001)
        self.assertEqual(f3._minValue, -1000000)
        self.assertEqual(f3._maxValue, 1000000)

        f4 = dbm.FixedPointValue(123.456, 16, 8)

        self.assertEqual(f4.precision, 16)
        self.assertEqual(f4.scale, 8)
        self.assertEqual(f4._value, 12345600000)
        self.assertEqual(f4._scaleFactor, 1 / 10 ** 8)
        self.assertEqual(f4._minValue, -(10 ** 16))
        self.assertEqual(f4._maxValue, 10 ** 16)



    def test_str(self):
        f0 = dbm.FixedPointValue('123.456')

        self.assertEqual(str(f0), '123.456')

        f1 = dbm.FixedPointValue('123.456', 16, 8)

        self.assertEqual(str(f1), '123.456')

    def test_float(self):
        f0 = dbm.FixedPointValue('123.456')

        self.assertEqual(float(f0), 123.456)

        f1 = dbm.FixedPointValue('123.456', 16, 8)

        self.assertEqual(float(f1), 123.456)

        f2 = dbm.FixedPointValue('-123.456')

        self.assertEqual(float(f2), -123.456)

    def test_eq(self):
        f0 = dbm.FixedPointValue('123.456')

        f1 = dbm.FixedPointValue('123.456', 16, 8)

        self.assertEqual(f1, 123.456)
        self.assertEqual(f1, f0)
        self.assertNotEqual(f1, 123)
        self.assertNotEqual(f1, '123')

        f2 = dbm.FixedPointValue('-123.456')

        self.assertEqual(f2, -123.456)
        self.assertNotEqual(f2, f0)
        self.assertNotEqual(f2, 123)
        self.assertNotEqual(f2, '123')

    def test_addition(self):
        f0 = dbm.FixedPointValue('12.5', 5, 2)
        f1 = dbm.FixedPointValue('2.25', 5, 2)

        f2 = dbm.FixedPointValue('14.75', 5, 2)

        f3 = dbm.FixedPointValue('-4.75', 5, 2)

        f4 = dbm.FixedPointValue('10.00', 5, 2)

        self.assertEqual(f0 + f1, f2)

        self.assertEqual(f2 + f3, f4)

    def test_negation(self):
        f0 = dbm.FixedPointValue('12.5', 5, 2)
        f1 = dbm.FixedPointValue('2.25', 5, 2)

        f2 = dbm.FixedPointValue('-12.5', 5, 2)
        f3 = dbm.FixedPointValue('-2.25', 5, 2)

        self.assertEqual(-f0, f2)
        self.assertEqual(-f1, f3)

    def test_subtraction(self):
        f0 = dbm.FixedPointValue('12.5', 5, 2)
        f1 = dbm.FixedPointValue('2.5', 5, 2)

        f2 = dbm.FixedPointValue('10', 5, 2)

        self.assertEqual(f0 - f1, f2)

    def test_scaleAndPrecisionShift(self):
        f0 = dbm.FixedPointValue('12.5', 5, 2)

        with self.assertRaises(dbm.FixedPointOverflowError):
            f0.changePrecisionAndScale(5, 5)

        f1 = dbm.FixedPointValue('12.5', 5, 3)

        self.assertEqual(f0.changePrecisionAndScale(5, 3), f1)
        self.assertEqual(f0.changePrecisionAndScale(5, 3).scale, 3)
        self.assertEqual(f0.changePrecisionAndScale(7, 3).precision, 7)

    def test_multiplication(self):
        f0 = dbm.FixedPointValue('12.25', 5, 2)
        f1 = dbm.FixedPointValue('1.25', 3, 2)

        f2 = dbm.FixedPointValue('27.32', 5, 2)
        f3 = dbm.FixedPointValue('15.31', 5, 2)

        self.assertEqual(f0 * 2.23, f2)
        self.assertEqual(f0 * f1, f3)

    def test_division(self):
        f0 = dbm.FixedPointValue('12.25', 5, 2)
        f1 = dbm.FixedPointValue('1.25', 5, 2)
        f2 = dbm.FixedPointValue('0.05', 5, 2)

        f3 = dbm.FixedPointValue('9.8', 5, 2)
        f4 = dbm.FixedPointValue('245', 5, 2)

        self.assertEqual(f0.divide(f1, 6), f3)
        self.assertEqual(f0 / f1, f3)

        self.assertEqual(f0 / f2, f4)


if __name__ == '__main__':
    unittest.main()
