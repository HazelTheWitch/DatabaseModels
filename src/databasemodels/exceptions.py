__all__ = [
    'DBModelError',
    'PrimaryKeyError',
    'NullValueError',
    'EnumValueError',
    'FieldDefaultValueError',
    'DBModelWarning',
    'ArrayLengthWarning',
    'FixedPointError',
    'FixedPointOverflowError',
    'FixedPointUnderflowError',
]


class DBModelError(Exception):
    ...


class PrimaryKeyError(DBModelError):
    ...


class NullValueError(DBModelError):
    ...


class EnumValueError(DBModelError):
    ...


class FieldDefaultValueError(DBModelError):
    ...


class DBModelWarning(RuntimeWarning):
    ...


class ArrayLengthWarning(DBModelWarning):
    ...


class FixedPointError(RuntimeError):
    ...


class FixedPointOverflowError(FixedPointError):
    def __init__(self, value: int, maxValue: int) -> None:
        super().__init__(f'{value} >= {maxValue}')
        self.value = value
        self.maxValue = maxValue


class FixedPointUnderflowError(FixedPointError):
    def __init__(self, value: int, minValue: int) -> None:
        super().__init__(f'{value} <= {minValue}')
        self.value = value
        self.maxValue = minValue
