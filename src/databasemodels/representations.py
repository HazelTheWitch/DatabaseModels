import re
from typing import Optional, overload, Union, cast

from .exceptions import FixedPointOverflowError, FixedPointError, FixedPointUnderflowError


__all__ = [
    'FixedPointValue'
]


VALUE_REGEX = re.compile(r'^(-)?(\d+)(?:\.(\d+))?$')


NUMERIC_VALUE = Union[int, float, 'FixedPointValue']


class FixedPointValue:
    @overload
    def __init__(self, value: Union[str, float]) -> None:
        ...

    @overload
    def __init__(self, value: Union[str, float, int], precision: int, scale: int) -> None:
        ...

    def __init__(self, value: Union[str, float, int], precision: Optional[int] = None, scale: Optional[int] = None) -> None:
        nones = (1 if precision is None else 0) + (1 if scale is None else 0)

        if nones == 1:
            raise ValueError('Please call one of the overloaded init methods.')

        if type(value) == float:
            value = str(round(value, scale))

        if type(value) == str:
            match = VALUE_REGEX.match(value)

            if match is None:
                raise ValueError(rf'Invalid value ({value}) must match ^(\d+)(?:\.(\d+))?$')

            sign = -1 if match.group(1) is not None else 1
            integer = match.group(2)
            decimal = match.group(3)

            if decimal is None:
                decimal = ''

            if precision is None:
                precision = len(integer) + len(decimal)

            if scale is None:
                scale = len(decimal)

            if len(integer + decimal + '0' * (scale - len(decimal))) > precision:
                raise FixedPointError(f'Invalid number for given scale and precision: {value}')

            self._value: int = sign * int(integer + decimal + '0' * (scale - len(decimal)))
        elif type(value) == int:
            self._value: int = value  # type: ignore

        if precision is None or precision < 1:
            raise FixedPointError('Invalid precision')

        if scale is None or scale < 0:
            raise FixedPointError('Invalid scale')

        self.precision = precision
        self.scale = scale

        self._maxValue = 10 ** self.precision
        self._minValue = -self._maxValue

        self._lscale = precision - scale

        self._scaleFactor = 1 / 10 ** scale

    def _compatible(self, other: 'FixedPointValue') -> bool:
        return self.precision == other.precision and self.scale == other.scale

    def __float__(self) -> float:
        return self._value * self._scaleFactor  # type: ignore

    def __str__(self) -> str:
        representation = str(self._value)
        representation = '0' * (self.precision - len(representation)) + representation
        representation = representation[:self._lscale] + '.' + representation[self._lscale:]
        return representation.strip('0.')

    def __eq__(self, other: object) -> bool:
        if type(other) == int or type(other) == float:
            return float(self) == other
        elif isinstance(other, FixedPointValue):
            return str(self) == str(other)
        return False

    def __add__(self, other: NUMERIC_VALUE) -> 'FixedPointValue':
        if type(other) == int or type(other) == float:
            other = FixedPointValue(str(other), self.precision, self.scale)

        other = cast('FixedPointValue', other)

        if not self._compatible(other):
            other = other.changePrecisionAndScale(self.precision, self.scale)

        newValue = self._value + other._value

        if newValue <= self._minValue:
            raise FixedPointUnderflowError(newValue, self._minValue)

        if newValue >= self._maxValue:
            raise FixedPointOverflowError(newValue, self._maxValue)

        return FixedPointValue(newValue, self.precision, self.scale)

    def __sub__(self, other: NUMERIC_VALUE) -> 'FixedPointValue':
        return self + -other

    def __mul__(self, other: NUMERIC_VALUE) -> 'FixedPointValue':
        if type(other) == int or type(other) == float:
            other = FixedPointValue(str(other), self.precision, self.scale)

        other = cast('FixedPointValue', other)

        if not self._compatible(other):
            other = other.changePrecisionAndScale(self.precision, self.scale)

        value = int(round(self._value * other._value * self._scaleFactor))

        return FixedPointValue(value, self.precision, self.scale)

    def __truediv__(self, other: NUMERIC_VALUE) -> 'FixedPointValue':
        return self.divide(other, self.scale + self.precision)

    def __neg__(self) -> 'FixedPointValue':
        return FixedPointValue(-self._value, self.precision, self.scale)

    def divide(self, other: NUMERIC_VALUE, newScale: int) -> 'FixedPointValue':
        if type(other) == int or type(other) == float:
            other = FixedPointValue(str(other), self.precision, self.scale)

        other = cast('FixedPointValue', other)

        dividend = self.changeScale(newScale)

        value = dividend._value // other._value

        scale = newScale - other.scale

        return FixedPointValue(value, dividend.precision, scale).changePrecisionAndScale(self.precision, self.scale)

    def changeScale(self, newScale: int) -> 'FixedPointValue':
        return self.changePrecisionAndScale(self.precision + (newScale - self.scale), newScale)

    def changePrecisionAndScale(self, newPrecision: int, newScale: int) -> 'FixedPointValue':
        if newScale < 0:
            raise FixedPointError(f'Invalid scale {newScale}')

        if newPrecision < 1:
            raise FixedPointError(f'Invalid precision {newPrecision}')

        difference = newScale - self.scale

        newValue = int(round(self._value * 10 ** difference))

        maxValue = 10 ** newPrecision

        if newValue >= maxValue:
            raise FixedPointOverflowError(newValue, maxValue)

        if newValue <= -maxValue:
            raise FixedPointUnderflowError(newValue, -maxValue)

        return FixedPointValue(newValue, newPrecision, newScale)
