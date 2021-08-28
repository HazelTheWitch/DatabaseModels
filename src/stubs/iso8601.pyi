import datetime
from typing import Optional


def parse_date(datestring: str, default_timezone: Optional['datetime.timezone'] = datetime.timezone.utc) -> 'datetime.datetime':
    ...