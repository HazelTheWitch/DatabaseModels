# Database Models

Python class decorator that creates a database representation of a class.
`setup.py` provided for installation, uses psycopg 3 rather than 2 for the
better context manager support and type hinting.

## Recommended Import Setup
```python
import databasemodels as dbm
from databasemodels.datatypes import *
```

This allows you to use bare datatypes such as `INTEGER`, or complex datatypes
such as `PrimaryKey`.

## Features

All basic types and pseudo-types implemented are all capital, whereas type modifiers
and constructed types like enums are capital camel case.

Currently, implemented types/pseudo-types include:
```
EnumType
INTEGER
SERIAL
REAL
TEXT
TIMESTAMP
TIMESTAMP_WITH_TIMEZONE
DATE
TIME
BOOL
VARCHAR
CHAR
NUMERIC
```

Currently, implemented constraints include:
```
ForeignKey
PrimaryKey
Unique
NotNull
```

## Planned Features

 - JSON type
 - Array types
 - Interval type
 - Composite type
 - Full documentation across all the module
 - Implementation of all types listed [here](https://www.postgresql.org/docs/12/datatype.html).
 - "Mutation" context managers for database model instances
 - Higher unittest coverage
 - Higher customizability with table creation and other database operations
 - Experiment with swapping order of decorators to hopefully get auto-complete