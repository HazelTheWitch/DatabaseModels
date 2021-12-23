# Database Models

Python class decorator that creates a database representation of a class and vice versa, a lightweight ORM.
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

Models have a "mutate" context manager which allows you to modify the model safely while reverting the changes if an 
error is raised.

## Planned Features

 - Interval type
 - Additional SQL parameter to createTable
 - Full documentation across all the module
 - Implementation of all types listed [here](https://www.postgresql.org/docs/12/datatype.html).
 - Higher unittest coverage
 - Higher customizability with table creation and other database operations
