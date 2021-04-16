
Overview
--------

kvlog is a key-value store with history, so that it can store multiple
values for the same key over time. It's intended to store data that only
changes relatively slowly (e.g. tens or hundreds of values per key), not
data that changes rapidly (like in a time series database, with thousands
or millions of data points per key).

Conceptually it changes from the simple key-value store (k,v) tuple to
include a timestamp component i.e. we have a (k,ts,v) tuple, indexed on
both `k` and `ts`.

Thus a kvlog can perform traditional key-value operations i.e.

- set key=value
- get key

plus additional operations that include a time component e.g.

- get key at this time
- get all values of key (ordered by time, after this time, etc.)
- set key=value at time T


Status
------

Experimental. API is unstable.

Copyright and Licence
---------------------

Copyright 2021 Gavin Carr <gavin@openfusion.net>.

This project is licensed under the terms of the MIT licence.

