# Clutch Sqlite3 API

Clutch is a simple and straightforward sqlite3 API for Lua.
Its primary design goal is to offer a effective sqlite3 interface while
staying out of the way as much as possible.

NB. The following examples use
[suppliers and parts](http://wiki.c2.com/?SupplierPartsDatabase)
as a sample database.

## Opening a database connection

```lua
clutch = require 'clutch'
db = clutch.open('mydatabase.db')
```

Clutch passes the name of the database file directly to the underlying sqlite3
API, so you can use `':memory:'` as a file name to open an in-memory database,
or an empty string to open a temporary on-disk database. Clutch's unit tests
use the latter mechanism, for example, to create a temporary database for
each test case.

## Querying the database

The primary interface for issuing queries is the `query()` method of the
database connection. It returns an iterator function, returning the query
results one row at a time. A simple query looks like

```lua
for r in db:query("select * from p") do
    print(r.pname, r.weight)
end
```

Clutch maps the result into Lua tables with the column names as keys.

As a convenience, `clutch` provides two query shorthands:

- `queryone()` checks that the query results into exactly one row and returns
that row as a single table. Otherwise it throws an error.
- `queryall()` returns all resulting rows in a Lua array. In case the query
returns an empty result set, the method returns an empty table.

## Binding parameters to queries

### Named parameters

The most staightforward way is to provide query parameters in a table:

```lua
db:query("select * from p where color = :color", {color = 'Red'})
```

Since Clutch uses sqlite3's prepare/bind functions internally, named parameters
can be prefixed by `:`, `$` and `@`, which means that all of the following are
equivalent:

```sql
select * from p where color = :color
select * from p where color = $color
select * from p where color = @color
```

### Anonymous and positional parameters

Since anonymous parameters are indexed from 1 onwards just like Lua arrays,
they're simple to use

```lua
db:query("select * from p where color = ?", {'Red'})
```

Positional parameters can be supplied by prefixing the `?` with a number:

```lua
db:query("select * from p where weight = ?2 color = ?1", {'Red', 12})
```

NB. Even though it is entirely possible to mix named, anonymous and positional
parameters in the same query, I wouldn't recommend trying to do that unless you
really want to confuse your readers.

For a small number of parameters, it is a bit inconvenient to always write
the extra braces, so Clutch supports binding anonymous and positional
parameters also as varargs:

```lua
db:query("select * from p where weight < ? and color = ?", 15, 'Red')
```

### Interpolated parameters

For added convenience, if there are no extra arguments after the query, Clutch
tries to look up for local variables with the same name as the query parameters.
This comes in handy when you have e.g. wrapper functions around common queries:

```lua
function getPartByPnum(pnum)
    return db:queryone('select * from p where pnum = $pnum')
do
```

NB. This functionality is strictly limited to the locals and arguments of
currently executing function. It cannot be used to interpolate global
variables, nor variables in the function's closure.

## Issuing updates to the database

For writing into the database, whether it be DDL statements, inserts or updates,
Clutch offers a single method `update()`. It checks that the query it ran
returns no results and throws an error otherwise.

For example:

```lua
local dbsetup = {
    [[
        CREATE TABLE p (
            pnum INTEGER NOT NULL PRIMARY KEY,
            pname TEXT NOT NULL,
            color TEXT NOT NULL,
            weight REAL NOT NULL,
            city TEXT NOT NULL,
            UNIQUE (pname, color, city)
        )
    ]],
    "INSERT INTO p VALUES (1, 'Nut', 'Red', 12, 'London')",
    "INSERT INTO p VALUES (2, 'Bolt', 'Green', 17, 'Paris')",
    "INSERT INTO p VALUES (3, 'Screw', 'Blue', 17, 'Oslo')",
    "INSERT INTO p VALUES (4, 'Screw', 'Red', 14, 'London')",
    "INSERT INTO p VALUES (5, 'Cam', 'Blue', 12, 'Paris')",
    "INSERT INTO p VALUES (6, 'Cog', 'Red', 19, 'London')",
}
for _, query in ipairs(dbsetup) do
    self.db:update(query)
end
```

`update()` uses the same code for preparing queries as `query()` and its
friends so you can use all the same mechanisms for parameter binding.

## Error handling

Whenever the underlying sqlite3 API returns anything else than success for
a call, Clutch throws an error with the sqlite3 error message as message.

## Missing values, NULLs and nils

There any many ways to handle mapping SQL NULLs into host language and vice
versa. Clutch takes the approach that whenever `nil` would mean "missing value"
in Lua, it is mapped to SQL `NULL`.

This means that missing parameter values in all different methods of parameter
binding are converted to SQL NULLs. So, if you omit or misspell a table key,
misspell an interpolated variable name or omit some of the arguments from a
vararg call, a `NULL` is bound to the parameter in question.

Also if an SQL query returns `NULL` for some column in a row, the resulting
table won't have a value for a key with that name.

The result of all this is that any row returned by a query is valid parameter
mapping for a corresponding insert or update. It also means that you don't
have to write awkward code mapping special NULL values to nil and vice versa in
your sqlite3 interface code.

As a sidenote, if you follow the practice of using `NOT NULL` by default for
SQL table columns, database constraint checks will catch the aforementioned
errors. And it does so more reliably than any library code could.

## Building, installing and running tests

Clutch is distributed as a Luarock, so the easiest way to install it is:
```sh
$ luarocks install clutch
```

The Sqlite3 library is always dynamically linked, which means that you have to
have it installed somewhere where the Lua dynamic loader can find it.

Additionally, since Clutch consists of a single C file you can link it
statically into your custom Lua application by including `clutch.c` into your
project and calling `luaopen_clutch()` from your `main()`, for example.

Clutch uses luarocks "builtin" build mechanism, so you can also build it from
source easily:
```sh
$ luarocks make
```

To run Clutch unit tests you need `luaunit` rock. The test can be run with:
```sh
$ lua test.lua
```
