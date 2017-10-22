package = "clutch"
version = "1.2-2"
source = {
    url = "git://github.com/akojo/clutch",
    tag = "v1.2.2"
}
description = {
    summary = "A simple API for sqlite3",
    detailed = [[
        clutch is an API for sqlite3 designed for simplicity and ease of use.
        It hides all the complexities of binding parameters to SQL queries and
        parsing the results returned by queries, allowing you to concentrate
        on writing application code.
    ]],
    homepage = "https://github.com/akojo/clutch",
    license = "MIT"
}
dependencies = {
    "lua >= 5.2"
}
build = {
    type = "builtin",
    modules = {
        clutch = {
            sources = "clutch.c",
            libraries = {"sqlite3"},
            incdirs = {"$(LIBSQLITE3_INCDIR)"},
            libdirs = {"$(LIBSQLITE3_LIBDIR)"}
        }
    }
}
external_dependencies = {
   LIBSQLITE3 = {
      header = "sqlite3.h"
   }
}
