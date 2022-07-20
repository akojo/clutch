rockspec_format = "3.0"
package = "clutch"
version = "1.3.2-2"
source = {
   url = "git://github.com/akojo/clutch",
   tag = "v1.3.2"
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
external_dependencies = {
   LIBSQLITE3 = {
      header = "sqlite3.h"
   }
}
build = {
   type = "builtin",
   modules = {
      clutch = {
         incdirs = {
            "$(LIBSQLITE3_INCDIR)"
         },
         libdirs = {
            "$(LIBSQLITE3_LIBDIR)"
         },
         libraries = {
            "sqlite3"
         },
         sources = "clutch.c"
      }
   }
}
test_dependencies = {
   "luaunit"
}
