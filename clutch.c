#include <lauxlib.h>
#include <lua.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int open_db(lua_State *L);
static int query(lua_State *L);
static int query_one(lua_State *L);
static int query_all(lua_State *L);
static int update(lua_State *L);
static int tostring(lua_State *L);
static int close_db(lua_State *L);

static int iter(lua_State *L);
static int step(lua_State *L, sqlite3_stmt **stmt);
static void handle_row(lua_State *L, sqlite3_stmt *stmt);
static int close_stmt(lua_State *L);

static void init_db_metatable(lua_State *L);
static void init_statement_metatable(lua_State *L);

static sqlite3_stmt **prepare(lua_State *L, sqlite3 *db);
static int bind_params(lua_State *L, sqlite3_stmt *stmt);
static int bind_varargs(lua_State *L, int nargs, sqlite3_stmt *stmt);
static int bind_locals(lua_State *L, sqlite3_stmt *stmt);
static int bind_one_param(lua_State *L, sqlite3_stmt *stmt, int index);
static int is_named_parameter(const char *name);
static void find_local(lua_State *L, const char *name);

static void close_sqlite(sqlite3 **db);
static void close_sqlite_stmt(sqlite3_stmt **stmt);

static const struct luaL_Reg clutch_funcs[] = {{"open", open_db}, {NULL, NULL}};

static const struct luaL_Reg clutch_db_methods[] = {
    {"query", query},        {"queryone", query_one},
    {"queryall", query_all}, {"update", update},
    {"close", close_db},     {"__tostring", tostring},
    {"__gc", close_db},      {NULL, NULL}};

static const struct luaL_Reg clutch_stmt_methods[] = {{"__gc", close_stmt},
                                                      {NULL, NULL}};

int luaopen_clutch(lua_State *L) {
  init_db_metatable(L);
  init_statement_metatable(L);

  luaL_newlib(L, clutch_funcs);
  return 1;
}

static void init_db_metatable(lua_State *L) {
  luaL_newmetatable(L, "sqlite3.db");

  lua_pushvalue(L, -1);
  lua_setfield(L, -2, "__index");

  luaL_setfuncs(L, clutch_db_methods, 0);
}

static void init_statement_metatable(lua_State *L) {
  luaL_newmetatable(L, "sqlite3.stmt");

  lua_pushvalue(L, -1);
  lua_setfield(L, -2, "__index");

  luaL_setfuncs(L, clutch_stmt_methods, 0);
}

static int open_db(lua_State *L) {
  const char *filename = luaL_checkstring(L, 1);

  sqlite3 **db = (sqlite3 **)lua_newuserdata(L, sizeof(sqlite3 *));
  *db = NULL;

  luaL_getmetatable(L, "sqlite3.db");
  lua_setmetatable(L, -2);

  if (sqlite3_open(filename, db) != SQLITE_OK) {
    lua_pushfstring(L, "%s: %s", filename, sqlite3_errmsg(*db));
    close_sqlite(db);
    return lua_error(L);
  }
  return 1;
}

static void close_sqlite(sqlite3 **db) {
  if (*db) {
    sqlite3_close_v2(*db);
    *db = NULL;
  }
}

static int query(lua_State *L) {
  prepare(L, *(sqlite3 **)luaL_checkudata(L, 1, "sqlite3.db"));
  lua_pushcclosure(L, iter, 1);
  return 1;
}

static sqlite3_stmt **prepare(lua_State *L, sqlite3 *db) {
  const char *sql = luaL_checkstring(L, 2);

  sqlite3_stmt **stmt =
      (sqlite3_stmt **)lua_newuserdata(L, sizeof(sqlite3_stmt **));
  *stmt = NULL;

  luaL_getmetatable(L, "sqlite3.stmt");
  lua_setmetatable(L, -2);

  lua_insert(L, 3);

  int status = sqlite3_prepare_v2(db, sql, strlen(sql), stmt, NULL);

  if (status == SQLITE_OK) {
    int top = lua_gettop(L);
    if (top < 4)
      status = bind_locals(L, *stmt);
    else if (lua_istable(L, 4))
      status = bind_params(L, *stmt);
    else
      status = bind_varargs(L, top - 3, *stmt);
  }

  if (status != SQLITE_OK) {
    close_sqlite_stmt(stmt);
    luaL_error(L, "%s", sqlite3_errmsg(db));
  }

  return stmt;
}

static int bind_params(lua_State *L, sqlite3_stmt *stmt) {
  int count = sqlite3_bind_parameter_count(stmt);
  int status = SQLITE_OK;

  for (int i = 1; i <= count; ++i) {
    const char *name = sqlite3_bind_parameter_name(stmt, i);
    if (!name || name[0] == '?') {
#if LUA_VERSION_NUM >= 503
      lua_geti(L, -1, i);
#else
      lua_pushinteger(L, i);
      lua_gettable(L, -2);
#endif
    } else {
      lua_getfield(L, -1, name + 1);
    }
    status = bind_one_param(L, stmt, i);
    if (status != SQLITE_OK) break;
  }
  lua_pop(L, 1);
  return status;
}

static int bind_one_param(lua_State *L, sqlite3_stmt *stmt, int index) {
  int status = SQLITE_OK;

  if (lua_isstring(L, -1)) {
    size_t len;
    const char *text = lua_tolstring(L, -1, &len);
    status = sqlite3_bind_text(stmt, index, text, len, SQLITE_TRANSIENT);
#if LUA_VERSION_NUM >= 503
  } else if (lua_isinteger(L, -1)) {
    status = sqlite3_bind_int64(stmt, index, lua_tointeger(L, -1));
#endif
  } else if (lua_isnumber(L, -1)) {
    status = sqlite3_bind_double(stmt, index, lua_tonumber(L, -1));
  } else if (lua_isnil(L, -1)) {
    status = sqlite3_bind_null(stmt, index);
  } else {
    return luaL_error(L, "unsupported lua type '%s' at position %d",
                      lua_typename(L, lua_type(L, -1)), index);
  }

  lua_pop(L, 1);
  return status;
}

static int bind_varargs(lua_State *L, int nparams, sqlite3_stmt *stmt) {
  int count = sqlite3_bind_parameter_count(stmt);

  if (nparams < count) {
    lua_settop(L, lua_gettop(L) + (count - nparams));
  } else if (count < nparams) {
    lua_settop(L, lua_gettop(L) - (nparams - count));
  }

  int status = SQLITE_OK;
  while (count > 0) {
    status = bind_one_param(L, stmt, count--);
    if (status != SQLITE_OK) break;
  }
  return status;
}

static int bind_locals(lua_State *L, sqlite3_stmt *stmt) {
  int count = sqlite3_bind_parameter_count(stmt);
  int status = SQLITE_OK;

  for (int i = 1; i <= count; ++i) {
    const char *name = sqlite3_bind_parameter_name(stmt, i);
    if (!name || !is_named_parameter(name)) {
      return luaL_error(L, "anonymous and numbered parameters not supported");
    }

    find_local(L, name + 1);
    status = bind_one_param(L, stmt, i);
    if (status != SQLITE_OK) break;
  }
  return status;
}

static int is_named_parameter(const char *name) {
  return name[0] == ':' || name[0] == '@' || name[0] == '$';
}

static void find_local(lua_State *L, const char *name) {
  lua_Debug debug;
  lua_getstack(L, 1, &debug);

  int index = 1;
  const char *lname;
  while ((lname = lua_getlocal(L, &debug, index++))) {
    if (!strcmp(name, lname)) return;
    lua_pop(L, 1);
  }
  lua_pushnil(L);
}

static int query_one(lua_State *L) {
  sqlite3 *db = *(sqlite3 **)luaL_checkudata(L, 1, "sqlite3.db");
  sqlite3_stmt **stmt = prepare(L, db);

  if (step(L, stmt) == 0) luaL_error(L, "no results");

  if (step(L, stmt) != 0) {
    close_sqlite_stmt(stmt);
    luaL_error(L, "too many results");
  }
  return 1;
}

static int query_all(lua_State *L) {
  sqlite3 *db = *(sqlite3 **)luaL_checkudata(L, 1, "sqlite3.db");
  sqlite3_stmt **stmt = prepare(L, db);

  lua_newtable(L);
  for (int i = 1; step(L, stmt) == 1; ++i) lua_rawseti(L, -2, i);
  return 1;
}

static int update(lua_State *L) {
  sqlite3 *db = *(sqlite3 **)luaL_checkudata(L, 1, "sqlite3.db");
  sqlite3_stmt **stmt = prepare(L, db);

  int status = sqlite3_step(*stmt);
  if (status != SQLITE_DONE) {
    return luaL_error(L, "%s", sqlite3_errmsg(db));
  }
  lua_pushinteger(L, sqlite3_changes(db));

  close_sqlite_stmt(stmt);

  return 1;
}

static int tostring(lua_State *L) {
  sqlite3 **db = (sqlite3 **)luaL_checkudata(L, 1, "sqlite3.db");
  const char *name = sqlite3_db_filename(*db, "main");
  lua_pushfstring(L, "sqlite3: %s", name);
  return 1;
}

static int close_db(lua_State *L) {
  close_sqlite((sqlite3 **)luaL_checkudata(L, 1, "sqlite3.db"));
  return 0;
}

static int iter(lua_State *L) {
  sqlite3_stmt **stmt = (sqlite3_stmt **)lua_touserdata(L, lua_upvalueindex(1));
  return step(L, stmt);
}

static int step(lua_State *L, sqlite3_stmt **stmt) {
  int status = sqlite3_step(*stmt);
  if (status != SQLITE_ROW) {
    close_sqlite_stmt(stmt);
    if (status != SQLITE_DONE) luaL_error(L, "%s", sqlite3_errstr(status));
    return 0;
  }

  handle_row(L, *stmt);
  return 1;
}

static void handle_row(lua_State *L, sqlite3_stmt *stmt) {
  int count = sqlite3_data_count(stmt);

  lua_createtable(L, 0, count);
  for (int i = 0; i < count; ++i) {
    lua_pushstring(L, sqlite3_column_name(stmt, i));
    switch (sqlite3_column_type(stmt, i)) {
      case SQLITE_INTEGER:
        lua_pushinteger(L, sqlite3_column_int64(stmt, i));
        break;
      case SQLITE_FLOAT:
        lua_pushnumber(L, sqlite3_column_double(stmt, i));
        break;
      case SQLITE_TEXT:
      case SQLITE_BLOB:
        lua_pushlstring(L, (const char *)sqlite3_column_blob(stmt, i),
                        sqlite3_column_bytes(stmt, i));
        break;
      case SQLITE_NULL:
      default:
        lua_pushnil(L);
        break;
    }
    lua_rawset(L, -3);
  }
}

static int close_stmt(lua_State *L) {
  close_sqlite_stmt((sqlite3_stmt **)luaL_checkudata(L, 1, "sqlite3.stmt"));
  return 0;
}

static void close_sqlite_stmt(sqlite3_stmt **stmt) {
  if (*stmt) {
    sqlite3_finalize(*stmt);
    *stmt = NULL;
  }
}
