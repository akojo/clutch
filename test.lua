local luaunit = require 'luaunit'
local clutch = require 'clutch'

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

TestClutch = {}

function TestClutch:setup()
    self.db = clutch.open("")
    for _, sql in ipairs(dbsetup) do
        self.db:update(sql)
    end
end

function TestClutch:teardown()
    self.db:close()
end

function TestClutch:testSimpleQueryReturnsCorrectNumberOfRows()
    assertResultCount(self.db:query('select * from p'), 6)
end

function TestClutch:testResultsAreBoundToCorrectKeys()
    assertSingleResult(
        self.db:query('select * from p where pnum = 1'),
        {pnum = 1, pname = 'Nut', color = 'Red', weight = 12.0, city = 'London'})
end

function TestClutch:testAnonymousParameterBinding()
    assertSingleResult(
        self.db:query('select pname from p where pnum = ?', {1}),
        {pname = 'Nut'})
end

function TestClutch:testAnonymousParameterBindingWithMultipleParameters()
    assertResultCount(
        self.db:query('select * from p where pnum > ? and pnum < ?', {1, 4}),
        2)
end

function TestClutch:testAnonymousParameterBindingWithVarargs()
    assertResultCount(
        self.db:query('select * from p where pnum > ? and pnum < ?', 1, 4),
        2)
end

function TestClutch:testVarargBindingIgnoresExtraArguments()
    assertResultCount(
        self.db:query('select * from p where pnum > ? and pnum < ?', 1, 4, 2, 3),
        2)
end

function TestClutch:testPositionalParameterBinding()
    assertSingleResult(
        self.db:query('select pname from p where pnum = ?1', {1}),
        {pname = 'Nut'})
end

function TestClutch:testPositionalParameterBindingWithMultipleParameters()
    assertResultCount(
        self.db:query('select * from p where pnum > ?2 and pnum < ?1', {4, 1}),
        2)
end

function TestClutch:testNamedParameterBinding()
    assertSingleResult(
        self.db:query('select pname from p where pnum = :pnum', {pnum = 1}),
        {pname = 'Nut'})
end

function TestClutch:testNamedParameterBindingWithAt()
    assertSingleResult(
        self.db:query('select pname from p where pnum = @pnum', {pnum = 1}),
        {pname = 'Nut'})
end

function TestClutch:testNamedParameterBindingWithDollar()
    assertSingleResult(
        self.db:query('select pname from p where pnum = $pnum', {pnum = 1}),
        {pname = 'Nut'})
end

function TestClutch:testNamedParameterBindingWithMultipleParameters()
    assertSingleResult(
        self.db:query('select count(1) from p where pnum > :min and pnum < :max', {min = 1, max = 4}),
        {count = 2})
end

function TestClutch:testInterpolatedParameterBindingWithLocal()
    local pnum = 1
    assertSingleResult(
        self.db:query('select pname from p where pnum = $pnum'),
        {pname = 'Nut'})
end

function TestClutch:testInterpolatedParameterBindingWithArgument()
    local f = function(pnum)
        assertSingleResult(
            self.db:query('select pname from p where pnum = $pnum'),
            {pname = 'Nut'})
    end
    f(1)
end

function TestClutch:testInterpolatedParameterBindingWithClosure()
    local pnum = 1
    local f = function()
        assertSingleResult(
            self.db:query('select pname from p where pnum = $pnum'),
            {pname = 'Nut'})
    end
    f()
end

function TestClutch:testInterpolatedParameterBindingWithGlobal()
    globalNum = 1
    local f = function()
        assertSingleResult(
            self.db:query('select pname from p where pnum = $globalNum'),
            {pname = 'Nut'})
    end
    f()
end

function TestClutch:testStringParameterBinding()
    assertSingleResult(
        self.db:query('select pnum from p where color = $color', {color = 'Green'}),
        {pnum = 2})
end

function TestClutch:testDoubleParameterBinding()
    assertSingleResult(
        self.db:query('select pname from p where weight = @weight', {weight = 19.0}),
        {pname = 'Cog'})
end

function TestClutch:testUpdateWithParameters()
    self.db:update('insert into p values (:pnum, :pname, :color, :weight, :city)',
        {pnum = 7, pname = 'Washer', color = 'Grey', weight = 5.0, city = 'Helsinki'})
    assertSingleResult(
        self.db:query('select pname from p where pnum = 7'),
        { pname = 'Washer'})
end

function TestClutch:testParametersAreQuotedProperlyInUpdate()
    self.db:update("insert into p values (:pnum, :pname, :color, :weight, :city)",
        {pnum = 7, pname = "'); delete from p; -- ", color = 'Grey', weight = 5.0, city = 'Helsinki'})
    assertSingleResult(
        self.db:query('select color from p where pnum = 7'),
        { pname = 'Grey'})
end

function TestClutch:testParametersAreQuotedProperyInQuery()
    assertResultCount(
        self.db:query('select 1 from p where pname = :pname', {pname = "' or '1'='1' -- "}),
        0
    )
end

function TestClutch:testParametersAreInterpolatedProperyInQuery()
    local pname = "' or '1'='1' -- "
    assertResultCount(self.db:query('select 1 from p where pname = :pname'), 0)
end

function TestClutch:testSupportsBooleanParameter()
    self.db:update('CREATE TABLE t (pnum INTEGER PRIMARY KEY, avail INTEGER NOT NULL)')
    self.db:update('insert into t values (?, ?)', 1, true)
    assertSingleResult(
        self.db:query('select avail from t where pnum = 1'),
        { avail = 1})
end

function TestClutch:testQueryOneReturnsSingleResultAsTable()
    luaunit.assertItemsEquals(
        self.db:queryone('select pname from p where pnum = ?', 1),
        {pname = 'Nut'})
end

function TestClutch:testQueryAllReturnsAllResultsInAnArray()
    local results = self.db:queryall('select pnum from p order by pnum asc')
    luaunit.assertEquals(#results, 6)
    for i = 1, 6 do
        luaunit.assertItemsEquals(results[i], {pnum = i})
    end
end

function TestClutch:testQueryAllReturnsEmptyTableForNoResults()
    local results = self.db:queryall('select pnum from p where pnum = -1')
    luaunit.assertItemsEquals(results, {})
end

function TestClutch:testInsertReturnsOneForNewRow()
    local n = self.db:update("insert into p values (7, 'Washer', 'Grey', 5, 'Helsinki')")
    luaunit.assertEquals(n, 1)
end

function TestClutch:testUpdateReturnsNumberOfModifiedRows()
    local n = self.db:update("update p set weight = weight + 1 where color = 'Red'")
    luaunit.assertEquals(n, 3)
end

function TestClutch:testUpdateWithNoMatchingRowsReturnsZero()
    local n = self.db:update("update p set weight = weight + 1 where color = 'Pink'")
    luaunit.assertEquals(n, 0)
end

function TestClutch:testDeleteReturnsNumberOfModifiedRows()
    local n = self.db:update("delete from p where city = 'Paris'")
    luaunit.assertEquals(n, 2)
end

function TestClutch:testDeleteWithNoMatchingRowsReturnsZero()
    local n = self.db:update("delete from p where city = 'Vienna'")
    luaunit.assertEquals(n, 0)
end

function TestClutch:testPreparedStatement()
    local stmt = self.db:prepare("select city from p where pnum = :pnum")
    local iter = stmt:query({pnum = 1})
    luaunit.assertItemsEquals(iter(), {city = "London"})
end

function TestClutch:testPreparedStatementCanBeRebound()
    local stmt = self.db:prepare("select pnum, city from p where pnum = :pnum")
    for pnum, city in ipairs({"London", "Paris"}) do
        local iter = stmt:query({pnum = pnum})
        luaunit.assertItemsEquals(iter(), {pnum = pnum, city = city})
    end
end

function TestClutch:testPreparedStatementIterReturnsNilAfterLastResult()
    local stmt = self.db:prepare("select city from p where pnum = :pnum")
    local iter = stmt:query({pnum = 2})
    luaunit.assertItemsEquals(iter(), {city = "Paris"})
    luaunit.assertNil(iter())
end

function TestClutch:testPreparedStatementIterReturnsNilForNoResults()
    local stmt = self.db:prepare("select city from p where pnum = :pnum")
    local iter = stmt:query({pnum = 100})
    luaunit.assertNil(iter())
end

function TestClutch:testPreparedStatementIterWorksWithTableArguments()
    local stmt = self.db:prepare("select city from p where pnum = ?")
    local iter = stmt:query({3})
    luaunit.assertItemsEquals(iter(), {city = "Oslo"})
end

function TestClutch:testPreparedStatementIterWorksWithVarargs()
    local stmt = self.db:prepare("select city from p where pnum = ?")
    local iter = stmt:query(3)
    luaunit.assertItemsEquals(iter(), {city = "Oslo"})
end

function TestClutch:testPreparedStatementReturnsOneResult()
    local stmt = self.db:prepare("select city from p where pnum = :pnum")
    luaunit.assertItemsEquals(stmt:queryone({pnum = 1}), {city = "London"})
end

function TestClutch:testPreparedStatementOneFailsWithNoResults()
    local stmt = self.db:prepare("select city from p where pnum = :pnum")
    luaunit.assertErrorMsgContains("no results",
        function() stmt:queryone({pnum = 100}) end)
end

function TestClutch:testPreparedStatementOneFailsWithTooManyResults()
    local stmt = self.db:prepare("select city from p where color = :color")
    luaunit.assertErrorMsgContains("too many results",
        function() stmt:queryone({color = "Red"}) end)
end

function TestClutch:testPreparedStatementOneWorksWithTableArguments()
    local stmt = self.db:prepare("select city from p where pnum = ?")
    luaunit.assertItemsEquals(stmt:queryone({4}), {city = "London"})
end

function TestClutch:testPreparedStatementOneWorksWithVarargs()
    local stmt = self.db:prepare("select city from p where pnum = ?")
    luaunit.assertItemsEquals(stmt:queryone(4), {city = "London"})
end

function TestClutch:testPreparedStatementReturnsAllResults()
    local stmt = self.db:prepare("select pname from p where color = :color")
    local results = stmt:queryall({color = "Red"})
    for i, name in ipairs({"Nut", "Screw", "Cog"}) do
        luaunit.assertItemsEquals(results[i], {pname = name})
    end
end

function TestClutch:testPreparedStatementAllResultsEmptyTableForNoResults()
    local stmt = self.db:prepare("select pname from p where color = :color")
    luaunit.assertEquals(stmt:queryall({color = "Pink"}), {})
end

function TestClutch:testPreparedStatementAllWorksWithTableArguments()
    local stmt = self.db:prepare("select city from p where pnum = ?")
    luaunit.assertItemsEquals(stmt:queryall({5})[1], {city = "Paris"})
end

function TestClutch:testPreparedStatementAllWorksWithVarargs()
    local stmt = self.db:prepare("select city from p where pnum = ?")
    luaunit.assertItemsEquals(stmt:queryall(5)[1], {city = "Paris"})
end

function TestClutch:testPreparedStatementUpdate()
    local stmt = self.db:prepare("insert into p values (?, ?, ?, ?, ?)")
    stmt:update({7, "Washer", "Grey", 5.0, "Helsinki"})

    local result = self.db:queryone("select pname from p where pnum = 7")
    luaunit.assertEquals(result.pname, "Washer")
end

function TestClutch:testUpdateInTransactionSucceeds()
    self.db:transaction(function (t)
        t:update("insert into p values (7, 'Washer', 'Grey', 5, 'Helsinki')")
    end)
    luaunit.assertItemsEquals(
        self.db:queryone('select city from p where pnum = 7'),
        {city = "Helsinki"}
    )
end

function TestClutch:testTransactionReturnsTheValuesFromTransactionFunction()
    local success, result = self.db:transaction(function (t)
        return t:update("insert into p values (7, 'Washer', 'Grey', 5, 'Helsinki')")
    end)
    luaunit.assertTrue(success)
    luaunit.assertEquals(result, 1)
end

function TestClutch:testTransactionRollsBackInCaseOfConstrainFailure()
    local success, result = self.db:transaction(function (t)
        t:update("insert into p values (7, 'Washer', 'Grey', 5, 'Helsinki')")
        t:update("insert into p values (7, 'Washer', 'Grey', 5, 'Helsinki')")
    end)
    luaunit.assertFalse(success)
    luaunit.assertStrContains(result, "UNIQUE constraint failed")
    luaunit.assertEquals(#self.db:queryall("select * from p where pnum = 7"), 0)
end

function TestClutch:testTransactionRollsBackInCaseOfLuaError()
    local success, result = self.db:transaction(function (t)
        t:update("insert into p values (7, 'Washer', 'Grey', 5, 'Helsinki')")
        error("Lua error")
    end)
    luaunit.assertFalse(success)
    luaunit.assertStrContains(result, "Lua error")
    luaunit.assertEquals(#self.db:queryall("select * from p where pnum = 7"), 0)
end

function TestClutch:testNestedTransactionWritesToDatabase()
    self.db:transaction(function (t)
        t:update("insert into p values (7, 'Washer', 'Grey', 5, 'Helsinki')")
        t:transaction(function (t2)
            t2:update("insert into p values (8, 'Washer', 'Black', 7, 'Helsinki')")
        end)
    end)
    luaunit.assertItemsEquals(
        #self.db:queryall("select city from p where city = 'Helsinki'"), 2)
end

function TestClutch:testErrorInNestedTransactionRollsBackOnlyInnerTransaction()
    local success, result = self.db:transaction(function (t)
        t:update("insert into p values (7, 'Washer', 'Grey', 5, 'Helsinki')")
        return t:transaction(function (t2)
            t2:update("insert into p values (8, 'Washer', 'Black', 7, 'Helsinki')")
            error("Inner transaction")
        end)
    end)
    luaunit.assertTrue(success)
    luaunit.assertItemsEquals(
        #self.db:queryall("select city from p where city = 'Helsinki'"), 1)
end

function TestClutch:testErrorInOuterTransactionRollsBackAlsoInnerTransaction()
    local success, result = self.db:transaction(function (t)
        t:transaction(function (t2)
            t2:update("insert into p values (8, 'Washer', 'Black', 7, 'Helsinki')")
        end)
        return t:update("insert into p values (8, 'Washer', 'Grey', 5, 'Helsinki')")
    end)
    luaunit.assertFalse(success)
    luaunit.assertStrContains(result, "UNIQUE constraint failed")
    luaunit.assertItemsEquals(
        #self.db:queryall("select city from p where city = 'Helsinki'"), 0)
end

function TestClutch:testQueryOneReportsErrorWithTooManyResults()
    luaunit.assertErrorMsgContains(
        "too many results",
        function() self.db:queryone('select * from p') end)
end

function TestClutch:testQueryOneReportsErrorWithZeroResults()
    luaunit.assertErrorMsgContains(
        "no results",
        function() self.db:queryone('select * from p where pnum = -1') end)
end

function TestClutch:testCanUseNilAsAnonymousParameter()
    luaunit.assertErrorMsgContains("NOT NULL constraint failed: p.city", function ()
        self.db:update("insert into p values (7, 'Washer', 'Grey', 5, ?)", nil)
    end)
end

function TestClutch:testCanUseNilAsNamedParameter()
    luaunit.assertErrorMsgContains("NOT NULL constraint failed: p.city", function ()
        self.db:update("insert into p values (7, 'Washer', 'Grey', 5, :city)", {city = nil})
    end)
end

function TestClutch:testMissingAnonymousParametersAreTreatedAsNil()
    luaunit.assertErrorMsgContains("NOT NULL constraint failed: p.city", function (city)
        self.db:update("insert into p values (7, 'Washer', 'Grey', ?, ?)", 5.0)
    end, nil)
end

function TestClutch:testMissingInterpolatedVariableIsTreatedAsNil()
    luaunit.assertErrorMsgContains("NOT NULL constraint failed: p.city", function ()
        self.db:update("insert into p values (7, 'Washer', 'Grey', 5, :city)")
    end)
end

function TestClutch:testSQLIntegrityViolationIsReportedAsError()
    luaunit.assertErrorMsgContains("UNIQUE constraint failed", function ()
        self.db:update('insert into p values (:pnum, :pname, :color, :weight, :city)',
            {pnum = 1, pname = 'Washer', color = 'Grey', weight = 5.0, city = 'Helsinki'})
    end)
end

function TestClutch:testSQLSyntaxErrorIsReportedAsError()
    luaunit.assertErrorMsgContains("syntax error", function ()
        self.db:query('insert values')
    end)
end

function assertResultCount(iter, count)
    local i = 0
    for _ in iter do
        i = i + 1
    end
    luaunit.assertEquals(i, count)
end

function assertSingleResult(iter, expected)
    luaunit.assertItemsEquals(iter(), expected)
end

os.exit(luaunit.LuaUnit.run())
