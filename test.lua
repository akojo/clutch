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
