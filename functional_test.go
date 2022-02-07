package shard_index

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zyguan/sqlz"
)

func mustSetupDB(t *testing.T, schema string, params map[string]string) *sql.DB {
	func() {
		db, err := sql.Open("mysql", dsn("", nil))
		require.NoError(t, err)
		defer db.Close()
		_, err = db.Exec(fmt.Sprintf("create database if not exists `%s`", schema))
		require.NoError(t, err)
	}()
	db, err := sql.Open("mysql", dsn(schema, params))
	require.NoError(t, err)
	for _, ddl := range []string{
		"drop table if exists test3, test33, test333, test4, test44, test5, test55, test6, test66, test7, test77, test8, test88, testx, testy, testz",
		"create table test3(id int primary key clustered, a int, b int, unique key uk_expr((tidb_shard(a)),a))",
		"create table test33(id int primary key clustered, a int, b int, unique key a(a))",
		"create table test333(id int primary key clustered, a int, b int, unique key uk_expr((vitess_hash(a)%256),a))",
		"create table test4(id int primary key clustered, a int, b int, unique key uk_expr((tidb_shard(a)),a),unique key uk_b_expr((tidb_shard(b)),b))",
		"create table test44(id int primary key clustered, a int, b int, unique key uk_expr(a),unique key uk_b_expr(b))",
		"create table test5(id int primary key clustered, a int, b int, unique key uk_expr((tidb_shard(a)),a,b))",
		"create table test55(id int primary key clustered, a int, b int, unique key a(a,b))",
		"create table test6(id int primary key clustered, a int, b int, c int, unique key uk_expr((tidb_shard(a)), a))",
		"create table test66(id int primary key clustered, a int, b int, c int, unique key a(a))",
		"create table test7(id int, a int, b int, unique key uk_expr((tidb_shard(a)),a)) PARTITION BY RANGE (a) " +
			"(PARTITION p0 VALUES LESS THAN (200), PARTITION p1 VALUES LESS THAN (400)," +
			"PARTITION p2 VALUES LESS THAN (600), PARTITION p3 VALUES LESS THAN (800)," +
			"PARTITION p4 VALUES LESS THAN MAXVALUE)",
		"create table test77(id int, a int, b int, unique key uk_expr(a)) PARTITION BY RANGE (a) " +
			"(PARTITION p0 VALUES LESS THAN (200), PARTITION p1 VALUES LESS THAN (400)," +
			"PARTITION p2 VALUES LESS THAN (600), PARTITION p3 VALUES LESS THAN (800)," +
			"PARTITION p4 VALUES LESS THAN MAXVALUE)",
		"create table test8(id int, a int primary key clustered , b int, unique key uk_expr((tidb_shard(a)),a))",
		"create table test88(id int, a int primary key clustered , b int, unique key uk_expr(a))",
		"create table testx(id int primary key clustered, a int, b int, unique key a(a))",
		"create table testy(id int primary key clustered, a int, b int, unique key uk_expr((tidb_shard(b)),a))",
		"create table testz(id int primary key clustered, a int, b int, unique key uk_expr((tidb_shard(a+b)),a))",
	} {
		_, err := db.Exec(ddl)
		require.NoError(t, err, "exec "+ddl)
	}

	for _, table := range []string{
		"test3", "test33", "test333", "test4", "test44", "test5", "test55", "test7", "test77", "test8", "test88", "testy", "testz",
	} {
		bulk := sqlz.BulkInsert{
			Prefix: "insert into " + table + " (id, a, b) values ",
			Row:    "(?, ?, ?)",
		}
		bulk.Init(db, 100)
		for i := 0; i < 1000; i++ {
			require.NoError(t, bulk.Next(context.TODO(), i, i, i))
		}
		require.NoError(t, bulk.Done(context.TODO()))
	}

	bulk := sqlz.BulkInsert{
		Prefix: "insert into test6 (id, a, b, c) values ",
		Row:    "(?, ?, ?, ?)",
	}
	bulk.Init(db, 100)
	for i := 0; i < 1000; i++ {
		require.NoError(t, bulk.Next(context.TODO(), i, i, i, i))
	}
	require.NoError(t, bulk.Done(context.TODO()))

	_, err = db.Exec("insert into test66 select * from test6")
	require.NoError(t, err)

	_, err = db.Exec("insert into testx select * from test33 where a % 37 = 0")
	require.NoError(t, err)

	return db
}

func SetupReverseIndexDB(t *testing.T, schema string, params map[string]string) *sql.DB {
	func() {
		db, err := sql.Open("mysql", dsn("", nil))
		require.NoError(t, err)
		defer db.Close()
		_, err = db.Exec(fmt.Sprintf("create database if not exists `%s`", schema))
		require.NoError(t, err)
	}()
	db, err := sql.Open("mysql", dsn(schema, params))
	require.NoError(t, err)
	for _, ddl := range []string{
		"drop table if exists testreverse3, testreverse33",
		"create table testreverse3(id int primary key clustered, a int, b int, unique key uk_expr((tidb_shard(a)) , a desc))",
		"create table testreverse33(id int primary key clustered, a int, b int, unique key uk_expr((tidb_shard(a)) desc , a))",
	} {
		_, err := db.Exec(ddl)
		require.NoError(t, err, "exec "+ddl)
	}

	for _, table := range []string{
		"testreverse3", "testreverse33",
	} {
		bulk := sqlz.BulkInsert{
			Prefix: "insert into " + table + " (id, a, b) values ",
			Row:    "(?, ?, ?)",
		}
		bulk.Init(db, 100)
		for i := 0; i < 1000; i++ {
			require.NoError(t, bulk.Next(context.TODO(), i, i, i))
		}
		require.NoError(t, bulk.Done(context.TODO()))
	}

	return db
}

func SetupPrepareDB(t *testing.T, schema string, params map[string]string) *sql.DB {
	func() {
		db, err := sql.Open("mysql", dsn("", nil))
		require.NoError(t, err)
		defer db.Close()
		_, err = db.Exec(fmt.Sprintf("create database if not exists `%s`", schema))
		require.NoError(t, err)
	}()
	db, err := sql.Open("mysql", dsn(schema, params))
	require.NoError(t, err)
	for _, ddl := range []string{
		"drop table if exists test3, test33",
		"create table test3(id int primary key clustered, a int, b int, unique key uk_expr((tidb_shard(a)),a))",
		"create table test33(id int primary key clustered, a int, b int, unique key a(a))",
	} {
		_, err := db.Exec(ddl)
		require.NoError(t, err, "exec "+ddl)
	}

	for _, table := range []string{
		"test3", "test33",
	} {
		bulk := sqlz.BulkInsert{
			Prefix: "insert into " + table + " (id, a, b) values ",
			Row:    "(?, ?, ?)",
		}
		bulk.Init(db, 100)
		for i := 0; i < 1000; i++ {
			require.NoError(t, bulk.Next(context.TODO(), i, i, i))
		}
		require.NoError(t, bulk.Done(context.TODO()))
	}

	return db
}

func hasPointPlan(rs *sqlz.ResultSet) bool {
	for i := 0; i < rs.NRows(); i++ {
		raw, _ := rs.RawValue(i, 0)
		if strings.Contains(string(raw), "Point_Get") {
			return true
		}
	}
	return false
}

func hasBatchPointPlan(rs *sqlz.ResultSet) bool {
	for i := 0; i < rs.NRows(); i++ {
		raw, _ := rs.RawValue(i, 0)
		if strings.Contains(string(raw), "Batch_Point_Get") {
			return true
		}
	}
	return false
}

func hasIndexScan(rs *sqlz.ResultSet) bool {
	for i := 0; i < rs.NRows(); i++ {
		raw, _ := rs.RawValue(i, 0)
		if strings.Contains(string(raw), "IndexRangeScan") {
			return true
		}
	}
	return false
}

func hasIndexLookUp(rs *sqlz.ResultSet) bool {
	for i := 0; i < rs.NRows(); i++ {
		raw, _ := rs.RawValue(i, 0)
		if strings.Contains(string(raw), "IndexLookUp") {
			return true
		}
	}
	return false
}

func hasHashAgg(rs *sqlz.ResultSet) bool {
	for i := 0; i < rs.NRows(); i++ {
		raw, _ := rs.RawValue(i, 0)
		if strings.Contains(string(raw), "HashAgg") {
			return true
		}
	}
	return false
}

func hasSort(rs *sqlz.ResultSet) bool {
	for i := 0; i < rs.NRows(); i++ {
		raw, _ := rs.RawValue(i, 0)
		if strings.Contains(string(raw), "Sort") {
			return true
		}
	}
	return false
}

func mustUsePointPlan(t *testing.T, rs *sqlz.ResultSet) {
	if !hasPointPlan(rs) {
		t.Log("must use point-get, but got:\n" + dumpResultSet(rs))
		t.FailNow()
	}
}

func mustUseBatchPointPlan(t *testing.T, rs *sqlz.ResultSet) {
	if !hasBatchPointPlan(rs) {
		t.Log("must use point-get, but got:\n" + dumpResultSet(rs))
		t.FailNow()
	}
}

func mustNotUsePointPlan(t *testing.T, rs *sqlz.ResultSet) {
	if hasPointPlan(rs) {
		t.Log("must not use point-get, but got:\n" + dumpResultSet(rs))
		t.FailNow()
	}
}

func mustHaveOneRow(t *testing.T, rs *sqlz.ResultSet) {
	if rs.NRows() != 1 {
		t.Log("must have one row, but got:" + fmt.Sprintf("%d\n", rs.NRows()))
		if rs.NRows() > 0 {
			t.Log("records:" + dumpResultSet(rs))
		}
		t.FailNow()
	}
}

func mustHaveNoneRow(t *testing.T, rs *sqlz.ResultSet) {
	if rs.NRows() != 0 {
		t.Log("must have none row, but got:\n" + dumpResultSet(rs))
		t.FailNow()
	}
}

// not use PointGet and IndexScan
func mustNotUsePPAndIS(t *testing.T, rs *sqlz.ResultSet) {
	if hasPointPlan(rs) {
		t.Log("must not use point-get, but got:\n" + dumpResultSet(rs))
		t.FailNow()
	}
	if hasIndexScan(rs) {
		t.Log("must not use index-scan, but got:\n" + dumpResultSet(rs))
		t.FailNow()
	}
}

func mustUseHashAgg(t *testing.T, rs *sqlz.ResultSet) {
	if !hasHashAgg(rs) {
		t.Log("must not use HashAgg, but got:\n" + dumpResultSet(rs))
		t.FailNow()
	}
}

func mustUseSort(t *testing.T, rs *sqlz.ResultSet) {
	if !hasSort(rs) {
		t.Log("must not use Sort, but got:\n" + dumpResultSet(rs))
		t.FailNow()
	}
}

// not use PointGet and IndexScan
func mustNotUsePPAndISAndLookUP(t *testing.T, rs *sqlz.ResultSet) {
	mustNotUsePPAndIS(t, rs)

	if hasIndexLookUp(rs) {
		t.Log("must not use IndexLookUp, but got:\n" + dumpResultSet(rs))
		t.FailNow()
	}
}

const (
	case25LookUPRowNum          = 1
	case25ISRowNum              = 2
	useShardIndexCase30ISRowNum = 1
	useShardIndexCase30PPRowNum = 2
)

func mustUseISAndLookUP(t *testing.T, rs *sqlz.ResultSet) {
	for i := 0; i < rs.NRows(); i++ {
		raw, _ := rs.RawValue(i, 0)
		if i == case25ISRowNum {
			if !strings.Contains(string(raw), "IndexRangeScan") {
				t.Log("must use IndexRangeScan, but got:\n" + dumpResultSet(rs))
				t.FailNow()
			}
		}

		if i == case25LookUPRowNum {
			if !strings.Contains(string(raw), "IndexLookUp") {
				t.Log("must use IndexLookUp, but got:\n" + dumpResultSet(rs))
				t.FailNow()
			}
		}

	}
}

func mustUseShardIndexAndPP(t *testing.T, rs *sqlz.ResultSet) {
	for i := 0; i < rs.NRows(); i++ {
		raw, _ := rs.RawValue(i, 0)
		if i == useShardIndexCase30ISRowNum {
			raw2, _ := rs.RawValue(i, 4)
			if !strings.Contains(string(raw2), "tidb_shard") {
				t.Log("must use Shard Index, but got:\n" + dumpResultSet(rs))
				t.FailNow()
			}
		}

		if i == useShardIndexCase30PPRowNum {
			if !strings.Contains(string(raw), "Point_Get") {
				t.Log("must use Point_Get, but got:\n" + dumpResultSet(rs))
				t.FailNow()
			}
		}

	}
}

type FunctionalTest struct {
	Pool   sqlz.ConnPool
	Query1 string
	Query2 string

	Serial bool

	ExplainAssert1 func(t *testing.T, rs *sqlz.ResultSet)
	ExplainAssert2 func(t *testing.T, rs *sqlz.ResultSet)
}

type dmlTest struct {
	Pool   sqlz.ConnPool
	Query1 string
	Query2 string

	Serial bool

	ExplainAssert func(t *testing.T, rs *sqlz.ResultSet)
	ResultAssert  func(t *testing.T, rs *sqlz.ResultSet)
}

type prepareTest struct {
	Pool sqlz.ConnPool
}

func (ft *FunctionalTest) Test(t *testing.T) {
	if !ft.Serial {
		t.Parallel()
	}

	ctx := context.Background()
	c, err := ft.Pool.Conn(ctx)
	require.NoError(t, err)
	defer sqlz.Release(c)

	for _, explain := range []struct {
		Query  string
		Assert func(t *testing.T, rs *sqlz.ResultSet)
	}{
		{ft.Query1, ft.ExplainAssert1},
		{ft.Query2, ft.ExplainAssert2},
	} {
		if explain.Assert != nil {
			rows, err := c.QueryContext(ctx, "explain "+explain.Query)
			require.NoError(t, err)
			rs, err := sqlz.ReadFromRows(rows)
			rows.Close()
			require.NoError(t, err)
			ft.ExplainAssert1(t, rs)
		}
	}

	if len(ft.Query2) == 0 {
		return
	}

	rows1, err := c.QueryContext(ctx, ft.Query1)
	require.NoError(t, err)
	rs1, err := sqlz.ReadFromRows(rows1)
	rows1.Close()
	require.NoError(t, err)

	rows2, err := c.QueryContext(ctx, ft.Query2)
	require.NoError(t, err)
	rs2, err := sqlz.ReadFromRows(rows2)
	rows1.Close()
	require.NoError(t, err)

	opts := sqlz.DigestOptions{Sort: true}
	if rs1.DataDigest(opts) != rs2.DataDigest(opts) {
		t.Logf("results are different:\n> %s\n%s\n> %s\n%s\n",
			ft.Query1, dumpResultSet(rs1), ft.Query2, dumpResultSet(rs2))
		t.FailNow()
	}
	// t.Logf("> %s\n%s\n", ft.Query1, dumpResultSet(rs1)) // uncomment this for debug
}

func (ft *dmlTest) executeDML(t *testing.T) {
	if !ft.Serial {
		t.Parallel()
	}

	ctx := context.Background()
	c, err := ft.Pool.Conn(ctx)
	require.NoError(t, err)
	defer sqlz.Release(c)

	if ft.ExplainAssert == nil ||
		len(ft.Query1) == 0 {
		return
	}

	rows, err := c.QueryContext(ctx, "explain "+ft.Query1)
	require.NoError(t, err)
	rs, err := sqlz.ReadFromRows(rows)
	rows.Close()
	require.NoError(t, err)
	ft.ExplainAssert(t, rs)

	if ft.ResultAssert == nil ||
		len(ft.Query2) == 0 {
		return
	}

	rows1, err := c.QueryContext(ctx, ft.Query1)
	require.NoError(t, err)
	rows1.Close()

	rows2, err := c.QueryContext(ctx, ft.Query2)
	require.NoError(t, err)
	rs2, err := sqlz.ReadFromRows(rows2)
	rows2.Close()
	require.NoError(t, err)
	ft.ResultAssert(t, rs2)
}

func executeBatchSql(t *testing.T, c *sql.Conn, queries []string,
	ctx context.Context, retLast bool) *sql.Rows {
	var err error
	var rows *sql.Rows
	lastQueryIndex := len(queries) - 1
	for i, query := range queries {
		rows, err = c.QueryContext(ctx, query)
		require.NoError(t, err)
		if !(retLast && i == lastQueryIndex) {
			rows.Close()
			rows = nil
		}
	}

	return rows
}

func (ft *prepareTest) executePrepare(t *testing.T) {
	ctx := context.Background()
	c, err := ft.Pool.Conn(ctx)
	require.NoError(t, err)
	defer sqlz.Release(c)

	queries1 := []string{
		"prepare stmt_test3 from \"select * from test3 where a=? and b=?\"",
		"set @var = 100",
		"execute stmt_test3 using @var,@var",
	}
	rows1 := executeBatchSql(t, c, queries1, ctx, true)
	rs1, err := sqlz.ReadFromRows(rows1)
	rows1.Close()
	require.NoError(t, err)

	queries2 := []string{
		"prepare stmt_test3 from \"select * from test3 where a=? and b=?\"",
		"set @var = 100",
		"execute stmt_test3 using @var,@var",
	}
	rows2 := executeBatchSql(t, c, queries2, ctx, true)
	rs2, err := sqlz.ReadFromRows(rows2)
	rows2.Close()
	require.NoError(t, err)

	opts := sqlz.DigestOptions{Sort: true}
	if rs1.DataDigest(opts) != rs2.DataDigest(opts) {
		t.Logf("results are different:\n> %s\n> %s\n",
			dumpResultSet(rs1), dumpResultSet(rs2))
	}
}

func TestFunctionalBasicQuery(t *testing.T) {
	schema := "shard_index"
	db := mustSetupDB(t, schema, nil)

	t.Run("UseShardIndex", func(t *testing.T) {
		t.Run("#1", (&FunctionalTest{
			Pool:           db,
			Query1:         "select * from test3 where a=100",
			Query2:         "select * from test33 where a=100",
			ExplainAssert1: mustUsePointPlan,
		}).Test)
		t.Run("#2", (&FunctionalTest{
			Pool:           db,
			Query1:         "select * from test3 where a=100 and b = 100",
			Query2:         "select * from test33 where a=100 and b = 100",
			ExplainAssert1: mustUsePointPlan,
		}).Test)
		t.Run("#3", (&FunctionalTest{
			Pool:           db,
			Query1:         "select * from test3 where a=100 and (b = 100 or b = 200)",
			Query2:         "select * from test33 where a=100 and (b = 100 or b = 200)",
			ExplainAssert1: mustUsePointPlan,
		}).Test)
		t.Run("#4", (&FunctionalTest{
			Pool:           db,
			Query1:         "select * from test3 where (id>990 or id<10) and a = 100",
			Query2:         "select * from test33 where (id>990 or id<10) and a = 100",
			ExplainAssert1: mustUsePointPlan,
		}).Test)
		t.Run("#5", (&FunctionalTest{
			Pool:           db,
			Query1:         "select * from test3 where a=100 or a = 200",
			Query2:         "select * from test33 where a=100 or a = 200",
			ExplainAssert1: mustUsePointPlan,
		}).Test)
		t.Run("#6", (&FunctionalTest{
			Pool:           db,
			Query1:         "select * from test3 where (a=100 and b = 100) or a = 300",
			Query2:         "select * from test33 where (a=100 and b = 100) or a = 300",
			ExplainAssert1: mustUseBatchPointPlan,
		}).Test)
		t.Run("#7", (&FunctionalTest{
			Pool:           db,
			Query1:         "select * from test3 where ((a=100 and b = 100) or a = 200) or a = 300",
			Query2:         "select * from test33 where ((a=100 and b = 100) or a = 200) or a = 300",
			ExplainAssert1: mustUseBatchPointPlan,
		}).Test)
		t.Run("#8", (&FunctionalTest{
			Pool:           db,
			Query1:         "select * from test4 where a=100",
			Query2:         "select * from test44 where a=100",
			ExplainAssert1: mustUsePointPlan,
		}).Test)
		t.Run("#9", (&FunctionalTest{
			Pool:           db,
			Query1:         "select * from test4 where b=100",
			Query2:         "select * from test44 where b=100",
			ExplainAssert1: mustUsePointPlan,
		}).Test)
		t.Run("#10", (&FunctionalTest{
			Pool:           db,
			Query1:         "select * from test4 where a=100 and b = 100",
			Query2:         "select * from test44 where a=100 and b = 100",
			ExplainAssert1: mustUsePointPlan,
		}).Test)
		t.Run("#11", (&FunctionalTest{
			Pool:           db,
			Query1:         "select * from test5 where a=100 and b = 100",
			Query2:         "select * from test55 where a=100 and b = 100",
			ExplainAssert1: mustUsePointPlan,
		}).Test)
		t.Run("#12", (&FunctionalTest{
			Pool:           db,
			Query1:         "select * from test5 where (a=100 and b = 100) or (a=200 and b = 200)",
			Query2:         "select * from test55 where (a=100 and b = 100) or (a=200 and b = 200)",
			ExplainAssert1: mustUseBatchPointPlan,
		}).Test)
		t.Run("#13", (&FunctionalTest{
			Pool:           db,
			Query1:         "select a+b from test5 where (a, b) in ((100, 100), (200, 200))",
			Query2:         "select a+b from test55 where (a, b) in ((100, 100), (200, 200))",
			ExplainAssert1: mustUseBatchPointPlan,
		}).Test)
		t.Run("#14", (&FunctionalTest{
			Pool:           db,
			Query1:         "select a+b from test5 where (a, b) in ((100, 100))",
			Query2:         "select a+b from test55 where (a, b) in ((100, 100))",
			ExplainAssert1: mustUsePointPlan,
		}).Test)
		t.Run("#15", (&FunctionalTest{
			Pool:           db,
			Query1:         "SELECT SUM(b) FROM test3 WHERE a = 100 GROUP BY id",
			Query2:         "SELECT SUM(b) FROM test33 WHERE a = 100 GROUP BY id",
			ExplainAssert1: mustUsePointPlan,
		}).Test)
		t.Run("#16", (&FunctionalTest{
			Pool:           db,
			Query1:         "SELECT SUM(b) FROM test3 WHERE a = 100 or a = 200 GROUP BY id",
			Query2:         "SELECT SUM(b) FROM test33 WHERE a = 100 or a = 200 GROUP BY id",
			ExplainAssert1: mustUsePointPlan,
		}).Test)
		t.Run("#17", (&FunctionalTest{
			Pool:           db,
			Query1:         "SELECT * FROM test3 WHERE a IN (100)",
			Query2:         "SELECT * FROM test33 WHERE a IN (100)",
			ExplainAssert1: mustUsePointPlan,
		}).Test)
		t.Run("#18", (&FunctionalTest{
			Pool:           db,
			Query1:         "SELECT * FROM test3 WHERE a IN (100, 200, 300)",
			Query2:         "SELECT * FROM test33 WHERE a IN (100, 200, 300)",
			ExplainAssert1: mustUseBatchPointPlan,
		}).Test)
		t.Run("#19", (&FunctionalTest{
			Pool:           db,
			Query1:         "SELECT sum(b) FROM test3 WHERE a IN (100, 200, 300)",
			Query2:         "SELECT sum(b) FROM test33 WHERE a IN (100, 200, 300)",
			ExplainAssert1: mustUseBatchPointPlan,
		}).Test)
		t.Run("#20", (&FunctionalTest{
			Pool:           db,
			Query1:         "SELECT * FROM test3 WHERE a IN (100, 200, 300) or a = 400",
			Query2:         "SELECT * FROM test33 WHERE a IN (100, 200, 300) or a = 400",
			ExplainAssert1: mustUseBatchPointPlan,
		}).Test)
		t.Run("#21", (&FunctionalTest{
			Pool:           db,
			Query1:         "SELECT * FROM testx  JOIN test3 ON testx.b = test3.b where test3.a = 100",
			Query2:         "SELECT * FROM testx  JOIN test33 ON testx.b = test33.b where test33.a = 100",
			ExplainAssert1: mustUsePointPlan,
		}).Test)
		t.Run("#22", (&FunctionalTest{
			Pool:           db,
			Query1:         "SELECT * FROM testx  JOIN test3 ON testx.b = test3.b where test3.a = 100 and testx.a > 10",
			Query2:         "SELECT * FROM testx  JOIN test33 ON testx.b = test33.b where test33.a = 100 and testx.a > 10",
			ExplainAssert1: mustUsePointPlan,
		}).Test)
		t.Run("#23", (&FunctionalTest{
			Pool:           db,
			Query1:         "SELECT * FROM test3 JOIN test6 ON test3.b = test6.b where test3.a = 100 and test6.a = 100",
			Query2:         "SELECT * FROM test33  JOIN test66 ON test33.b = test66.b where test33.a = 100 and test66.a = 100",
			ExplainAssert1: mustUsePointPlan,
		}).Test)
		t.Run("#24", (&FunctionalTest{
			Pool:           db,
			Query1:         "SELECT * FROM testx  JOIN test3 ON testx.a = test3.a where test3.a = 100",
			Query2:         "SELECT * FROM testx  JOIN test33 ON testx.a = test33.a where test33.a = 100",
			ExplainAssert1: mustUsePointPlan,
		}).Test)
		t.Run("#25", (&FunctionalTest{
			Pool:           db,
			Query1:         "SELECT * FROM (SELECT SUM(b) FROM test3 WHERE a = 100 GROUP BY id) dt",
			Query2:         "SELECT * FROM (SELECT SUM(b) FROM test33 WHERE a = 100 GROUP BY id) dt",
			ExplainAssert1: mustUsePointPlan,
		}).Test)
		t.Run("#26", (&FunctionalTest{
			Pool:           db,
			Query1:         "SELECT * FROM (SELECT COUNT(b) x FROM test3 WHERE a = 444 GROUP BY id) dt JOIN test33 ON dt.x = test33.a",
			Query2:         "SELECT * FROM (SELECT COUNT(b) x FROM test3 WHERE a = 444 GROUP BY id) dt JOIN test333 ON dt.x = test333.a",
			ExplainAssert1: mustUsePointPlan,
		}).Test)
		t.Run("#27", (&FunctionalTest{
			Pool:           db,
			Query1:         "SELECT * FROM test33 WHERE test33.a IN (SELECT test3.a FROM test3 WHERE test3.a = 100)",
			Query2:         "SELECT * FROM test333 WHERE test333.a IN (SELECT test3.a FROM test3 WHERE test3.a = 100)",
			ExplainAssert1: mustUsePointPlan,
		}).Test)
		t.Run("#28", (&FunctionalTest{
			Pool:           db,
			Query1:         "select * from test3 where a = b and b = 100",
			Query2:         "select * from test33 where a = b and b = 100",
			ExplainAssert1: mustUsePointPlan,
		}).Test)
		t.Run("#29", (&FunctionalTest{
			Pool:           db,
			Query1:         "select * from test3 where a=100 and a = 100",
			Query2:         "select * from test33 where a=100 and a = 100",
			ExplainAssert1: mustUsePointPlan,
		}).Test)
		t.Run("#30", (&FunctionalTest{
			Pool:           db,
			Query1:         "SELECT * FROM test8 WHERE a = 100",
			Query2:         "SELECT * FROM test88 WHERE a = 100",
			ExplainAssert1: mustUseShardIndexAndPP,
		}).Test)
		t.Run("#31", (&FunctionalTest{
			Pool:           db,
			Query1:         "SELECT * FROM test3 WHERE a IN (100) OR a IN (200)",
			Query2:         "SELECT * FROM test33 WHERE a IN (100) OR a IN (200)",
			ExplainAssert1: mustUseBatchPointPlan,
		}).Test)
		t.Run("#32", (&FunctionalTest{
			Pool:           db,
			Query1:         "SELECT * FROM test3 WHERE a IN (100, 300) OR a IN (200,400)",
			Query2:         "SELECT * FROM test33 WHERE a IN (100, 300) OR a IN (200,400)",
			ExplainAssert1: mustUseBatchPointPlan,
		}).Test)
		t.Run("#33", (&FunctionalTest{
			Pool:           db,
			Query1:         "SELECT * FROM test3 WHERE a IN (100) OR a IN (200,400)",
			Query2:         "SELECT * FROM test33 WHERE a IN (100) OR a IN (200,400)",
			ExplainAssert1: mustUseBatchPointPlan,
		}).Test)
		t.Run("#34", (&FunctionalTest{
			Pool:           db,
			Query1:         "SELECT * FROM test3 WHERE a IN (100) OR a IN (100,200,200)",
			Query2:         "SELECT * FROM test33 WHERE a IN (100) OR a IN (100,200,200)",
			ExplainAssert1: mustUseBatchPointPlan,
		}).Test)
		t.Run("#35", (&FunctionalTest{
			Pool:           db,
			Query1:         "SELECT * FROM test3 WHERE a =100 OR a IN (200,300)",
			Query2:         "SELECT * FROM test33 WHERE a =100 OR a IN (200,300)",
			ExplainAssert1: mustUseBatchPointPlan,
		}).Test)
	})

	t.Run("NotUseShardIndex", func(t *testing.T) {
		t.Run("#1", (&FunctionalTest{
			Pool:           db,
			Query1:         "select * from test333 where a=100",
			ExplainAssert1: mustNotUsePointPlan,
		}).Test)
		t.Run("#2", (&FunctionalTest{
			Pool:           db,
			Query1:         "select * from test3 where b = 8",
			Query2:         "select * from test33 where b = 8",
			ExplainAssert1: mustNotUsePointPlan,
		}).Test)
		t.Run("#3", (&FunctionalTest{
			Pool:           db,
			Query1:         "select * from test3 where a=100 or b = 200",
			Query2:         "select * from test33 where a=100 or b = 200",
			ExplainAssert1: mustNotUsePointPlan,
		}).Test)
		t.Run("#4", (&FunctionalTest{
			Pool:           db,
			Query1:         "select * from test3 where a=100 or a = 300 or a > 997",
			Query2:         "select * from test33 where a=100 or a = 300 or a > 997",
			ExplainAssert1: mustNotUsePointPlan,
		}).Test)
		t.Run("#5", (&FunctionalTest{
			Pool:           db,
			Query1:         "select * from test3 where a=100 or a = 200  or 1",
			Query2:         "select * from test33 where a=100 or a = 200  or 1",
			ExplainAssert1: mustNotUsePointPlan,
		}).Test)
		t.Run("#6", (&FunctionalTest{
			Pool:           db,
			Query1:         "select * from test4 where a=100 or a = 300 or a > 997",
			Query2:         "select * from test44 where a=100 or a = 300 or a > 997",
			ExplainAssert1: mustNotUsePointPlan,
		}).Test)
		t.Run("#7", (&FunctionalTest{
			Pool:           db,
			Query1:         "SELECT * FROM testx  JOIN test3 ON testx.b = test3.b where test3.a = 100 or testx.a = 10",
			Query2:         "SELECT * FROM testx  JOIN test33 ON testx.b = test33.b where test33.a = 100 or testx.a = 10",
			ExplainAssert1: mustNotUsePointPlan,
		}).Test)
		t.Run("#8", (&FunctionalTest{
			Pool:           db,
			Query1:         "SELECT * FROM test3  JOIN test6 ON test3.b = test6.b where test3.a = 100 or test6.a = 10",
			Query2:         "SELECT * FROM test33  JOIN test66 ON test33.b = test66.b where test33.a = 100 or test66.a = 10",
			ExplainAssert1: mustNotUsePointPlan,
		}).Test)
		t.Run("#9", (&FunctionalTest{
			Pool:           db,
			Query1:         "SELECT * FROM testx  JOIN test3 ON testx.a = test3.a where test3.a > 800",
			Query2:         "SELECT * FROM testx  JOIN test33 ON testx.a = test33.a where test33.a > 800",
			ExplainAssert1: mustNotUsePointPlan,
		}).Test)
		t.Run("#10", (&FunctionalTest{
			Pool:           db,
			Query1:         "select * from test333 where a=100",
			ExplainAssert1: mustNotUsePointPlan,
		}).Test)
		t.Run("#11", (&FunctionalTest{
			Pool:           db,
			Query1:         "select * from test3 where b = 8",
			Query2:         "select * from test33 where b = 8",
			ExplainAssert1: mustNotUsePointPlan,
		}).Test)
		t.Run("#12", (&FunctionalTest{
			Pool:           db,
			Query1:         "SELECT * FROM test3 WHERE b IN (100, 200, 300)",
			Query2:         "SELECT * FROM test33 WHERE b IN (100, 200, 300)",
			ExplainAssert1: mustNotUsePointPlan,
		}).Test)
		t.Run("#13", (&FunctionalTest{
			Pool:           db,
			Query1:         "select * from test3 where tidb_shard(b) = 8",
			Query2:         "select * from test33 where tidb_shard(b) = 8",
			ExplainAssert1: mustNotUsePointPlan,
		}).Test)
		t.Run("#14", (&FunctionalTest{
			Pool:           db,
			Query1:         "select * from test3 where ((a=100 and b = 100) or a = 200) and b = 300",
			Query2:         "select * from test33 where ((a=100 and b = 100) or a = 200) and b = 300",
			ExplainAssert1: mustNotUsePointPlan,
		}).Test)
		t.Run("#15", (&FunctionalTest{
			Pool:           db,
			Query1:         "select * from test3 where a = b",
			Query2:         "select * from test33 where a = b",
			ExplainAssert1: mustNotUsePointPlan,
		}).Test)
		t.Run("#16", (&FunctionalTest{
			Pool:           db,
			Query1:         "select * from testy where a=100",
			Query2:         "select * from testz where a=100",
			ExplainAssert1: mustNotUsePointPlan,
		}).Test)
		t.Run("#17", (&FunctionalTest{
			Pool:           db,
			Query1:         "select * from testy where a=100",
			Query2:         "select * from testz where a=100",
			ExplainAssert1: mustNotUsePointPlan,
		}).Test)
		t.Run("#18", (&FunctionalTest{
			Pool:           db,
			Query1:         "select * from test3 where a=100 and a = 200",
			ExplainAssert1: mustNotUsePointPlan,
		}).Test)
		t.Run("#19", (&FunctionalTest{
			Pool:           db,
			Query1:         "select * from test3 where a>100 and a < 200",
			Query2:         "select * from test33 where a>100 and a < 200",
			ExplainAssert1: mustNotUsePPAndIS,
		}).Test)
		t.Run("#20", (&FunctionalTest{
			Pool:           db,
			Query1:         "select * from test3 where a > 90",
			Query2:         "select * from test33 where a > 90",
			ExplainAssert1: mustNotUsePPAndIS,
		}).Test)
		t.Run("#21", (&FunctionalTest{
			Pool:           db,
			Query1:         "select * from test3 where a = 3 or a > 900",
			Query2:         "select * from test33 where a = 3 or a > 900",
			ExplainAssert1: mustNotUsePPAndIS,
		}).Test)
		t.Run("#22", (&FunctionalTest{
			Pool:           db,
			Query1:         "SELECT SUM(b) FROM test3 GROUP BY a",
			Query2:         "SELECT SUM(b) FROM test33 GROUP BY a",
			ExplainAssert1: mustUseHashAgg,
		}).Test)
		t.Run("#23", (&FunctionalTest{
			Pool:           db,
			Query1:         "SELECT a, b FROM test3 ORDER BY a",
			Query2:         "SELECT a, b FROM test33 ORDER BY a",
			ExplainAssert1: mustUseSort,
		}).Test)
		t.Run("#24", (&FunctionalTest{
			Pool:           db,
			Query1:         "SELECT * FROM testx  JOIN test3 ON testx.a = test3.a",
			Query2:         "SELECT * FROM testx  JOIN test33 ON testx.a = test33.a",
			ExplainAssert1: mustNotUsePPAndISAndLookUP,
		}).Test)
		t.Run("#25", (&FunctionalTest{
			Pool:           db,
			Query1:         "SELECT * FROM test7 WHERE a = 100",
			Query2:         "SELECT * FROM test77 WHERE a = 100",
			ExplainAssert1: mustUsePointPlan,
		}).Test)
	})

	//db.Exec("drop database shard_index")
	db.Close()
}

func TestFunctionalUpdateDelete(t *testing.T) {
	// t.Fatalf("implement me")
	schema := "shard_index_dml"
	db := mustSetupDB(t, schema, nil)
	t.Run("UseShardIndexDML", func(t *testing.T) {
		t.Run("#1", (&dmlTest{
			Pool:          db,
			Query1:        "update test6 set c = 1000 where a=50 and b = 50",
			Query2:        "select c from test6 where a=50 and b = 50 and c = 1000",
			ExplainAssert: mustUsePointPlan,
			ResultAssert:  mustHaveOneRow,
		}).executeDML)
		t.Run("#2", (&dmlTest{
			Pool:          db,
			Query1:        "delete from test6 where a = 45 and b = 45",
			Query2:        "select * from test6 where a = 45 and b = 45",
			ExplainAssert: mustUsePointPlan,
			ResultAssert:  mustHaveNoneRow,
		}).executeDML)
	})
}

func TestReverseShardIndex(t *testing.T) {
	// t.Fatalf("implement me")
	schema := "reverse_shard_index"
	db := SetupReverseIndexDB(t, schema, nil)
	t.Run("UseReverseShardIndex", func(t *testing.T) {
		t.Run("#1", (&FunctionalTest{
			Pool:           db,
			Query1:         "select * from testreverse3 where a=100",
			ExplainAssert1: mustUsePointPlan,
		}).Test)
		t.Run("#2", (&FunctionalTest{
			Pool:           db,
			Query1:         "select * from testreverse33 where a=100",
			ExplainAssert1: mustUsePointPlan,
		}).Test)
	})
}

func TestPrepareShardIndex(t *testing.T) {
	// t.Fatalf("implement me")
	schema := "prepare_shard_index"
	db := SetupPrepareDB(t, schema, nil)
	t.Run("PrepareShardIndex", func(t *testing.T) {
		t.Run("#1", (&prepareTest{
			Pool: db,
		}).executePrepare)
	})
}
