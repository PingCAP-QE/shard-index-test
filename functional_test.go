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
		"drop table if exists test3, test33, test333, test4, test44, test5, test55, test6, testx, testy, testz",
		"create table test3(id int primary key clustered, a int, b int, unique key uk_expr((tidb_shard(a)),a))",
		"create table test33(id int primary key clustered, a int, b int, unique key a(a))",
		"create table test333(id int primary key clustered, a int, b int, unique key uk_expr((vitess_hash(a)%256),a))",
		"create table test4(id int primary key clustered, a int, b int, unique key uk_expr((tidb_shard(a)),a),unique key uk_b_expr((tidb_shard(b)),b))",
		"create table test44(id int primary key clustered, a int, b int, unique key uk_expr(a),unique key uk_b_expr(b))",
		"create table test5(id int primary key clustered, a int, b int, unique key uk_expr((tidb_shard(a)),a,b))",
		"create table test55(id int primary key clustered, a int, b int, unique key a(a,b))",
		"create table test6(id int primary key clustered, a int, b int, c int, unique key uk_expr((tidb_shard(a)), a))",
		"create table testx(id int primary key clustered, a int, b int, unique key a(a))",
		"create table testy(id int primary key clustered, a int, b int, unique key uk_expr((tidb_shard(b)),a))",
		"create table testz(id int primary key clustered, a int, b int, unique key uk_expr((tidb_shard(a+b)),a))",
	} {
		_, err := db.Exec(ddl)
		require.NoError(t, err, "exec "+ddl)
	}

	for _, table := range []string{
		"test3", "test33", "test333", "test4", "test44", "test5", "test55", "testy", "testz",
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

	_, err = db.Exec("insert into testx select * from test33 where a % 37 = 0")
	require.NoError(t, err)

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

func mustUsePointPlan(t *testing.T, rs *sqlz.ResultSet) {
	if !hasPointPlan(rs) {
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

type FunctionalTest struct {
	Pool   sqlz.ConnPool
	Query1 string
	Query2 string

	Serial bool

	ExplainAssert1 func(t *testing.T, rs *sqlz.ResultSet)
	ExplainAssert2 func(t *testing.T, rs *sqlz.ResultSet)
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
	}
	// t.Logf("> %s\n%s\n", ft.Query1, dumpResultSet(rs1)) // uncomment this for debug
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
		// ...
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
		// ...
	})

	//db.Exec("drop database shard_index")
	db.Close()
}

func TestFunctionalUpdateDelete(t *testing.T) {
	t.Fatalf("implement me")
	schema := "shard_index_dml"
	mustSetupDB(t, schema, nil)
}
