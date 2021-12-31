package shard_index

import (
	"flag"
	"fmt"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/olekukonko/tablewriter"
	"github.com/zyguan/sqlz"
)

var Global struct {
	User string
	Pass string
	Host string
	Port int
}

func init() {
	flag.StringVar(&Global.User, "user", "root", "tidb user")
	flag.StringVar(&Global.Pass, "pass", "", "tidb password")
	flag.StringVar(&Global.Host, "host", "127.0.0.1", "tidb host")
	flag.IntVar(&Global.Port, "port", 4000, "tidb port")
}

func dsn(db string, params map[string]string) string {
	config := mysql.NewConfig()
	config.Net = "tcp"
	config.Addr = fmt.Sprintf("%s:%d", Global.Host, Global.Port)
	config.User = Global.User
	config.Passwd = Global.Pass
	config.DBName = db
	config.Params = params
	return config.FormatDSN()
}

func dumpResultSet(rs *sqlz.ResultSet) string {
	buf := new(strings.Builder)
	out := tablewriter.NewWriter(buf)
	out.SetAutoFormatHeaders(false)
	out.SetAutoWrapText(false)
	rs.Dump(out)
	out.Render()
	return buf.String()
}
