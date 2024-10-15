package main

import (
	"flag"
	_ "net/http/pprof"

	"github.com/sirupsen/logrus"
)

const VERSION = "0.1.0"

func main() {
	//go func() {
	//	http.ListenAndServe("localhost:6060", nil)
	//}()
	logrus.Infof("duck_server %s", VERSION)
	pgListen := flag.String("pg_listen", ":5432", "Postgres listen address")
	chListen := flag.String("ch_listen", ":8123", "Clickhouse listen address")
	dbPath := flag.String("db_path", "./test.db", "Path to the database file")
	logLevel := flag.String("log_level", "trace", "Log level")
	hack := flag.Bool("hack", true, "hack")
	auth := flag.Bool("auth", false, "enable auth")
	flag.Parse()
	switch *logLevel {
	case "trace":
		logrus.SetLevel(logrus.TraceLevel)
	case "debug":
		logrus.SetLevel(logrus.DebugLevel)
	case "info":
		logrus.SetLevel(logrus.InfoLevel)
	case "warn":
		logrus.SetLevel(logrus.WarnLevel)
	case "error":
		logrus.SetLevel(logrus.ErrorLevel)
	}
	server := PgServer{}
	defer server.CloseConn()
	server.Start(serverOptions{
		DbPath:  *dbPath,
		Listen:  *pgListen,
		UseHack: *hack,
		ClickhouseOptions: ClickhouseOptions{
			Enabled: true,
			Listen:  *chListen,
		},
		Auth: *auth,
	})
}
