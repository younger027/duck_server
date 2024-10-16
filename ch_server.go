package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/marcboeker/go-duckdb"
	"github.com/sirupsen/logrus"
	"golang.org/x/crypto/pbkdf2"
)

const authTTL = 60
const AuthToken = "O1xCJhBIvkaqEc3R"

type authItem struct {
	user     string
	password string
	time     time.Time
}

type ChServer struct {
	conn      *sql.DB
	connector driver.Connector
	pgServer  *PgServer
	authCache sync.Map
}

/*
testInsertFormatRegexp 用于匹配带format关键字的INSERT语句。
testInsertValuesQueryRegexp 用于匹配包含VALUES的INSERT语句。
testInsertRegexp 用于匹配单独的INSERT关键字。
*/

var testInsertFormatRegexp = regexp.MustCompile(`(?i)^\s*INSERT\s+INTO.*?format\s+\S+[\s;]*$`)
var testInsertValuesQueryRegexp = regexp.MustCompile(`(?i)^\s*INSERT\s+INTO.*VALUES.*[\s;]*$`)
var testInsertRegexp = regexp.MustCompile(`(?i)^\s*INSERT$`)

func getSHA256Sum(key []byte) []byte {
	h := sha256.New()
	_, _ = h.Write(key)
	return h.Sum(nil)
}

func (c *ChServer) Auth(user, password string) error {
	if cacheItem, ok := c.authCache.Load("user"); ok {
		if time.Since(cacheItem.(*authItem).time).Seconds() < authTTL {
			if cacheItem.(*authItem).password == password {
				return nil
			} else {
				return fmt.Errorf("invalid username or password")
			}
		}
	}
	pgpassword, err := c.pgServer.GetPassword(user)
	if err != nil {
		return fmt.Errorf("invalid username or password")
	}
	groups := regexp.MustCompile(`^SCRAM-SHA-256\$(\d+):(.*?)\$(.*?):(.*?)$`).FindStringSubmatch(pgpassword)
	if len(groups) != 5 {
		logrus.Warnf("invalid password format: %s", pgpassword)
	}
	salt, _ := base64.StdEncoding.DecodeString(groups[2])
	iterations := groups[1]
	serverKey, _ := base64.StdEncoding.DecodeString(groups[4])
	iterationsInt, _ := strconv.Atoi(iterations)
	digestKey := pbkdf2.Key([]byte(password), salt, iterationsInt, 32, sha256.New)
	computed := computeHMAC(digestKey, []byte("Server Key"))
	if !bytes.Equal(computed, serverKey) {
		return fmt.Errorf("invalid username or password")
	}
	c.authCache.Store(user, &authItem{user: user, password: password, time: time.Now()})
	return nil
}

func (c *ChServer) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if c.pgServer.enableAuth {
		user, password, ok := r.BasicAuth()
		if !ok {
			user = r.URL.Query().Get("user")
			password = r.URL.Query().Get("password")
		}
		if user == "" {
			wr.WriteHeader(401)
			_, _ = fmt.Fprintf(wr, "User not specified")
			return
		}
		if password == "" {
			wr.WriteHeader(401)
			_, _ = fmt.Fprintf(wr, "Password not specified")
			return
		}
		err := c.Auth(user, password)
		if err != nil {
			wr.WriteHeader(401)
			_, _ = fmt.Fprintf(wr, "Unauthorized: %s", err)
			return
		}
	}

	fmt.Println("uri ", r.RequestURI)
	//use simple auth check
	token := r.Header.Get("token")
	if token != AuthToken {
		wr.WriteHeader(401)
		return
	}

	if r.RequestURI == "/report" {
		businessID := r.Header.Get("business_id")
		d, _ := io.ReadAll(r.Body)
		c.MustExecuteQuery(r.Context(), businessID, string(d), wr)
		return
	}

	if r.Method == http.MethodGet {
		query := r.URL.Query().Get("query")
		d, _ := io.ReadAll(r.Body)
		query += " "
		query += string(d)
		c.SelectQuery(r.Context(), query, wr)
	}
	if r.Method == http.MethodPost {
		query := r.URL.Query().Get("query")
		if query != "" {
			query += "\n"
		}
		rd := bufio.NewReader(r.Body)
		for {
			if testSelectQueryRegexp.MatchString(query) {
				d, _ := io.ReadAll(rd)
				query += string(d)
				c.SelectQuery(r.Context(), query, wr)
				return
			}
			if testInsertFormatRegexp.MatchString(query) {
				c.InsertFormat(r.Context(), query, rd, wr)
				return
			}

			//处理其他情况，不是插入语句，或者是插入insert values类似sql
			if query != "" && (!testInsertRegexp.MatchString(query) || testInsertValuesQueryRegexp.MatchString(query)) {
				d, _ := io.ReadAll(rd)
				query += string(d)
				c.ExecuteQuery(r.Context(), query, wr)
				return
			}
			line, err := rd.ReadString('\n')
			query += strings.ReplaceAll(line, "\n", " ")
			if err != nil {
				break
			}
		}
		if testSelectQueryRegexp.MatchString(query) {
			c.SelectQuery(r.Context(), query, wr)
			return
		}
		if !testInsertRegexp.MatchString(query) || testInsertValuesQueryRegexp.MatchString(query) {
			c.ExecuteQuery(r.Context(), query, wr)
			return
		}
	}
}

var testSelectQueryRegexp = regexp.MustCompile(`(?i)^\s*SELECT.*$`)
var selectFormatRegexp = regexp.MustCompile(`(?i)^\s*SELECT.* format (\S*?)[\s;]*$`)
var formatCleanRegexp = regexp.MustCompile(`(?i)^\s*(SELECT.* )(format \S*?)[\s;]*$`)
var limitRewriteRegexp = regexp.MustCompile(`(?i)LIMIT\s+(\d+)\s*,\s*(\d+)`)

func (c *ChServer) SelectQuery(ctx context.Context, query string, wr http.ResponseWriter) {
	//quick fix for datagrip
	query = strings.TrimSpace(query)
	query = strings.ReplaceAll(query, "version()", "'23.3.1.2823'")
	query = strings.Replace(query, "select table", `select "table"`, 1)
	logrus.Debugf("Executing ch query: %s", query)
	query = strings.ReplaceAll(query, "\n", " ")
	query = limitRewriteRegexp.ReplaceAllString(query, "LIMIT $2 OFFSET $1")
	if !testSelectQueryRegexp.MatchString(query) {
		wr.WriteHeader(400)
		_, _ = fmt.Fprintf(wr, "Invalid query")
		return
	}
	format := "TabSeparated"
	if m := selectFormatRegexp.FindStringSubmatch(query); len(m) > 1 {
		format = m[1]
		query = formatCleanRegexp.ReplaceAllString(query, "$1")
	}
	formater := GetClickhouseOutputFormat(format)
	if formater == nil {
		wr.WriteHeader(400)
		_, _ = fmt.Fprintf(wr, "Unknown format %s", format)
		return
	}
	rows, err := c.conn.QueryContext(ctx, query)
	if err != nil {
		wr.WriteHeader(500)
		_, _ = fmt.Fprintf(wr, "Error executing query: %s", err)
		return
	}
	defer rows.Close()
	columnsDesc, err := rows.ColumnTypes()
	columnNames := make([]string, len(columnsDesc))
	columnTypes := make([]string, len(columnsDesc))
	for i, col := range columnsDesc {
		columnNames[i] = col.Name()
		columnTypes[i] = col.DatabaseTypeName()
	}
	//gz := gzip.NewWriter(wr)
	fmter, err := formater(columnNames, columnTypes, wr)
	if err != nil {
		wr.WriteHeader(500)
		_, _ = fmt.Fprintf(wr, "Error creating format: %s", err)
		return
	}
	wr.Header().Set("Transfer-Encoding", "chunked")
	wr.Header().Set("x-clickhouse-format", format)
	wr.Header().Set("Content-Type", GetClickhouseFormatContentType(format))
	wr.WriteHeader(200)
	values := make([]any, len(columnNames))
	valuePointers := make([]any, len(columnNames))
	for i := range values {
		valuePointers[i] = &values[i]
	}
	for rows.Next() {
		err = rows.Scan(valuePointers...)
		if err != nil {
			_, _ = fmt.Fprintf(wr, "Error scanning row: %s", err)
			return
		}
		err = fmter.Write(values)
		if err != nil {
			_, _ = fmt.Fprintf(wr, "Error writing row: %s", err)
			return
		}
	}
	err = fmter.Close()
}

func (c *ChServer) ExecuteQuery(ctx context.Context, query string, wr http.ResponseWriter) {
	_, err := c.conn.ExecContext(ctx, query)
	if err != nil {
		wr.WriteHeader(500)
		_, _ = fmt.Fprintf(wr, "Error executing query: %s", err)
		return
	}
	wr.WriteHeader(200)
}

func (c *ChServer) MustExecuteQuery(ctx context.Context, tableName, query string, wr http.ResponseWriter) {
	if tableName == "" {
		wr.WriteHeader(400)
		fmt.Fprint(wr, "businessID is empty on http header")
		return
	}
	retData, insertValueSql, err := ParseJSONStrToSQLField(tableName, query)
	if err != nil {
		wr.WriteHeader(400)
		fmt.Fprint(wr, err.Error())
		return
	}

	for {
		_, err = c.conn.ExecContext(ctx, insertValueSql)
		if err == nil {
			wr.WriteHeader(200)
			return
		}

		fmt.Println("exec insert sql err:", err.Error())

		tableName, field, _ := ParseSqlErrType(err.Error())
		if tableName == "" && field == "" {
			wr.WriteHeader(500)
			fmt.Fprintf(wr, "parse sql err msg failed for get table and field failed: %s", err.Error())
			return
		}

		createSql, err := ProduceCreateSql(retData, tableName, field)
		if err != nil {
			wr.WriteHeader(500)
			fmt.Fprintf(wr, "produce create sql failed: %s", err.Error())
			return
		}

		fmt.Println("new sql ---", createSql)
		_, err = c.conn.ExecContext(ctx, createSql)
		if err != nil {
			fmt.Println("exec create sql err:", err.Error())
			wr.WriteHeader(500)
			return
		}

	}

}

func (c *ChServer) InsertFormat(ctx context.Context, query string, rd *bufio.Reader, wr http.ResponseWriter) {
	var insertFormatRegexp = regexp.MustCompile(`(?i)^\s*INSERT\s+INTO(.*?)format\s+(\S+)[\s;]*$`)
	groups := insertFormatRegexp.FindStringSubmatch(query)
	if len(groups) < 3 {
		wr.WriteHeader(400)
		_, _ = fmt.Fprintf(wr, "Invalid query")
		return
	}
	tableExpr := groups[1]
	format := groups[2]
	formater := GetClickhouseInputFormat(format)
	if formater == nil {
		wr.WriteHeader(400)
		_, _ = fmt.Fprintf(wr, "Unknown format %s", format)
		return
	}
	schema, table, columns, err := parseTablesAndColumns(tableExpr)
	if err != nil {
		wr.WriteHeader(400)
		_, _ = fmt.Fprintf(wr, "Invalid table expression: %s", err)
		return
	}
	rows, err := c.conn.QueryContext(context.Background(), fmt.Sprintf("SELECT * FROM %s.%s LIMIT 0", schema, table))
	if err != nil {
		wr.WriteHeader(500)
		_, _ = fmt.Fprintf(wr, "Error getting table description: %s", err)
		return
	}
	columnDesc, err := rows.ColumnTypes()
	if err != nil {
		wr.WriteHeader(500)
		_, _ = fmt.Fprintf(wr, "Error getting table description: %s", err)
		return
	}
	_ = rows.Close()
	columnNames := make([]string, 0)
	columnTypes := make([]string, 0)
	if len(columns) == 0 {
		for _, col := range columnDesc {
			columnNames = append(columnNames, col.Name())
			columnTypes = append(columnTypes, col.DatabaseTypeName())
		}
	} else {
		for _, c := range columns {
			found := false
			for _, col := range columnDesc {
				if col.Name() == c {
					columnNames = append(columnNames, c)
					columnTypes = append(columnTypes, col.DatabaseTypeName())
					found = true
					break
				}
			}
			if !found {
				wr.WriteHeader(400)
				_, _ = fmt.Fprintf(wr, "Column %s not found in table", c)
				return
			}
		}
	}
	//todo reuse connection
	conn, err := c.connector.Connect(context.Background())
	defer conn.Close()
	appender, err := duckdb.NewAppenderFromConn(conn, schema, table)
	if err != nil {
		wr.WriteHeader(500)
		_, _ = fmt.Fprintf(wr, "Error creating appender: %s", err)
		return
	}
	defer appender.Close()
	formatWriter, err := formater(columnNames, columnTypes, rd)
	if err != nil {
		wr.WriteHeader(500)
		_, _ = fmt.Fprintf(wr, "Error creating formater: %s", err)
		return
	}
	values := make([]driver.Value, len(columnNames))
	var done = false
	go func() {
		<-ctx.Done()
		done = true
	}()
	for {
		if done {
			wr.WriteHeader(500)
			_, _ = fmt.Fprintf(wr, "Request cancelled")
			return
		}
		err = formatWriter.Read(values)
		if err == io.EOF {
			break
		}
		if err != nil {
			wr.WriteHeader(500)
			_, _ = fmt.Fprintf(wr, "Error reading values: %s", err)
			return
		}
		err = appender.AppendRow(values...)
	}
	err = appender.Flush()
	if err != nil {
		wr.WriteHeader(500)
		_, _ = fmt.Fprintf(wr, "Error flushing appender: %s", err)
		return
	}
	wr.WriteHeader(200)
}

func parseTablesAndColumns(t string) (string, string, []string, error) {
	t = regexp.MustCompile(`\s+`).ReplaceAllString(t, "")
	groups := regexp.MustCompile(`^(\w+\.|)(\w+)(\([\w,]+\)|)$`).FindStringSubmatch(t)
	if len(groups) != 4 {
		return "", "", nil, fmt.Errorf("invalid table name " + t)
	}
	schema := groups[1]
	if schema == "" {
		schema = "main"
	}
	table := groups[2]
	columns := groups[3]
	if columns == "" {
		return schema, table, nil, nil
	}
	columns = columns[1 : len(columns)-1]
	return schema, table, strings.Split(columns, ","), nil
}
