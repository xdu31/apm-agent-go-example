package main

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"

	"go.elastic.co/apm"
	"go.elastic.co/apm/module/apmgorilla"
	"go.elastic.co/apm/module/apmlogrus"
	"go.elastic.co/apm/module/apmsql"
	_ "go.elastic.co/apm/module/apmsql/sqlite3"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
)

var (
	db  *sql.DB
	log = &logrus.Logger{
		Out:   os.Stderr,
		Hooks: make(logrus.LevelHooks),
		Level: logrus.DebugLevel,
		Formatter: &logrus.JSONFormatter{
			FieldMap: logrus.FieldMap{
				logrus.FieldKeyTime:  "@timestamp",
				logrus.FieldKeyLevel: "log.level",
				logrus.FieldKeyMsg:   "message",
				logrus.FieldKeyFunc:  "function.name", // non-ECS
			},
		},
	}
)

func init() {
	// apmlogrus.Hook will send "error", "panic", and "fatal" log messages to Elastic APM.
	log.AddHook(&apmlogrus.Hook{})
}

func helloHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)

	log := log.WithFields(apmlogrus.TraceContext(req.Context()))
	log.WithField("vars", vars).Info("handling hello request")

	name := vars["name"]
	requestCount, err := updateRequestCount(req.Context(), name)
	if err != nil {
		log.WithError(err).Error("failed to update request count")
		http.Error(w, "failed to update request count", 500)
		return
	}
	fmt.Fprintf(w, "Hello, %s! (#%d)\n", name, requestCount)
}

// updateRequestCount increments a count for name in db, returning the new count.
func updateRequestCount(ctx context.Context, name string) (int, error) {
	span, ctx := apm.StartSpan(ctx, "updateRequestCount", "custom")
	defer span.End()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return -1, err
	}
	row := tx.QueryRowContext(ctx, "SELECT count FROM stats WHERE name=?", name)
	var count int
	switch err := row.Scan(&count); err {
	case nil:
		count++
		if _, err := tx.ExecContext(ctx, "UPDATE stats SET count=?WHERE name=?", count, name); err != nil {
			return -1, err
		}
	case sql.ErrNoRows:
		count = 1
		if _, err := tx.ExecContext(ctx, "INSERT INTO stats (name, count) VALUES (?, ?)", name, count); err != nil {
			return -1, err
		}
	default:
		return -1, err
	}

	return count, tx.Commit()
}

func main() {
	// test on db
	var err error
	//https://github.com/mattn/go-sqlite3/blob/master/README.md#faq
	//db, err = apmsql.Open("sqlite3", "file::memory:?mode=memory&cache=shared")
	db, err = apmsql.Open("sqlite3", "file::memory:?mode=memory&cache=shared")
	if err != nil {
		log.Fatal(err)
	}
	//db.SetMaxOpenConns(1)
	if _, err := db.Exec("CREATE TABLE stats (name TEXT PRIMARY KEY, count INTEGER);"); err != nil {
		log.Fatal(err)
	}

	// test on http handler
	r := mux.NewRouter()
	r.HandleFunc("/hello/{name}", helloHandler)
	r.Use(apmgorilla.Middleware())
	log.Fatal(http.ListenAndServe(":8000", r))
}
