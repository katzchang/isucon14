package main

import (
	"context"
	"database/sql/driver"
	"testing"

	"github.com/XSAM/otelsql"
	"go.opentelemetry.io/otel/attribute"
)

func TestParseSQL(t *testing.T) {
	cases := []struct{ query, op, table string }{
		{"SELECT * FROM rides WHERE id = ?", "SELECT", "rides"},
		{"select * from `chairs` where id = ?", "SELECT", "chairs"},
		{"INSERT INTO ride_statuses (id) VALUES (?)", "INSERT", "ride_statuses"},
		{"UPDATE rides SET chair_id = ? WHERE id = ?", "UPDATE", "rides"},
		{"SELECT COUNT(*) = 0 FROM (SELECT 1) is_completed", "SELECT", ""},
		{"SELECT * FROM chairs INNER JOIN (SELECT id FROM chairs) AS tmp", "SELECT", "chairs"},
		{"", "", ""},
	}
	for _, c := range cases {
		op, table := parseSQL(c.query)
		if op != c.op || table != c.table {
			t.Errorf("parseSQL(%q) = %q, %q; want %q, %q", c.query, op, table, c.op, c.table)
		}
	}
}

func TestDBQueryAttributesOmitsSubqueryTable(t *testing.T) {
	got := dbQueryAttributes(context.Background(), otelsql.MethodStmtQuery, "SELECT COUNT(*) FROM (SELECT 1) x", nil)
	want := []attribute.KeyValue{attribute.String("db.operation.name", "SELECT")}
	if len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestExecutedStatement(t *testing.T) {
	args := []driver.NamedValue{{Ordinal: 1, Value: 1}}
	cases := []struct {
		method      otelsql.Method
		args        []driver.NamedValue
		interpolate bool
		want        bool
	}{
		{otelsql.MethodStmtQuery, args, false, true},
		{otelsql.MethodStmtExec, args, false, true},
		{otelsql.MethodConnQuery, nil, false, true},     // 引数なしは conn でそのまま実行される
		{otelsql.MethodConnQuery, args, false, false},   // ErrSkip でプリペアドに切り替わる
		{otelsql.MethodConnExec, args, true, true},      // interpolateParams なら conn で実行される
		{otelsql.MethodConnPrepare, args, false, false}, // prepare は実行ではない
		{otelsql.MethodConnBeginTx, nil, false, false},
	}
	for _, c := range cases {
		if got := executedStatement(c.method, c.args, c.interpolate); got != c.want {
			t.Errorf("executedStatement(%s, %d args, %v) = %v; want %v", c.method, len(c.args), c.interpolate, got, c.want)
		}
	}
}

func TestCountDBCall(t *testing.T) {
	countDBCall(context.Background()) // カウンタがなくても panic しない
}

func TestNewResourceServiceVersion(t *testing.T) {
	old := buildVersion
	t.Cleanup(func() { buildVersion = old })
	buildVersion = "abc1234"

	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "")
	res, err := newResource(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := res.Set().Value("service.version"); v.AsString() != "abc1234" {
		t.Errorf("service.version = %q; want build default", v.AsString())
	}

	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "service.version=operator")
	res, err = newResource(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := res.Set().Value("service.version"); v.AsString() != "operator" {
		t.Errorf("service.version = %q; want operator override", v.AsString())
	}
}
