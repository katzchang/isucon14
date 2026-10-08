package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"log/slog"
	"net/url"

	"github.com/XSAM/otelsql"
	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	otelruntime "go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const (
	defaultServiceName  = "isuride"
	instrumentationName = "github.com/isucon/isucon14/webapp/go"
)

var (
	tracer = otel.Tracer(instrumentationName)

	// initAppMetrics で MeterProvider 登録後に作る
	matchingOutcomes  metric.Int64Counter
	matchingAttempts  metric.Int64Histogram
	dbCallsPerRequest metric.Int64Histogram
)

// buildVersion はビルド時に -ldflags "-X main.buildVersion=<commit>" で埋め込む。
// 空なら service.version を付けない。
var buildVersion string

// setupOTel は TracerProvider / MeterProvider / Propagator を初期化する。
// エクスポート先は OTEL_EXPORTER_OTLP_* 環境変数だけで決まる。
// OTEL_SDK_DISABLED=true のときは何も登録せず、グローバルは no-op のままになる。
func setupOTel(ctx context.Context) (func(context.Context) error, error) {
	if v, _ := strconv.ParseBool(os.Getenv("OTEL_SDK_DISABLED")); v {
		return func(context.Context) error { return nil }, nil
	}

	res, err := newResource(ctx)
	if err != nil {
		return nil, err
	}

	traceExporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter),
		sdktrace.WithResource(res),
	)

	metricExporter, err := otlpmetrichttp.New(ctx)
	if err != nil {
		return nil, err
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter,
			sdkmetric.WithInterval(envMillis("OTEL_METRIC_EXPORT_INTERVAL", 1000)),
			sdkmetric.WithTimeout(envMillis("OTEL_METRIC_EXPORT_TIMEOUT", 500)),
		)),
		sdkmetric.WithResource(res),
	)

	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	if err := otelruntime.Start(otelruntime.WithMinimumReadMemStatsInterval(time.Second)); err != nil {
		return nil, err
	}
	if err := initAppMetrics(); err != nil {
		return nil, err
	}

	lp, err := setupLogs(ctx, res)
	if err != nil {
		return nil, err
	}

	return func(ctx context.Context) error {
		// どれかが失敗しても全部 flush を試みる
		errs := []error{tp.Shutdown(ctx), mp.Shutdown(ctx)}
		if lp != nil {
			errs = append(errs, lp.Shutdown(ctx))
		}
		return errors.Join(errs...)
	}, nil
}

// newResource は既定値を先に置き、環境変数 (OTEL_SERVICE_NAME / OTEL_RESOURCE_ATTRIBUTES) 由来の属性で上書きする。
func newResource(ctx context.Context) (*resource.Resource, error) {
	defaults := []attribute.KeyValue{attribute.String("service.name", defaultServiceName)}
	if buildVersion != "" {
		defaults = append(defaults, attribute.String("service.version", buildVersion))
	}
	return resource.Merge(
		resource.NewSchemaless(defaults...),
		mustResource(resource.New(ctx, resource.WithFromEnv(), resource.WithTelemetrySDK())),
	)
}

func initAppMetrics() error {
	meter := otel.Meter(instrumentationName)
	var err error
	matchingOutcomes, err = meter.Int64Counter("isuride.matching.outcomes",
		metric.WithDescription("Number of matching runs by outcome"),
		metric.WithUnit("{run}"))
	if err != nil {
		return err
	}
	matchingAttempts, err = meter.Int64Histogram("isuride.matching.attempts",
		metric.WithDescription("Random chair picks tried per matching run"),
		metric.WithUnit("{attempt}"),
		metric.WithExplicitBucketBoundaries(1, 2, 3, 5, 8, 10))
	if err != nil {
		return err
	}
	dbCallsPerRequest, err = meter.Int64Histogram("isuride.http.server.db_calls",
		metric.WithDescription("Database statements executed per HTTP request"),
		metric.WithUnit("{call}"),
		metric.WithExplicitBucketBoundaries(1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000))
	return err
}

// setupLogs はアプリの slog を stdout とローカルの OTLP (obstudio) の両方へ出す。
// OTEL_LOGS_EXPORTER=none なら stdout だけ。ログの送信先がローカルでなければ OTLP は張らない。
func setupLogs(ctx context.Context, res *resource.Resource) (*sdklog.LoggerProvider, error) {
	stdout := slog.NewTextHandler(os.Stderr, nil)
	if os.Getenv("OTEL_LOGS_EXPORTER") == "none" {
		slog.SetDefault(slog.New(stdout))
		return nil, nil
	}
	if ep := os.Getenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT"); ep != "" && !isLocalEndpoint(ep) {
		slog.SetDefault(slog.New(stdout))
		slog.Warn("OTLP log endpoint is not local; application logs stay on stdout only", "endpoint", ep)
		return nil, nil
	}
	exp, err := otlploghttp.New(ctx)
	if err != nil {
		return nil, err
	}
	lp := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewBatchProcessor(exp)), sdklog.WithResource(res))
	global.SetLoggerProvider(lp)
	slog.SetDefault(slog.New(fanoutHandler{stdout, otelslog.NewHandler(instrumentationName, otelslog.WithLoggerProvider(lp))}))
	return lp, nil
}

func isLocalEndpoint(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	switch u.Hostname() {
	case "localhost", "127.0.0.1", "::1", "host.docker.internal":
		return true
	}
	return false
}

// fanoutHandler は 1 レコードを複数の slog.Handler に渡す (Go 1.23 には標準の fan-out がない)
type fanoutHandler []slog.Handler

func (f fanoutHandler) Enabled(ctx context.Context, l slog.Level) bool {
	for _, h := range f {
		if h.Enabled(ctx, l) {
			return true
		}
	}
	return false
}

func (f fanoutHandler) Handle(ctx context.Context, r slog.Record) error {
	var errs []error
	for _, h := range f {
		if h.Enabled(ctx, r.Level) {
			errs = append(errs, h.Handle(ctx, r.Clone()))
		}
	}
	return errors.Join(errs...)
}

func (f fanoutHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	out := make(fanoutHandler, len(f))
	for i, h := range f {
		out[i] = h.WithAttrs(attrs)
	}
	return out
}

func (f fanoutHandler) WithGroup(name string) slog.Handler {
	out := make(fanoutHandler, len(f))
	for i, h := range f {
		out[i] = h.WithGroup(name)
	}
	return out
}

func mustResource(r *resource.Resource, err error) *resource.Resource {
	if err != nil && r == nil {
		return resource.Empty()
	}
	return r
}

func envMillis(key string, def int) time.Duration {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil && v > 0 {
		return time.Duration(v) * time.Millisecond
	}
	return time.Duration(def) * time.Millisecond
}

// openInstrumentedDB は database/sql を otelsql で包んで開く。
// クエリ引数は記録しない (otelsql は db.statement にプレースホルダ付きの SQL だけを載せる)。
// interpolateParams は DSN の設定値。false のとき go-sql-driver は引数付きの
// conn.Query/Exec で driver.ErrSkip を返し、prepare + stmt で実行し直す。
func openInstrumentedDB(dsn string, interpolateParams bool) (*sql.DB, error) {
	attrs := []attribute.KeyValue{attribute.String("db.system.name", "mysql"), attribute.String("db.system", "mysql")}
	sqlDB, err := otelsql.Open("mysql", dsn,
		otelsql.WithAttributes(attrs...),
		otelsql.WithSpanNameFormatter(dbSpanName),
		otelsql.WithAttributesGetter(dbQueryAttributes),
		// メトリクス側でも driver.ErrSkip をエラーにしない (スパン側は DisableErrSkip)
		otelsql.WithDisableSkipErrMeasurement(true),
		otelsql.WithInstrumentAttributesGetter(func(ctx context.Context, method otelsql.Method, query string, args []driver.NamedValue) []attribute.KeyValue {
			if executedStatement(method, args, interpolateParams) {
				countDBCall(ctx)
			}
			return dbQueryAttributes(ctx, method, query, args)
		}),
		otelsql.WithSpanOptions(otelsql.SpanOptions{
			OmitConnResetSession: true,
			OmitRows:             true,
			OmitConnectorConnect: true,
			// go-sql-driver はプリペアドへのフォールバック合図として driver.ErrSkip を返す。エラー扱いしない
			DisableErrSkip: true,
		}),
	)
	if err != nil {
		return nil, err
	}
	if err := otelsql.RegisterDBStatsMetrics(sqlDB, otelsql.WithAttributes(attrs...)); err != nil {
		return nil, err
	}
	return sqlDB, nil
}

// dbSpanName は "SELECT rides" のように操作名とテーブル名だけでスパン名を作る (低カーディナリティ)。
func dbSpanName(_ context.Context, method otelsql.Method, query string) string {
	op, table := parseSQL(query)
	switch {
	case op == "":
		return string(method)
	case table == "":
		return op
	default:
		return op + " " + table
	}
}

// dbQueryAttributes はスパン名と同じ解析結果を db.operation.name / db.collection.name として付ける。
func dbQueryAttributes(_ context.Context, _ otelsql.Method, query string, _ []driver.NamedValue) []attribute.KeyValue {
	op, table := parseSQL(query)
	if op == "" {
		return nil
	}
	if table == "" {
		return []attribute.KeyValue{attribute.String("db.operation.name", op)}
	}
	return []attribute.KeyValue{attribute.String("db.operation.name", op), attribute.String("db.collection.name", table)}
}

// parseSQL は先頭のキーワードを操作名、FROM/INTO/UPDATE の直後をテーブル名として返す。
// サブクエリや解析できない文ではテーブル名を空にする。
func parseSQL(query string) (op, table string) {
	fields := strings.Fields(query)
	if len(fields) == 0 {
		return "", ""
	}
	op = strings.ToUpper(fields[0])
	var marker string
	switch op {
	case "SELECT", "DELETE":
		marker = "FROM"
	case "INSERT":
		marker = "INTO"
	case "UPDATE":
		return op, cleanTable(fieldAt(fields, 1))
	default:
		return op, ""
	}
	for i, f := range fields {
		if strings.EqualFold(f, marker) {
			// FROM (SELECT ...) のようなサブクエリはテーブル名を付けない
			if next := fieldAt(fields, i+1); strings.HasPrefix(next, "(") {
				return op, ""
			}
			return op, cleanTable(fieldAt(fields, i+1))
		}
	}
	return op, ""
}

// executedStatement は実際に DB で 1 回実行された文かを返す。
// prepare と、ErrSkip でプリペアドに切り替わる conn 側の呼び出しは数えない。
func executedStatement(method otelsql.Method, args []driver.NamedValue, interpolateParams bool) bool {
	switch method {
	case otelsql.MethodStmtExec, otelsql.MethodStmtQuery:
		return true
	case otelsql.MethodConnExec, otelsql.MethodConnQuery:
		return len(args) == 0 || interpolateParams
	}
	return false
}

type dbCallCounterKey struct{}

func countDBCall(ctx context.Context) {
	if c, ok := ctx.Value(dbCallCounterKey{}).(*atomic.Int64); ok {
		c.Add(1)
	}
}

func fieldAt(fields []string, i int) string {
	if i < len(fields) {
		return fields[i]
	}
	return ""
}

func cleanTable(s string) string {
	return strings.Trim(s, "`(),;")
}

// otelRouteMiddleware は chi のルーティング後に、ルートパターンを
// otelhttp のサーバースパン名・http.route 属性・メトリクス属性へ反映する。
// あわせて、リクエスト中に実行された DB 文の数を数えて記録する。
func otelRouteMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dbCalls := new(atomic.Int64)
		r = r.WithContext(context.WithValue(r.Context(), dbCallCounterKey{}, dbCalls))
		next.ServeHTTP(w, r)

		span := trace.SpanFromContext(r.Context())
		span.SetAttributes(attribute.Int64("isuride.db.call_count", dbCalls.Load()))

		rctx := chi.RouteContext(r.Context())
		if rctx == nil {
			return
		}
		pattern := rctx.RoutePattern()
		if pattern == "" {
			return
		}
		if dbCallsPerRequest != nil {
			dbCallsPerRequest.Record(r.Context(), dbCalls.Load(), metric.WithAttributes(attribute.String("http.route", pattern)))
		}
		span.SetName(r.Method + " " + pattern)
		span.SetAttributes(attribute.String("http.route", pattern))
		if labeler, ok := otelhttp.LabelerFromContext(r.Context()); ok {
			labeler.Add(attribute.String("http.route", pattern))
		}
	})
}

// paymentHTTPClient は決済ゲートウェイ呼び出し用。client スパンを出し、traceparent を伝搬する。
var paymentHTTPClient = &http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)}

// ctxResponseWriter はハンドラに渡る ResponseWriter にリクエストの context を持たせる。
// writeError(w, ...) の呼び出し箇所を変えずに、エラーを現在のスパンへ記録するため。
type ctxResponseWriter struct {
	http.ResponseWriter
	ctx context.Context
}

func (w ctxResponseWriter) Context() context.Context { return w.ctx }

func requestContextMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(ctxResponseWriter{ResponseWriter: w, ctx: r.Context()}, r)
	})
}

func contextFromWriter(w http.ResponseWriter) context.Context {
	if cw, ok := w.(interface{ Context() context.Context }); ok {
		return cw.Context()
	}
	return context.Background()
}
