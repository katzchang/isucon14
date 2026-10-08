package main

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"os"
	"strconv"
	"strings"
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
	matchingOutcomes metric.Int64Counter
	matchingAttempts metric.Int64Histogram
)

// setupOTel は TracerProvider / MeterProvider / Propagator を初期化する。
// エクスポート先は OTEL_EXPORTER_OTLP_* 環境変数だけで決まる。
// OTEL_SDK_DISABLED=true のときは何も登録せず、グローバルは no-op のままになる。
func setupOTel(ctx context.Context) (func(context.Context) error, error) {
	if v, _ := strconv.ParseBool(os.Getenv("OTEL_SDK_DISABLED")); v {
		return func(context.Context) error { return nil }, nil
	}

	// 既定値を先に置き、環境変数由来の属性で上書きする
	res, err := resource.Merge(
		resource.NewSchemaless(attribute.String("service.name", defaultServiceName)),
		mustResource(resource.New(ctx, resource.WithFromEnv(), resource.WithTelemetrySDK())),
	)
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
func openInstrumentedDB(dsn string) (*sql.DB, error) {
	attrs := []attribute.KeyValue{attribute.String("db.system.name", "mysql"), attribute.String("db.system", "mysql")}
	sqlDB, err := otelsql.Open("mysql", dsn,
		otelsql.WithAttributes(attrs...),
		otelsql.WithSpanNameFormatter(dbSpanName),
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
	if query == "" {
		return string(method)
	}
	fields := strings.Fields(query)
	if len(fields) == 0 {
		return string(method)
	}
	op := strings.ToUpper(fields[0])
	var marker string
	switch op {
	case "SELECT", "DELETE":
		marker = "FROM"
	case "INSERT":
		marker = "INTO"
	case "UPDATE":
		return op + " " + cleanTable(fieldAt(fields, 1))
	default:
		return op
	}
	for i, f := range fields {
		if strings.EqualFold(f, marker) {
			// FROM (SELECT ...) のようなサブクエリはテーブル名を付けない
			if next := fieldAt(fields, i+1); strings.HasPrefix(next, "(") {
				break
			}
			if t := cleanTable(fieldAt(fields, i+1)); t != "" {
				return op + " " + t
			}
			break
		}
	}
	return op
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
func otelRouteMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)

		rctx := chi.RouteContext(r.Context())
		if rctx == nil {
			return
		}
		pattern := rctx.RoutePattern()
		if pattern == "" {
			return
		}
		span := trace.SpanFromContext(r.Context())
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
