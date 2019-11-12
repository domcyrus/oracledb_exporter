package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	_ "github.com/mattn/go-oci8"

	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/log"
	//Required for debugging
	//_ "net/http/pprof"
)

var (
	// Version will be set at build time.
	Version            = "0.0.0.dev"
	listenAddress      = flag.String("web.listen-address", ":9161", "Address to listen on for web interface and telemetry.")
	metricPath         = flag.String("web.telemetry-path", "/metrics", "Path under which to expose metrics.")
	landingPage        = []byte("<html><head><title>Oracle DB Exporter " + Version + "</title></head><body><h1>Oracle DB Exporter " + Version + "</h1><p><a href='" + *metricPath + "'>Metrics</a></p></body></html>")
	defaultFileMetrics = flag.String("default.metrics", "default-metrics.toml", "File with default metrics in a TOML file.")
	customMetrics      = flag.String("custom.metrics", os.Getenv("CUSTOM_METRICS"), "File that may contain various custom metrics in a TOML file.")
	queryTimeout       = flag.String("query.timeout", "5", "Query timeout (in seconds).")
	version            = flag.Bool("version", false, "Prints the Version.")
)

// Metric name parts.
const (
	namespace = "oracledb"
	exporter  = "exporter"
)

// Metric object description
type Metric struct {
	Context          string
	Labels           []string
	MetricsType      map[string]string
	MetricsDesc      map[string]string
	FieldToAppend    string
	Request          string
	IgnoreZeroResult bool
}

// Exporter collects Oracle DB metrics. It implements prometheus.Collector.
type Exporter struct {
	dbEnvs         []*dbEnvironment
	metricsToScrap []Metric
	duration       *prometheus.GaugeVec
	err            *prometheus.GaugeVec
	totalScrapes   *prometheus.CounterVec
	scrapeErrors   *prometheus.CounterVec
	up             *prometheus.GaugeVec
}

// NewExporter returns a new Oracle DB exporter for the provided DSN.
func NewExporter(dbEnvs []*dbEnvironment) *Exporter {
	for _, env := range dbEnvs {
		var err error
		env.db, err = sql.Open("oci8", env.dsn)
		if err != nil {
			log.Fatalf("unable to connect to: %s, failed with: %s", env.dsn, err)
		}
		env.db.SetMaxIdleConns(0)
		env.db.SetMaxOpenConns(10)
	}
	return &Exporter{
		duration: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: exporter,
			Name:      "last_scrape_duration_seconds",
			Help:      "Duration of the last scrape of metrics from Oracle DB.",
		}, []string{"env"}),
		totalScrapes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: exporter,
			Name:      "scrapes_total",
			Help:      "Total number of times Oracle DB was scraped for metrics.",
		}, []string{"env"}),
		scrapeErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: exporter,
			Name:      "scrape_errors_total",
			Help:      "Total number of times an error occured scraping a Oracle database.",
		}, []string{"env"}),
		err: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: exporter,
			Name:      "last_scrape_error",
			Help:      "Whether the last scrape of metrics from Oracle DB resulted in an error (1 for error, 0 for success).",
		}, []string{"env"}),
		up: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "up",
			Help:      "Whether the Oracle database server is up.",
		}, []string{"env"}),
		dbEnvs: dbEnvs,
	}
}

// Describe describes all the metrics exported by the SQL exporter.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	// We cannot know in advance what metrics the exporter will generate
	// So we use the poor man's describe method: Run a collect
	// and send the descriptors of all the collected metrics. The problem
	// here is that we need to connect to the Oracle DB. If it is currently
	// unavailable, the descriptors will be incomplete. Since this is a
	// stand-alone exporter and not used as a library within other code
	// implementing additional metrics, the worst that can happen is that we
	// don't detect inconsistent metrics created by this exporter
	// itself. Also, a change in the monitored Oracle instance may change the
	// exported metrics during the runtime of the exporter.

	metricCh := make(chan prometheus.Metric)
	doneCh := make(chan struct{})

	go func() {
		for m := range metricCh {
			ch <- m.Desc()
		}
		close(doneCh)
	}()

	e.Collect(metricCh)
	close(metricCh)
	<-doneCh
}

// Collect implements prometheus.Collector.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	for _, env := range e.dbEnvs {
		e.scrapeEnv(env, ch)
		//		e.duration.Collect(ch)
		//		e.totalScrapes.Collect(ch)
		//		e.err.Collect(ch)
		//		e.scrapeErrors.Collect(ch)
		//		e.up.Collect(ch)
	}
}

func (e *Exporter) scrapeEnv(env *dbEnvironment, ch chan<- prometheus.Metric) {
	e.totalScrapes.WithLabelValues(env.name).Inc()
	var err error
	defer func(start time.Time) {
		e.duration.WithLabelValues(env.name).Set(time.Since(start).Seconds())
		if err == nil {
			e.err.WithLabelValues(env.name).Set(0)
		} else {
			e.err.WithLabelValues(env.name).Set(1)
		}
	}(time.Now())
	if err = env.db.Ping(); err != nil {
		if strings.Contains(err.Error(), "sql: database is closed") {
			log.Infoln("reconnecting to DB")
			env.db, err = sql.Open("oci8", env.dsn)
			env.db.SetMaxIdleConns(0)
			env.db.SetMaxOpenConns(10)
		}
	}
	if err = env.db.Ping(); err != nil {
		log.Errorln("error pinging oracle:", err)
		env.db.Close()
		e.up.WithLabelValues(env.name).Set(0)
		return
	}
	e.up.WithLabelValues(env.name).Set(1)

	for _, metric := range e.metricsToScrap {
		if err = ScrapeMetric(env.name, env.db, ch, metric); err != nil {
			log.Errorln("error scraping for", metric.Context, ":", err)
			e.scrapeErrors.WithLabelValues(metric.Context).Inc()
		}
	}
}

// GetMetricType omg omg omg
func GetMetricType(metricType string, metricsType map[string]string) prometheus.ValueType {
	var strToPromType = map[string]prometheus.ValueType{
		"gauge":   prometheus.GaugeValue,
		"counter": prometheus.CounterValue,
	}

	strType, ok := metricsType[strings.ToLower(metricType)]
	if !ok {
		return prometheus.GaugeValue
	}
	valueType, ok := strToPromType[strings.ToLower(strType)]
	if !ok {
		log.Fatalf("failed getting prometheus type from str type: %s", strings.ToLower(strType))
	}
	return valueType
}

// ScrapeMetric interface method to call ScrapeGenericValues using Metric struct values
func ScrapeMetric(env string, db *sql.DB, ch chan<- prometheus.Metric, metricDefinition Metric) error {
	return ScrapeGenericValues(env, db, ch, metricDefinition.Context, metricDefinition.Labels,
		metricDefinition.MetricsDesc, metricDefinition.MetricsType,
		metricDefinition.FieldToAppend, metricDefinition.IgnoreZeroResult,
		metricDefinition.Request)
}

// ScrapeGenericValues generic method for retrieving metrics.
func ScrapeGenericValues(env string, db *sql.DB, ch chan<- prometheus.Metric, context string, labels []string,
	metricsDesc map[string]string, metricsType map[string]string, fieldToAppend string, ignoreZeroResult bool, request string) error {
	metricsCount := 0
	genericParser := func(row map[string]string) error {
		// Construct labels value
		labelsValues := []string{}
		for _, label := range labels {
			labelsValues = append(labelsValues, row[label])
		}
		labels = append(labels, "env")
		labelsValues = append(labelsValues, env)
		// Construct Prometheus values to sent back
		for metric, metricHelp := range metricsDesc {
			value, err := strconv.ParseFloat(strings.TrimSpace(row[metric]), 64)
			// If not a float, skip current metric
			if err != nil {
				continue
			}
			// If metric do not use a field content in metric's name
			if strings.Compare(fieldToAppend, "") == 0 {
				desc := prometheus.NewDesc(
					prometheus.BuildFQName(namespace, context, metric),
					metricHelp,
					labels, nil,
				)
				ch <- prometheus.MustNewConstMetric(desc, GetMetricType(metric, metricsType), value, labelsValues...)
			} else {
				desc := prometheus.NewDesc(
					prometheus.BuildFQName(namespace, context, cleanName(row[fieldToAppend])),
					metricHelp,
					labels, nil,
				)
				ch <- prometheus.MustNewConstMetric(desc, GetMetricType(metric, metricsType), value, labelsValues...)
			}
			metricsCount++
		}
		return nil
	}
	err := GeneratePrometheusMetrics(db, genericParser, request)
	if err != nil {
		return err
	}
	if !ignoreZeroResult && metricsCount == 0 {
		return errors.New("no metrics found while parsing")
	}
	return err
}

// GeneratePrometheusMetrics inspired by https://kylewbanks.com/blog/query-result-to-map-in-golang
// Parse SQL result and call parsing function to each row
func GeneratePrometheusMetrics(db *sql.DB, parse func(row map[string]string) error, query string) error {

	// Add a timeout
	timeout, err := strconv.Atoi(*queryTimeout)
	if err != nil {
		log.Fatalf("error while converting timeout option value: %s with err: %s", *queryTimeout, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, query)

	if ctx.Err() == context.DeadlineExceeded {
		return errors.New("oracle query timed out")
	}

	if err != nil {
		return err
	}
	cols, err := rows.Columns()
	defer rows.Close()

	for rows.Next() {
		// Create a slice of interface{}'s to represent each column,
		// and a second slice to contain pointers to each item in the columns slice.
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		// Scan the result into the column pointers...
		if err := rows.Scan(columnPointers...); err != nil {
			return err
		}

		// Create our map, and retrieve the value for each column from the pointers slice,
		// storing it in the map with the name of the column as the key.
		m := make(map[string]string)
		for i, colName := range cols {
			val := columnPointers[i].(*interface{})
			m[strings.ToLower(colName)] = fmt.Sprintf("%v", *val)
		}
		// Call function to parse row
		if err := parse(m); err != nil {
			return err
		}
	}

	return nil

}

// Oracle gives us some ugly names back. This function cleans things up for Prometheus.
func cleanName(s string) string {
	s = strings.Replace(s, " ", "_", -1) // Remove spaces
	s = strings.Replace(s, "(", "", -1)  // Remove open parenthesis
	s = strings.Replace(s, ")", "", -1)  // Remove close parenthesis
	s = strings.Replace(s, "/", "", -1)  // Remove forward slashes
	s = strings.ToLower(s)
	return s
}

type dbEnvironment struct {
	name string
	dsn  string
	db   *sql.DB
}

// system/blabla@docker.for.mac.localhost:1521/DINTDB
func parseDSN(s string) ([]*dbEnvironment, error) {
	var dbEnvs []*dbEnvironment
	dsnEnvs := strings.Split(s, ",")
	for _, env := range dsnEnvs {
		parts := strings.Split(env, "/")
		if len(parts) < 3 {
			return nil, fmt.Errorf("unable to get oracle SID from data source environment: %s", env)
		}
		oracleSID := parts[len(parts)-1]
		log.Infof("found oracle SID: %s in connection string: %s", oracleSID, env)
		dbEnvs = append(dbEnvs, &dbEnvironment{name: oracleSID, dsn: env})
	}
	return dbEnvs, nil
}

func main() {
	flag.Parse()
	if *version == true {
		fmt.Printf("version: %s\n", Version)
		return
	}
	log.Infoln("starting oracledb_exporter " + Version)
	dsn := os.Getenv("DATA_SOURCE_NAME")
	dbEnvs, err := parseDSN(dsn)
	if err != nil {
		log.Fatalln(err)
	}

	// Load default metrics
	var metrics struct{ Metric []Metric }
	if _, err := toml.DecodeFile(*defaultFileMetrics, &metrics); err != nil {
		log.Fatalf("failed loading default metrics: %s with: %s", *defaultFileMetrics, err)
	}

	// If custom metrics, load it
	var addMetrics struct{ Metric []Metric }
	if strings.Compare(*customMetrics, "") != 0 {
		if _, err := toml.DecodeFile(*customMetrics, &addMetrics); err != nil {
			log.Fatalf("failed loading custom metrics: %s with: %s", *customMetrics, err)
		}
		metrics.Metric = append(metrics.Metric, addMetrics.Metric...)
	}
	exporter := NewExporter(dbEnvs)
	prometheus.MustRegister(exporter)
	http.Handle(*metricPath, promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write(landingPage)
	})
	log.Infoln("listening on", *listenAddress)
	log.Fatal(http.ListenAndServe(*listenAddress, nil))
}
