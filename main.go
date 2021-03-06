package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ssm"

	_ "github.com/mattn/go-oci8"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/log"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	// Version will be set at build time.
	Version            = "0.0.0.dev"
	app                = kingpin.New("oracle exporter", "A oracle metrics exporter")
	listenAddress      = app.Flag("web.listen-address", "Address to listen on for web interface and telemetry.").Default(":9161").String()
	metricPath         = app.Flag("web.telemetry-path", "Path under which to expose metrics.").Default("/metrics").String()
	landingPage        = []byte("<html><head><title>Oracle DB Exporter " + Version + "</title></head><body><h1>Oracle DB Exporter " + Version + "</h1><p><a href='" + *metricPath + "'>Metrics</a></p></body></html>")
	defaultFileMetrics = app.Flag("default.metrics", "File with default metrics in a TOML file.").Default("default-metrics.toml").String()
	customMetrics      = app.Flag("custom.metrics", "File that may contain various custom metrics in a TOML file.").Envar("CUSTOM_METRICS").String()

	dataSourceNames = app.Flag("dsn", "The data source names (DSNs) comma separated strings like: system/blabla@docker.for.mac.localhost:1521/DINTDB. Only use it if you don't use SSM parameters.").Envar("DATA_SOURCE_NAME").String()

	// aws ssm related flags
	awsRegion   = app.Flag("aws.region", "The aws region to use").Default("eu-central-1").String()
	ssmPrefix   = app.Flag("ssm.prefix", "The ssm parameter prefix").Required().String()
	ssmUser     = app.Flag("ssm.user", "The ssm parameter to get the oracle user").Default("monitoring-user").String()
	ssmPassword = app.Flag("ssm.password", "The ssm parameter to get the oracle password").Default("monitoring-password").String()
	ssmPort     = app.Flag("ssm.port", "The ssm parameter to get the oracle port").Default("port").String()
	ssmSIDs     = app.Flag("ssm.sids", "The ssm parameter to get the oracle sids comma separated list").Default("sids").String()
	ssmHost     = app.Flag("ssm.host", "The ssm parameter to get the oracle host").Default("host").String()

	queryTimeout = app.Flag("query.timeout", "Query timeout (in seconds).").Default("5").Int()
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
	metricsToScrap []*Metric
	duration       *prometheus.GaugeVec
	err            *prometheus.GaugeVec
	totalScrapes   *prometheus.CounterVec
	scrapeErrors   *prometheus.CounterVec
	up             *prometheus.GaugeVec
}

// NewExporter returns a new Oracle DB exporter for the provided DSN.
func NewExporter(dbEnvs []*dbEnvironment, metrics []*Metric) *Exporter {
	for _, env := range dbEnvs {
		var err error
		env.db, err = sql.Open("oci8", env.dsn)
		if err != nil {
			log.Fatalf("unable to connect to: %s, failed with: %s", env.dsn, err)
		}
		// By design exporter should use maximum one connection per request.
		env.db.SetMaxOpenConns(1)
		env.db.SetMaxIdleConns(1)
		// Set max lifetime for a connection.
		env.db.SetConnMaxLifetime(1 * time.Minute)
	}

	// adding env label to all metrics
	for _, metric := range metrics {
		metric.Labels = append(metric.Labels, "sid")
	}

	return &Exporter{
		metricsToScrap: metrics,
		duration: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: exporter,
			Name:      "last_scrape_duration_seconds",
			Help:      "Duration of the last scrape of metrics from Oracle DB.",
		}, []string{"sid"}),
		totalScrapes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: exporter,
			Name:      "scrapes_total",
			Help:      "Total number of times Oracle DB was scraped for metrics.",
		}, []string{"sid"}),
		scrapeErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: exporter,
			Name:      "scrape_errors_total",
			Help:      "Total number of times an error occured scraping a Oracle database.",
		}, []string{"collector", "sid"}),
		err: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: exporter,
			Name:      "last_scrape_error",
			Help:      "Whether the last scrape of metrics from Oracle DB resulted in an error (1 for error, 0 for success).",
		}, []string{"sid"}),
		up: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "up",
			Help:      "Whether the Oracle database server is up.",
		}, []string{"sid"}),
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
			log.Debugf("registering metric: %s", m.Desc())
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
	var wg sync.WaitGroup
	for _, env := range e.dbEnvs {
		wg.Add(1)
		go e.scrapeEnv(env, ch, &wg)
	}
	wg.Wait()
	e.duration.Collect(ch)
	e.totalScrapes.Collect(ch)
	e.err.Collect(ch)
	e.scrapeErrors.Collect(ch)
	e.up.Collect(ch)
}

func (e *Exporter) scrapeEnv(env *dbEnvironment, ch chan<- prometheus.Metric, wg *sync.WaitGroup) {
	e.totalScrapes.WithLabelValues(env.sid).Inc()
	var err error
	defer func(start time.Time) {
		e.duration.WithLabelValues(env.sid).Set(time.Since(start).Seconds())
		if err == nil {
			e.err.WithLabelValues(env.sid).Set(0)
		} else {
			e.err.WithLabelValues(env.sid).Set(1)
		}
		wg.Done()
	}(time.Now())

	if err = env.db.Ping(); err != nil {
		if strings.Contains(err.Error(), "sql: database is closed") {
			log.Infof("reconnecting to DB SID: %s", env.sid)
			env.db, err = sql.Open("oci8", env.dsn)

			if err != nil {
				log.Errorf("pinging oracle failed SID: %s connection string: %s, with error: %s", env.sid, env.dsn, err)
				env.db.Close()
				e.up.WithLabelValues(env.sid).Set(0)
				return
			}

			// By design exporter should use maximum one connection per request.
			env.db.SetMaxOpenConns(1)
			env.db.SetMaxIdleConns(1)
			// Set max lifetime for a connection.
			env.db.SetConnMaxLifetime(2 * time.Minute)
		}
	}

	e.up.WithLabelValues(env.sid).Set(1)
	for _, metric := range e.metricsToScrap {
		log.Debugf("scrape metric: %s", metric.Context)
		if err = ScrapeMetric(env.sid, env.db, ch, metric); err != nil {
			log.Errorln("error scraping for", metric.Context, ":", err)
			e.scrapeErrors.WithLabelValues(metric.Context, env.sid).Inc()
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
func ScrapeMetric(env string, db *sql.DB, ch chan<- prometheus.Metric, metricDefinition *Metric) error {
	log.Debugln("scrape metric")
	return ScrapeGenericValues(env, db, ch, metricDefinition.Context, metricDefinition.Labels,
		metricDefinition.MetricsDesc, metricDefinition.MetricsType,
		metricDefinition.FieldToAppend, metricDefinition.IgnoreZeroResult,
		metricDefinition.Request)
}

const oracleDate = "2006/01/02:15:04:05"

// ScrapeGenericValues generic method for retrieving metrics.
func ScrapeGenericValues(
	env string,
	db *sql.DB,
	ch chan<- prometheus.Metric,
	context string,
	labels []string,
	metricsDesc map[string]string,
	metricsType map[string]string,
	fieldToAppend string,
	ignoreZeroResult bool,
	request string,
) error {
	log.Debugln("scrape generic values")
	var metricsCount int
	genericParser := func(row map[string]string) error {
		// Construct labels value
		labelsValues := []string{}
		for _, label := range labels[:len(labels)-1] {
			labelsValues = append(labelsValues, row[label])
		}
		// adding env as the last label
		labelsValues = append(labelsValues, env)
		// Construct Prometheus values to sent back
		for metric, metricHelp := range metricsDesc {
			value, err := strconv.ParseFloat(strings.TrimSpace(row[metric]), 64)
			// If not a float, skip current metric
			if err != nil {
				// check if it is an oracle date string
				// 2020/01/23:16:00:03 using timezone of the box
				t, err := time.Parse(oracleDate, strings.TrimSpace(row[metric]))
				if err != nil {
					continue
				}
				value = float64(t.Unix())
			}
			// If metric do not use a field content in metric's name
			if strings.Compare(fieldToAppend, "") == 0 {
				desc := prometheus.NewDesc(
					prometheus.BuildFQName(namespace, context, metric),
					metricHelp,
					labels, nil,
				)
				log.Debugf("adding generic metric: %s", desc)
				ch <- prometheus.MustNewConstMetric(desc, GetMetricType(metric, metricsType), value, labelsValues...)
			} else {
				desc := prometheus.NewDesc(
					prometheus.BuildFQName(namespace, context, cleanName(row[fieldToAppend])),
					metricHelp,
					labels, nil,
				)
				log.Debugf("adding generic metric: %s", desc)
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*queryTimeout)*time.Second)
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
	sid string
	dsn string
	db  *sql.DB
}

type credentials struct {
	user     string
	password string
}

func getParameter(ssmsvc *ssm.SSM, keyname *string) string {
	key := fmt.Sprintf("/%s/%s", *ssmPrefix, *keyname)
	withDecryption := true
	param, err := ssmsvc.GetParameter(&ssm.GetParameterInput{
		Name:           &key,
		WithDecryption: &withDecryption,
	})
	if err != nil {
		log.Fatalf("failed to retrieve aws key: %s with: %s", *keyname, err)
	}
	return *param.Parameter.Value
}

const dsnFormat = "%s/%s@%s:%s/%s"

func generateDSN(s string) ([]*dbEnvironment, error) {
	var dbEnvs []*dbEnvironment
	if s != "" {
		// system/blabla@docker.for.mac.localhost:1521/DINTDB
		dsnEnvs := strings.Split(s, ",")
		for _, env := range dsnEnvs {
			parts := strings.Split(env, "/")
			if len(parts) < 3 {
				return nil, fmt.Errorf("unable to get oracle SID from data source environment: %s", env)
			}
			oracleSID := parts[len(parts)-1]
			log.Infof("found oracle SID: %s in connection string: %s", oracleSID, env)
			dbEnvs = append(dbEnvs, &dbEnvironment{sid: oracleSID, dsn: env})
		}
		return dbEnvs, nil
	}

	sess, err := session.NewSessionWithOptions(session.Options{
		Config:            aws.Config{Region: aws.String(*awsRegion)},
		SharedConfigState: session.SharedConfigEnable,
	})
	if err != nil {
		log.Fatalf("failed to create aws session with: %s", err)
	}

	ssmsvc := ssm.New(sess, aws.NewConfig().WithRegion(*awsRegion))

	user := getParameter(ssmsvc, ssmUser)
	pw := getParameter(ssmsvc, ssmPassword)
	port := getParameter(ssmsvc, ssmPort)
	sids := getParameter(ssmsvc, ssmSIDs)
	host := getParameter(ssmsvc, ssmHost)

	sidsList := strings.Split(sids, ",")
	if len(sidsList) == 0 {
		log.Fatalf("no sid defined in sid ssm parameter: %s", *ssmSIDs)
	}
	for _, sid := range sidsList {
		dsn := fmt.Sprintf(dsnFormat, user, pw, host, port, sid)
		dbEnvs = append(dbEnvs, &dbEnvironment{sid: sid, dsn: dsn})
	}
	return dbEnvs, nil
}

func main() {
	app.Version(Version)
	log.AddFlags(app)
	kingpin.MustParse(app.Parse(os.Args[1:]))

	log.Infoln("starting oracledb_exporter " + Version)
	dbEnvs, err := generateDSN(*dataSourceNames)
	if err != nil {
		log.Fatalln(err)
	}

	// Load default metrics
	var metrics struct{ Metric []*Metric }
	if _, err := toml.DecodeFile(*defaultFileMetrics, &metrics); err != nil {
		log.Fatalf("failed loading default metrics: %s with: %s", *defaultFileMetrics, err)
	}

	// If custom metrics, load it
	var addMetrics struct{ Metric []*Metric }
	if strings.Compare(*customMetrics, "") != 0 {
		if _, err := toml.DecodeFile(*customMetrics, &addMetrics); err != nil {
			log.Fatalf("failed loading custom metrics: %s with: %s", *customMetrics, err)
		}
		metrics.Metric = append(metrics.Metric, addMetrics.Metric...)
	}
	exporter := NewExporter(dbEnvs, metrics.Metric)
	prometheus.MustRegister(exporter)
	http.Handle(*metricPath, promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write(landingPage)
	})
	log.Infoln("listening on", *listenAddress)
	log.Fatal(http.ListenAndServe(*listenAddress, nil))
}
