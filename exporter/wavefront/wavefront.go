package wavefront

import (
	"log"
	"sync"
	"sync/atomic"

	"github.com/wavefronthq/wavefront-sdk-go/application"
	"github.com/wavefronthq/wavefront-sdk-go/histogram"
	wfsender "github.com/wavefronthq/wavefront-sdk-go/senders"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/trace"
)

const (
	DefaultSource    = ""
	DefaultQueueSize = 1000

	nanoToMillis int64 = 1e6
)

// Options
type Options struct {
	Source         string
	Hgs            map[histogram.Granularity]bool
	VerboseLogging bool
	appMap         map[string]string
	qSize          int
}

type Option func(*Options)
type SendCmd func()

// Source overrides the deault source
func Source(source string) Option {
	return func(o *Options) {
		o.Source = source
	}
}

// Granularity enables specified granularities when
// sending Wavefront histograms
func Granularity(hgs ...histogram.Granularity) Option {
	return func(o *Options) {
		for _, g := range hgs {
			o.Hgs[g] = true
		}
	}
}

// AppTags allows setting Application, Service, etc...
// Shown in Wavefront UI
func AppTags(app application.Tags) Option {
	return func(o *Options) {
		o.appMap = app.Map()
	}
}

// QueueSize sets the maximum size of the queue which holds
// requests that are waiting to be sent.
// Spans/Metrics are dropped if the Queue is full
func QueueSize(queueSize int) Option {
	return func(o *Options) {
		o.qSize = queueSize
	}
}

// VerboseLogging enables logging of errors per span/metric.
// Logs to stderr or equivalent
func VerboseLogging() Option {
	return func(o *Options) {
		o.VerboseLogging = true
	}
}

// Exporter
type Exporter struct {
	sender wfsender.Sender
	sem    chan struct{}
	wg     sync.WaitGroup

	// Embeddings
	Options
	_SelfMetrics
}

// NewExporter returns a trace.Exporter configured to upload traces and views
// to the configured wavefront instance (via Wavefront Sender)
//
// Documentation for Wavefront Sender is available at
// https://github.com/wavefrontHQ/wavefront-sdk-go
//
// Options adds additional options to the exporter.
func NewExporter(sender wfsender.Sender, option ...Option) (*Exporter, error) {
	defOptions := Options{
		Source: DefaultSource,
		Hgs:    map[histogram.Granularity]bool{},
		qSize:  DefaultQueueSize,
	}

	for _, o := range option {
		o(&defOptions)
	}

	exp := &Exporter{
		sender:  sender,
		Options: defOptions,
		sem:     make(chan struct{}, defOptions.qSize),
	}

	exp.ReportSelfHealth() // Disable by default?

	return exp, nil
}

// Flush blocks until the queue is flushed at the Sender.
func (e *Exporter) Flush() {
	e.wg.Wait()
	e.sender.Flush()
}

// ExportSpan exports given span to Wavefront
func (e *Exporter) ExportSpan(spanData *trace.SpanData) {
	e.processSpan(spanData)
}

// ExportView exports given view to Wavefront
func (e *Exporter) ExportView(viewData *view.Data) {
	e.processView(viewData)
}

// Helpers

func (e *Exporter) queueCmd(cmd SendCmd) bool {
	select {
	case e.sem <- struct{}{}:
		e.wg.Add(1)
		go cmd()
		return true
	default:
		return false
	}
}

func (e *Exporter) semRelease() {
	<-e.sem
	e.wg.Done()
}

func (e *Exporter) logError(msg string, err error) {
	if err != nil {
		atomic.AddUint64(&e.senderErrors, 1)
		if e.VerboseLogging {
			log.Println(msg, err)
		}
	}
}
