package wavefront

import (
	"testing"
	"time"

	wfsender "github.com/wavefronthq/wavefront-sdk-go/senders"
	"go.opencensus.io/exemplar"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"go.opencensus.io/trace"
)

var benchexp *Exporter
var exs *trace.SpanData
var vd1, vd2 *view.Data

func init() {
	proxyCfg := &wfsender.ProxyConfiguration{
		Host:             "localhost",
		MetricsPort:      30000,
		DistributionPort: 40000,
		TracingPort:      50000,
	}
	sender, _ := wfsender.NewProxySender(proxyCfg)
	benchexp, _ = NewExporter(sender, QueueSize(0)) // Drop msgs when benching

	exs = &trace.SpanData{
		SpanContext: trace.SpanContext{
			TraceID:      trace.TraceID{},
			SpanID:       trace.SpanID{},
			TraceOptions: 0x1,
		},
		ParentSpanID: trace.SpanID{},
		Name:         "span",
		StartTime:    time.Now(),
		EndTime:      time.Now(),
		// Added in sub-benchmarks
		// Status: trace.Status{
		// 	Code:    trace.StatusCodeUnknown,
		// 	Message: "some error",
		// },
		// Attributes: map[string]interface{}{
		// 	"foo1": "bar1",
		// 	"foo2": 5.25,
		// 	"foo3": 42,
		// },
		// Annotations: []trace.Annotation{
		// 	{Message: "1.500000", Attributes: map[string]interface{}{"key1": "value1"}},
		// 	{Message: "Annotate", Attributes: map[string]interface{}{"key2": "value2"}},
		// },
		// MessageEvents: []trace.MessageEvent{
		// 	{EventType: 2, MessageID: 0x3, UncompressedByteSize: 0x190, CompressedByteSize: 0x12c},
		// 	{EventType: 1, MessageID: 0x1, UncompressedByteSize: 0xc8, CompressedByteSize: 0x64},
		// },
	}

	vd1 = &view.Data{
		Start: time.Now(),
		End:   time.Now(),
		View: &view.View{
			Name:        "v1",
			Description: "v1",
			Measure:     stats.Int64("v1", "v1", "v1unit"),
			Aggregation: view.LastValue(),
		},
		Rows: []*view.Row{
			&view.Row{
				Tags: []tag.Tag{},
				Data: &view.LastValueData{
					Value: 4,
				},
			},
		},
	}

	vd2 = &view.Data{
		Start: time.Now(),
		End:   time.Now(),
		View: &view.View{
			Name:        "v2",
			Description: "v2",
			Measure:     stats.Int64("v2", "v2", "v2unit"),
			Aggregation: view.Distribution(10),
		},
		Rows: []*view.Row{
			&view.Row{
				Tags: []tag.Tag{},
				Data: &view.DistributionData{
					Count: 2, Min: 9, Max: 27, Mean: 18, SumOfSquaredDev: 0,
					CountPerBucket: []int64{1, 1},
					ExemplarsPerBucket: []*exemplar.Exemplar{
						&exemplar.Exemplar{Value: 9},
						&exemplar.Exemplar{Value: 27},
					},
				},
			},
		},
	}
}

func BenchmarkProcessSpan(t *testing.B) {

	t.Run("Basic", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			benchexp.processSpan(exs)
		}
	})

	exs.Status = trace.Status{
		Code:    trace.StatusCodeUnknown,
		Message: "some error",
	}

	t.Run("+Status", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			benchexp.processSpan(exs)
		}
	})

	exs.Attributes = map[string]interface{}{
		"foo1": "bar1",
		"foo2": 5.25,
		"foo3": 42,
	}

	t.Run("+Attributes", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			benchexp.processSpan(exs)
		}
	})

	exs.Annotations = []trace.Annotation{
		{Message: "1.500000", Attributes: map[string]interface{}{"key1": "value1"}},
		{Message: "Annotate", Attributes: map[string]interface{}{"key2": "value2"}},
	}

	t.Run("+Annotations", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			benchexp.processSpan(exs)
		}
	})

	exs.MessageEvents = []trace.MessageEvent{
		{EventType: 2, MessageID: 0x3, UncompressedByteSize: 0x190, CompressedByteSize: 0x12c},
		{EventType: 1, MessageID: 0x1, UncompressedByteSize: 0xc8, CompressedByteSize: 0x64},
	}

	t.Run("+MsgEvents", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			benchexp.processSpan(exs)
		}
	})
}

func BenchmarkProcessView(t *testing.B) {
	t.Run("Metric", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			benchexp.processView(vd1)
		}
	})
	t.Run("Distribution", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			benchexp.processView(vd2)
		}
	})
}
