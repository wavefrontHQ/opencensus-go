package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"go.opencensus.io/exporter/wavefront"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"go.opencensus.io/trace"

	"github.com/wavefronthq/wavefront-sdk-go/application"
	wfsender "github.com/wavefronthq/wavefront-sdk-go/senders"
)

var (
	invCount = stats.Int64("invocation_count", "", stats.UnitDimensionless)
	workSize = stats.Int64("work_size", "", stats.UnitBytes)
	wg       = sync.WaitGroup{}
)

func main() {
	ctx := context.Background()

	// Configure Wavefront Sender
	proxyCfg := &wfsender.ProxyConfiguration{
		Host: "localhost",

		// At least one port should be set below.
		MetricsPort:      30000, // set this (typically 2878) to send metrics
		DistributionPort: 40000, // set this (typically 40000) to send distributions
		TracingPort:      50000, // set this to send tracing spans

		FlushIntervalSeconds: 1, // flush the buffer periodically, defaults to 5 seconds.
	}
	sender, _ := wfsender.NewProxySender(proxyCfg)

	appTags := application.New("test-app", "test-service")

	// Configure Wavefront Exporter
	qSize := 100
	exporter, _ := wavefront.NewExporter(sender, wavefront.QueueSize(qSize), wavefront.AppTags(appTags))

	// Register view exporter
	view.RegisterExporter(exporter)
	view.SetReportingPeriod(time.Second)
	fkey, _ := tag.NewKey("func_name")
	ukey, _ := tag.NewKey("unset_tag")
	if err := view.Register(
		&view.View{
			Name:        "invocation_count",
			Description: "",
			Measure:     invCount,
			TagKeys:     []tag.Key{fkey, ukey},
			Aggregation: view.Count(),
		},
		&view.View{
			Name:        "work_size_gauge",
			Description: "",
			Measure:     workSize,
			Aggregation: view.LastValue(),
		},
		&view.View{
			Name:        "work_size_sum",
			Description: "",
			Measure:     workSize,
			Aggregation: view.Sum(),
		},
		&view.View{
			Name:        "work_size_dist",
			Description: "",
			Measure:     workSize,
			Aggregation: view.Distribution(25, 50, 75, 100),
		},
	); err != nil {
		log.Fatalf("Cannot register views: %v", err)
	}

	// Register trace exporter
	trace.RegisterExporter(exporter)
	trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})

	ctx, span := trace.StartSpan(ctx, "main", trace.WithSpanKind(trace.SpanKindClient))
	span.AddMessageReceiveEvent(6789, 100, 50)
	//span.AddAttributes(trace.StringAttribute("parent", "none"))
	span.Annotate(nil, "just before work()")
	q := []int64{155, 33, 43, 55, 60, 90, 49, 58, 89, 57, 68, 43, 90, 98, 100}
	for i := 0; i < qSize-1; i++ {
		wg.Add(1)
		stats.RecordWithTags(ctx, []tag.Mutator{tag.Upsert(fkey, "work()")}, invCount.M(1), workSize.M(q[i%len(q)]))
		go work(ctx, i)
	}
	wg.Wait()
	span.End()

	time.Sleep(1 * time.Second)
	exporter.Flush()
}

func work(ctx context.Context, index int) {
	defer wg.Done()
	ctx, span := trace.StartSpan(ctx, fmt.Sprintf("work-%d", index))
	defer span.End()

	// Do work
	a := []trace.Attribute{}
	time.Sleep(34 * time.Millisecond)
	a = append(a, trace.Int64Attribute("phase1_set_duration", 34))
	span.Annotate(nil, "phase1_complete")
	time.Sleep(200 * time.Millisecond)
	a = append(a, trace.Int64Attribute("phase2_set_duration", 200))
	span.Annotate(a, "phase2_complete")
	span.AddAttributes(trace.Int64Attribute("total_set_duration", 234))
}
