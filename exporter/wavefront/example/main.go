package main

import (
	"context"
	"fmt"
	"time"

	wfsender "github.com/wavefronthq/wavefront-sdk-go/senders"
	"go.opencensus.io/exporter/wavefront"
	"go.opencensus.io/trace"
)

func main() {
	ctx := context.Background()

	// Configure Wavefront Sender
	proxyCfg := &wfsender.ProxyConfiguration{
		Host: "localhost",

		// At least one port should be set below.
		MetricsPort:      2878,  // set this (typically 2878) to send metrics
		DistributionPort: 40000, // set this (typically 40000) to send distributions
		TracingPort:      50000, // set this to send tracing spans

		FlushIntervalSeconds: 10, // flush the buffer periodically, defaults to 5 seconds.
	}
	sender, _ := wfsender.NewProxySender(proxyCfg)

	// Configure Wavefront Exporter
	exporter, _ := wavefront.NewExporter(sender, wavefront.Options{})

	// Register Exporter
	trace.RegisterExporter(exporter)
	trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})

	ctx, span := trace.StartSpan(ctx, "main", trace.WithSpanKind(trace.SpanKindClient))
	span.AddMessageReceiveEvent(6789, 100, 50)
	span.AddAttributes(trace.StringAttribute("parent", "none"))
	span.Annotate(nil, "just before work()")
	for i := 0; i < 5; i++ {
		work(ctx, i)
	}
	span.End()

	exporter.Flush()
}

func work(ctx context.Context, index int) {
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
