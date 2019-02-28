package wavefront

import (
	"encoding/hex"
	"log"
	"strconv"

	wfsender "github.com/wavefronthq/wavefront-sdk-go/senders"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/trace"
	"google.golang.org/api/support/bundler"
)

type Options struct {
	Source string
}

type Exporter struct {
	sender  wfsender.Sender
	bundler *bundler.Bundler
	Options Options
}

const (
	nanoToMillis = 1000000

	spanKindKey = "SpanKind"

	// Annotations
	annoMsgKey = "log_msg"

	// Message Events
	msgIDKey     = "MsgID"
	msgTypeKey   = "MsgType"
	msgCmpSzKey  = "MsgCompressedByteSize"
	msgUcmpSzKey = "MsgUncompressedByteSize"
)

var (
	zeroUUID        = []byte("00000000-0000-0000-0000-000000000000")
	spanKindStrings = [...]string{
		"unspecified",
		"server",
		"client",
	}
	msgEventStrings = [...]string{
		"unspecified",
		"sent",
		"received",
	}
)

// NewExporter returns a trace.Exporter configured to upload traces and views
// to the configured wavefront instance (via Wavefront Sender)
//
// Documentation for Wavefront Sender is available at
// https://github.com/wavefrontHQ/wavefront-sdk-go
//
// Options adds additional options to the exporter.
func NewExporter(sender wfsender.Sender, o Options) (*Exporter, error) {
	exp := &Exporter{
		sender:  sender,
		Options: o,
	}
	bundler := bundler.NewBundler((*trace.SpanData)(nil), func(bundle interface{}) {
		exp.processBundle(bundle.([]*trace.SpanData))
	})
	exp.bundler = bundler

	return exp, nil
}

// ExportSpan exports given span to Wavefront
func (e *Exporter) ExportSpan(spanData *trace.SpanData) {
	err := e.bundler.Add(spanData, 1)
	if err != nil {
		log.Println("Error exporting span:", err)
	}
}

func (e *Exporter) Flush() {
	e.bundler.Flush()
	e.sender.Flush()
}

func (e *Exporter) convertAndSendSpan(sd *trace.SpanData) error {
	// Span Tags
	spanTags := make([](wfsender.SpanTag), 0, 2+len(sd.Attributes))
	for k, v := range sd.Attributes {
		spanTags = append(spanTags, wfsender.SpanTag{Key: k, Value: serialize(v)})
	}

	spanKind := sd.SpanKind
	if spanKind != trace.SpanKindUnspecified {
		spanTags = append(spanTags, wfsender.SpanTag{Key: spanKindKey,
			Value: enumString(sd.SpanKind, spanKindStrings[:])})
	}

	if sd.Status.Code != trace.StatusCodeOK {
		spanTags = append(spanTags, wfsender.SpanTag{Key: "error", Value: "true"})
	}
	//TODO: Send error code as tag?

	// Span Logs
	spanLogs := make([]wfsender.SpanLog, len(sd.Annotations)+len(sd.MessageEvents))
	for i, a := range sd.Annotations {
		annoTags := make(map[string]string, 1+len(a.Attributes))
		for k, v := range a.Attributes {
			annoTags[k] = serialize(v)
		}
		annoTags[annoMsgKey] = a.Message
		spanLogs[i].Timestamp = a.Time.UnixNano() / nanoToMillis
		spanLogs[i].Fields = annoTags
	}
	for i, m := range sd.MessageEvents {
		i2 := i + len(sd.Annotations)
		meTags := make(map[string]string, 4)
		meTags[msgIDKey] = serialize(m.MessageID)
		meTags[msgTypeKey] = enumString(int(m.EventType), msgEventStrings[:])
		meTags[msgCmpSzKey] = serialize(m.CompressedByteSize)
		meTags[msgUcmpSzKey] = serialize(m.UncompressedByteSize)
		spanLogs[i2].Timestamp = m.Time.UnixNano() / nanoToMillis
		spanLogs[i2].Fields = meTags
	}

	// Send
	return e.sender.SendSpan(
		sd.Name,
		sd.StartTime.UnixNano()/nanoToMillis,
		sd.EndTime.Sub(sd.StartTime).Nanoseconds()/nanoToMillis,
		e.Options.Source,
		serialize(sd.TraceID),
		serialize(sd.SpanID),
		[]string{serialize(sd.ParentSpanID)},
		nil,
		spanTags,
		spanLogs,
	)
}

func (e *Exporter) processBundle(sdbundle []*trace.SpanData) {
	for _, sd := range sdbundle {
		err := e.convertAndSendSpan(sd)
		if err != nil {
			log.Println("Error while uploading span: ", err)
		}
	}
}

func serialize(sval interface{}) string {
	switch val := sval.(type) {
	case trace.TraceID:
		// RFC4122 format
		b := [36]byte{}
		//copy(b[:], zeroUUID) // not needed
		hex.Encode(b[:], val[:4])
		hex.Encode(b[9:], val[4:6])
		hex.Encode(b[14:], val[6:8])
		hex.Encode(b[19:], val[8:10])
		hex.Encode(b[24:], val[10:])
		return string(b[:])
	case trace.SpanID:
		// RFC4122 format
		b := [36]byte{}
		copy(b[:], zeroUUID)
		hex.Encode(b[19:], val[:2])
		hex.Encode(b[24:], val[2:])
		return string(b[:])
	case string:
		return val
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case int64:
		return strconv.FormatInt(val, 10)
	case bool:
		return strconv.FormatBool(val)
	default:
		return "<unsupported value type>"
	}
}

func enumString(val int, enum []string) string {
	if val < 0 || val >= len(enum) {
		return "unknown"
	}
	return enum[val]
}

// TODO
func (e *Exporter) ExportView(viewData *view.Data) {

}
