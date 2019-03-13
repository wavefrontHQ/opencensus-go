package wavefront

import (
	"bytes"
	"encoding/hex"
	"strconv"
	"sync/atomic"

	wfsender "github.com/wavefronthq/wavefront-sdk-go/senders"
	"go.opencensus.io/trace"
)

const (
	// Span Tags
	spanKindKey   = "span.kind"
	errTagKey     = "error"
	errCodeTagKey = "error_code"

	// Span Logs
	spanLogErrMsgKey = "message"
	spanLogEventKey  = "event"
	annoMsgKey       = "log_msg"
	msgIDKey         = "MsgID"
	msgTypeKey       = "MsgType"
	msgCmpSzKey      = "MsgCompressedByteSize"
	msgUcmpSzKey     = "MsgUncompressedByteSize"
)

var (
	zeroSpanID      = [8]byte(trace.SpanID{})
	zeroUUID        = []byte("00000000-0000-0000-0000-000000000000")
	spanKindStrings = [...]string{
		"unspecified",
		"server",
		"client",
	}

	// Status codes from gRPC.
	// https://github.com/googleapis/googleapis/blob/master/google/rpc/code.proto
	statusCodeStrings = [...]string{
		"OK",
		"Cancelled",
		"Unknown",
		"InvalidArgument",
		"DeadlineExceeded",
		"NotFound",
		"AlreadyExists",
		"PermissionDenied",
		"ResourceExhausted",
		"FailedPrecondition",
		"Aborted",
		"OutOfRange",
		"Unimplemented",
		"Internal",
		"Unavailable",
		"DataLoss",
		"Unauthenticated",
	}

	msgEventStrings = [...]string{
		"unspecified",
		"sent",
		"received",
	}
)

func (e *Exporter) processSpan(sd *trace.SpanData) {
	// Span Tags
	appTags := e.appMap
	spanTags := make([](wfsender.SpanTag), 0, 3+len(sd.Attributes)+len(appTags))
	for k, v := range sd.Attributes {
		spanTags = append(spanTags, wfsender.SpanTag{Key: k, Value: serialize(v)})
	}
	for k, v := range appTags {
		spanTags = append(spanTags, wfsender.SpanTag{Key: k, Value: v})
	}

	spanKind := sd.SpanKind
	if spanKind != trace.SpanKindUnspecified {
		spanTags = append(spanTags, wfsender.SpanTag{Key: spanKindKey,
			Value: enumString(sd.SpanKind, spanKindStrings[:])})
	}

	if sd.Status.Code != trace.StatusCodeOK {
		spanTags = append(spanTags,
			wfsender.SpanTag{Key: errTagKey, Value: "true"},
			wfsender.SpanTag{Key: errCodeTagKey, Value: enumString(int(sd.Status.Code), statusCodeStrings[:])},
		)
	}

	// Span Logs
	spanLogs := make([]wfsender.SpanLog, 0, 1+len(sd.Annotations)+len(sd.MessageEvents))

	if sd.Status.Code != trace.StatusCodeOK && sd.Status.Message != "" {
		spanLogs = append(spanLogs, wfsender.SpanLog{
			Timestamp: sd.EndTime.UnixNano() / nanoToMillis,
			Fields: map[string]string{
				spanLogErrMsgKey: sd.Status.Message,
				spanLogEventKey:  errTagKey,
			},
		})
	}

	for _, a := range sd.Annotations {
		annoTags := make(map[string]string, 1+len(a.Attributes))
		annoTags[annoMsgKey] = a.Message
		for k, v := range a.Attributes {
			annoTags[k] = serialize(v)
		}
		spanLogs = append(spanLogs, wfsender.SpanLog{
			Timestamp: a.Time.UnixNano() / nanoToMillis,
			Fields:    annoTags,
		})
	}
	for _, m := range sd.MessageEvents {
		meTags := map[string]string{
			msgIDKey:     serialize(m.MessageID),
			msgTypeKey:   enumString(int(m.EventType), msgEventStrings[:]),
			msgCmpSzKey:  serialize(m.CompressedByteSize),
			msgUcmpSzKey: serialize(m.UncompressedByteSize),
		}
		spanLogs = append(spanLogs, wfsender.SpanLog{
			Timestamp: m.Time.UnixNano() / nanoToMillis,
			Fields:    meTags,
		})
	}

	startTime := sd.StartTime.UnixNano() / nanoToMillis
	endTime := sd.EndTime.Sub(sd.StartTime).Nanoseconds() / nanoToMillis
	traceId := serialize(sd.TraceID)
	spanId := serialize(sd.SpanID)
	var parents []string
	pspanBytes := [8]byte(sd.ParentSpanID)
	if !bytes.Equal(zeroSpanID[:], pspanBytes[:]) { //don't add parent in case of root span
		parents = []string{serialize(sd.ParentSpanID)}
	}

	cmd := func() {
		defer e.semRelease()

		e.logError("Error sending span: ", e.sender.SendSpan(
			sd.Name,
			startTime, endTime,
			e.Source,
			traceId, spanId, parents, nil,
			spanTags, spanLogs,
		))
	}

	if !e.queueCmd(cmd) {
		atomic.AddUint64(&e.spansDropped, 1)
	}
}

func serialize(sval interface{}) string {
	switch val := sval.(type) {
	case trace.TraceID:
		// RFC4122 format
		b := [36]byte{}
		copy(b[:], zeroUUID) //TODO: directly set '-'?
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
