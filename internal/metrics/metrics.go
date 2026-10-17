// Package metrics is the server's Prometheus instrumentation.
//
// It is one struct rather than a set of package-level variables, and it is
// passed to the packages that report into it. That costs a field in two configs
// and buys two things: a test can build a fresh set against its own registry
// and assert on what was recorded, and Nop exists, so no caller ever has to
// check for nil before counting something.
//
// What is measured is what a person on call would ask about: how many
// connections and rooms there are, how long integrating an update takes, how
// long the database takes, and what the server is refusing. Everything else is
// noise that costs cardinality.
package metrics

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds every collector the server reports into.
type Metrics struct {
	ConnectionsOpen   prometheus.Gauge
	ConnectionsTotal  prometheus.Counter
	ConnectionsClosed *prometheus.CounterVec
	RoomsResident     prometheus.Gauge
	RoomsStarted      prometheus.Counter

	MessagesReceived *prometheus.CounterVec
	FramesSent       prometheus.Counter
	FramesDropped    prometheus.Counter
	BytesReceived    prometheus.Counter
	BytesSent        prometheus.Counter

	// Throttled counts frames that had to wait for their connection's rate
	// limit, and ThrottledSeconds the time spent waiting. Both being zero is the
	// normal state; either one growing means somebody is sending faster than the
	// server is willing to read.
	Throttled        prometheus.Counter
	ThrottledSeconds prometheus.Counter

	ApplyDuration prometheus.Histogram
	ApplyFailed   prometheus.Counter
	UpdateBytes   prometheus.Histogram
	Denied        *prometheus.CounterVec

	StoreDuration *prometheus.HistogramVec
	StoreFailed   *prometheus.CounterVec
	Compactions   prometheus.Counter
	LoadDuration  prometheus.Histogram
	// Versions counts history rows written. A document being edited produces
	// one an interval; a flat line while people are editing means versioning
	// is off or failing.
	Versions prometheus.Counter

	// Hooks are the webhook deliveries. Dropped is the one to watch: it means
	// the receiver was slow enough that events were thrown away rather than
	// allowed to slow a document down.
	HooksSent    *prometheus.CounterVec
	HooksFailed  *prometheus.CounterVec
	HooksDropped prometheus.Counter
	HookAttempts prometheus.Counter
	HookDuration prometheus.Histogram
	HookQueue    prometheus.Gauge

	// Auth is the external authorization callback. The result label separates
	// the three things an operator has to tell apart: the endpoint refusing
	// somebody, the endpoint being unreachable, and the endpoint answering
	// something this server cannot act on.
	AuthRequests *prometheus.CounterVec
	AuthCache    *prometheus.CounterVec
	AuthDuration prometheus.Histogram

	// BlobBytes counts what the snapshots and version payloads weighed before
	// and after compression. Two counters rather than a ratio, because a ratio
	// cannot be summed across replicas and this one is a claim about cost that
	// an operator should be able to check against their own documents.
	BlobBytes *prometheus.CounterVec

	// RoomsEvicted counts rooms dropped from memory, by what pushed them out.
	// "idle" is the ordinary case; "budget" and "cap" mean a bound was reached,
	// which is the difference between a server with headroom and one at its
	// limit.
	RoomsEvicted *prometheus.CounterVec
}

// New registers a set of collectors and returns them.
//
// A registry is taken rather than assumed, because the alternative - the
// default registry - makes two tests in one binary collide.
func New(reg prometheus.Registerer) *Metrics {
	factory := promauto(reg)
	return &Metrics{
		ConnectionsOpen: factory.gauge(prometheus.GaugeOpts{
			Name: "ycollab_connections_open",
			Help: "WebSocket connections currently being served.",
		}),
		ConnectionsTotal: factory.counter(prometheus.CounterOpts{
			Name: "ycollab_connections_total",
			Help: "WebSocket connections accepted since start.",
		}),
		ConnectionsClosed: factory.counterVec(prometheus.CounterOpts{
			Name: "ycollab_connections_closed_total",
			Help: "Connections closed, by WebSocket close code.",
		}, []string{"code"}),
		RoomsResident: factory.gauge(prometheus.GaugeOpts{
			Name: "ycollab_rooms_resident",
			Help: "Documents currently held in memory.",
		}),
		RoomsStarted: factory.counter(prometheus.CounterOpts{
			Name: "ycollab_rooms_started_total",
			Help: "Rooms created since start.",
		}),

		MessagesReceived: factory.counterVec(prometheus.CounterOpts{
			Name: "ycollab_messages_received_total",
			Help: "Client messages, by protocol message type.",
		}, []string{"type"}),
		FramesSent: factory.counter(prometheus.CounterOpts{
			Name: "ycollab_frames_sent_total",
			Help: "Frames queued for clients.",
		}),
		FramesDropped: factory.counter(prometheus.CounterOpts{
			Name: "ycollab_frames_dropped_total",
			Help: "Frames a connection could not accept, each of which closes it with 1008.",
		}),
		BytesReceived: factory.counter(prometheus.CounterOpts{
			Name: "ycollab_bytes_received_total",
			Help: "Bytes read from clients.",
		}),
		BytesSent: factory.counter(prometheus.CounterOpts{
			Name: "ycollab_bytes_sent_total",
			Help: "Bytes written to clients.",
		}),

		Throttled: factory.counter(prometheus.CounterOpts{
			Name: "ycollab_throttled_total",
			Help: "Frames that had to wait for their connection's rate limit.",
		}),
		ThrottledSeconds: factory.counter(prometheus.CounterOpts{
			Name: "ycollab_throttled_seconds_total",
			Help: "Time spent waiting on rate limits, summed across connections.",
		}),

		ApplyDuration: factory.histogram(prometheus.HistogramOpts{
			Name: "ycollab_apply_duration_seconds",
			Help: "Time to integrate one update into a document.",
			// The interesting range is tens of microseconds to a few
			// milliseconds: this is CPU work on an in-memory structure, and the
			// default buckets start at 5 ms, where everything would land in the
			// first one.
			Buckets: prometheus.ExponentialBuckets(0.00002, 3, 10),
		}),
		ApplyFailed: factory.counter(prometheus.CounterOpts{
			Name: "ycollab_apply_failed_total",
			Help: "Updates that would not integrate.",
		}),
		UpdateBytes: factory.histogram(prometheus.HistogramOpts{
			Name:    "ycollab_update_bytes",
			Help:    "Size of the updates clients send.",
			Buckets: prometheus.ExponentialBuckets(16, 4, 9),
		}),
		Denied: factory.counterVec(prometheus.CounterOpts{
			Name: "ycollab_denied_total",
			Help: "Requests and messages refused, by reason.",
		}, []string{"reason"}),

		StoreDuration: factory.histogramVec(prometheus.HistogramOpts{
			Name:    "ycollab_store_duration_seconds",
			Help:    "Time spent on a storage operation, including retries, by operation.",
			Buckets: prometheus.ExponentialBuckets(0.0005, 3, 10),
		}, []string{"op"}),
		StoreFailed: factory.counterVec(prometheus.CounterOpts{
			Name: "ycollab_store_failed_total",
			Help: "Database operations that failed, by operation.",
		}, []string{"op"}),
		Compactions: factory.counter(prometheus.CounterOpts{
			Name: "ycollab_compactions_total",
			Help: "Update logs folded into a snapshot.",
		}),
		Versions: factory.counter(prometheus.CounterOpts{
			Name: "ycollab_versions_total",
			Help: "Document versions written to the history.",
		}),
		LoadDuration: factory.histogram(prometheus.HistogramOpts{
			Name:    "ycollab_document_load_seconds",
			Help:    "Time to read a document and replay its log at room start.",
			Buckets: prometheus.ExponentialBuckets(0.001, 3, 10),
		}),

		HooksSent: factory.counterVec(prometheus.CounterOpts{
			Name: "ycollab_hooks_sent_total",
			Help: "Webhook events the receiver accepted, by event.",
		}, []string{"event"}),
		HooksFailed: factory.counterVec(prometheus.CounterOpts{
			Name: "ycollab_hooks_failed_total",
			Help: "Webhook events given up on, by event and reason.",
		}, []string{"event", "reason"}),
		HooksDropped: factory.counter(prometheus.CounterOpts{
			Name: "ycollab_hooks_dropped_total",
			Help: "Webhook events discarded because the delivery queue was full.",
		}),
		HookAttempts: factory.counter(prometheus.CounterOpts{
			Name: "ycollab_hook_attempts_total",
			Help: "HTTP requests made to the webhook endpoint, including retries.",
		}),
		HookDuration: factory.histogram(prometheus.HistogramOpts{
			Name:    "ycollab_hook_duration_seconds",
			Help:    "Time for one request to the webhook endpoint.",
			Buckets: prometheus.ExponentialBuckets(0.001, 3, 10),
		}),
		HookQueue: factory.gauge(prometheus.GaugeOpts{
			Name: "ycollab_hook_queue_depth",
			Help: "Webhook events waiting to be delivered.",
		}),

		AuthRequests: factory.counterVec(prometheus.CounterOpts{
			Name: "ycollab_auth_requests_total",
			Help: "Calls to the authorization endpoint, by result: allow, deny, error or misconfigured.",
		}, []string{"result"}),
		AuthCache: factory.counterVec(prometheus.CounterOpts{
			Name: "ycollab_auth_cache_total",
			Help: "Authorization decisions answered from the cache or not, by result: hit or miss.",
		}, []string{"result"}),
		AuthDuration: factory.histogram(prometheus.HistogramOpts{
			Name:    "ycollab_auth_duration_seconds",
			Help:    "Time for one call to the authorization endpoint. A client is waiting on this.",
			Buckets: prometheus.ExponentialBuckets(0.001, 3, 10),
		}),

		RoomsEvicted: factory.counterVec(prometheus.CounterOpts{
			Name: "ycollab_rooms_evicted_total",
			Help: "Documents dropped from memory, by reason: idle, cap or budget.",
		}, []string{"reason"}),

		BlobBytes: factory.counterVec(prometheus.CounterOpts{
			Name: "ycollab_store_blob_bytes_total",
			Help: "Snapshot and version bytes written, by state: raw before compression, stored after.",
		}, []string{"state"}),
	}
}

// Nop returns a set of collectors registered nowhere, for tests and for code
// paths that run without a metrics endpoint. Counting into it is a few atomic
// adds and nothing else, which is cheaper than a nil check at every call site
// and much harder to get wrong.
func Nop() *Metrics { return New(nil) }

// CloseCode records a connection ending with a WebSocket close code.
//
// The code is turned into a string here rather than at the call sites so the
// label values are a small fixed set: an unbounded label is how a metrics
// endpoint becomes the thing that takes the server down.
func (m *Metrics) CloseCode(code int) {
	m.ConnectionsClosed.WithLabelValues(strconv.Itoa(code)).Inc()
}

// Observe records how long something took, in seconds.
func Observe(h prometheus.Observer, since time.Time) {
	h.Observe(time.Since(since).Seconds())
}

// factory registers collectors, tolerating a nil registerer.
type factory struct{ reg prometheus.Registerer }

func promauto(reg prometheus.Registerer) factory { return factory{reg} }

func (f factory) register(c prometheus.Collector) {
	if f.reg == nil {
		return
	}
	f.reg.MustRegister(c)
}

func (f factory) gauge(opts prometheus.GaugeOpts) prometheus.Gauge {
	c := prometheus.NewGauge(opts)
	f.register(c)
	return c
}

func (f factory) counter(opts prometheus.CounterOpts) prometheus.Counter {
	c := prometheus.NewCounter(opts)
	f.register(c)
	return c
}

func (f factory) counterVec(opts prometheus.CounterOpts, labels []string) *prometheus.CounterVec {
	c := prometheus.NewCounterVec(opts, labels)
	f.register(c)
	return c
}

func (f factory) histogram(opts prometheus.HistogramOpts) prometheus.Histogram {
	c := prometheus.NewHistogram(opts)
	f.register(c)
	return c
}

func (f factory) histogramVec(opts prometheus.HistogramOpts, labels []string) *prometheus.HistogramVec {
	c := prometheus.NewHistogramVec(opts, labels)
	f.register(c)
	return c
}
