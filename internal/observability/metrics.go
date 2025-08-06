package observability

import "github.com/prometheus/client_golang/prometheus"

const (
	namespace     = "dfs"
	subsystemRPC  = "rpc"
	metricPutName = "put_total"
	metricGetName = "get_total"
	helpPut       = "Total number of Put RPCs"
	helpGet       = "Total number of Get RPCs"
)

var (
	// PutCounter counts Put RPC invocations.
	PutCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystemRPC,
		Name:      metricPutName,
		Help:      helpPut,
	})
	// GetCounter counts Get RPC invocations.
	GetCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystemRPC,
		Name:      metricGetName,
		Help:      helpGet,
	})
)

// Register registers all observability metrics.
func Register() {
	prometheus.MustRegister(PutCounter, GetCounter)
}
