package metrics

import (
	"embed"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

//go:embed ui/index.html
var uiFS embed.FS

// ControlHandler lets HTTP control endpoints act on a running job. It is
// optional; when nil, control endpoints return 501.
type ControlHandler interface {
	// Command applies "pause", "resume", or "stop" to the given job ID.
	Command(jobID, action string) error
}

// Handler builds the control-plane HTTP handler. control may be nil.
func Handler(reg *Registry, control ControlHandler) http.Handler {
	mux := http.NewServeMux()
	RegisterAPI(mux, reg, control)

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		data, err := uiFS.ReadFile("ui/index.html")
		if err != nil {
			http.Error(w, "ui unavailable", http.StatusInternalServerError)
			return
		}
		_, _ = w.Write(data)
	})

	return mux
}

// RegisterAPI wires the health, metrics, and status/stream/control endpoints
// onto mux — everything except the "/" dashboard page. The web console reuses
// this so it can serve its own config page at "/" while sharing the same live
// status API (/api/status, /api/stream) and cooperative control (/api/control).
func RegisterAPI(mux *http.ServeMux, reg *Registry, control ControlHandler) {
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if reg.Ready() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ready"))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("not ready"))
	})

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = w.Write([]byte(writePrometheus(reg)))
	})

	mux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(reg.Status())
	})

	mux.HandleFunc("/api/stream", func(w http.ResponseWriter, r *http.Request) {
		streamSSE(w, r, reg)
	})

	mux.HandleFunc("/api/control", func(w http.ResponseWriter, r *http.Request) {
		handleControl(w, r, control)
	})
}

func handleControl(w http.ResponseWriter, r *http.Request, control ControlHandler) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	if control == nil {
		http.Error(w, "control not available", http.StatusNotImplemented)
		return
	}
	var req struct {
		Job    string `json:"job"`
		Action string `json:"action"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	switch req.Action {
	case "pause", "resume", "stop":
	default:
		http.Error(w, "action must be pause|resume|stop", http.StatusBadRequest)
		return
	}
	if err := control.Command(req.Job, req.Action); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// streamSSE pushes a status snapshot every second until the client disconnects.
func streamSSE(w http.ResponseWriter, r *http.Request, reg *Registry) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	send := func() bool {
		data, err := json.Marshal(reg.Status())
		if err != nil {
			return true
		}
		if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
			return false
		}
		flusher.Flush()
		return true
	}

	if !send() {
		return
	}
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			if !send() {
				return
			}
		}
	}
}

// writePrometheus renders the registry in Prometheus text exposition format.
func writePrometheus(reg *Registry) string {
	var b strings.Builder

	help := func(name, typ, desc string) {
		b.WriteString("# HELP " + name + " " + desc + "\n")
		b.WriteString("# TYPE " + name + " " + typ + "\n")
	}
	metricLine := func(name, labels string, val float64) {
		b.WriteString(name + "{" + labels + "} " + strconv.FormatFloat(val, 'f', -1, 64) + "\n")
	}

	help("mongodb_migration_up", "gauge", "Whether the migrator is ready (1) or not (0).")
	up := 0.0
	if reg.Ready() {
		up = 1.0
	}
	b.WriteString("mongodb_migration_up " + strconv.FormatFloat(up, 'f', -1, 64) + "\n")

	cols := reg.Collections()

	help("mongodb_migration_docs_done", "gauge", "Documents migrated for a collection.")
	for _, m := range cols {
		metricLine("mongodb_migration_docs_done", labelsFor(m), float64(m.Snapshot.DoneDocs))
	}
	help("mongodb_migration_docs_total", "gauge", "Total documents to migrate for a collection.")
	for _, m := range cols {
		metricLine("mongodb_migration_docs_total", labelsFor(m), float64(m.Snapshot.TotalDocs))
	}
	help("mongodb_migration_bytes_done", "gauge", "Bytes migrated for a collection.")
	for _, m := range cols {
		metricLine("mongodb_migration_bytes_done", labelsFor(m), float64(m.Snapshot.DoneBytes))
	}
	help("mongodb_migration_bytes_total", "gauge", "Total bytes to migrate for a collection.")
	for _, m := range cols {
		metricLine("mongodb_migration_bytes_total", labelsFor(m), float64(m.Snapshot.TotalBytes))
	}
	help("mongodb_migration_bytes_per_second", "gauge", "EWMA-smoothed throughput in bytes/sec.")
	for _, m := range cols {
		metricLine("mongodb_migration_bytes_per_second", labelsFor(m), m.Snapshot.BytesPerSec)
	}
	help("mongodb_migration_percent", "gauge", "Completion percent (byte-based, doc-based fallback).")
	for _, m := range cols {
		pct := m.Snapshot.PercentBytes
		if m.Snapshot.TotalBytes == 0 {
			pct = m.Snapshot.PercentDocs
		}
		metricLine("mongodb_migration_percent", labelsFor(m), pct)
	}
	help("mongodb_migration_eta_seconds", "gauge", "Estimated seconds remaining (-1 if unknown).")
	for _, m := range cols {
		eta := m.Snapshot.ETA.Seconds()
		if m.Snapshot.ETA < 0 {
			eta = -1
		}
		metricLine("mongodb_migration_eta_seconds", labelsFor(m), eta)
	}
	help("mongodb_migration_lag_seconds", "gauge", "Live replication lag in seconds (-1 if unknown).")
	for _, m := range cols {
		metricLine("mongodb_migration_lag_seconds", labelsFor(m), m.LagSeconds)
	}
	return b.String()
}

func labelsFor(m CollectionMetric) string {
	return fmt.Sprintf(`job=%q,database=%q,collection=%q,phase=%q`,
		escapeLabel(m.Job), escapeLabel(m.Database), escapeLabel(m.Collection), escapeLabel(m.Phase))
}

// escapeLabel escapes backslashes, quotes, and newlines per the Prometheus text format.
func escapeLabel(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	s = strings.ReplaceAll(s, "\n", `\n`)
	return s
}
