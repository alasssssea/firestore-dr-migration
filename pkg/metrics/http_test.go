package metrics

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"firestore-dr-migration/pkg/progress"
)

func testRegistry() *Registry {
	r := NewRegistry()
	r.SetReady(true)
	r.UpsertJob("job-1", "migrate", StateInitialLoad)
	r.SetCollection(CollectionMetric{
		Job: "job-1", Database: "shop", Collection: "orders", Phase: "initial",
		Snapshot: progress.Snapshot{
			TotalDocs: 1000, DoneDocs: 400, TotalBytes: 2048, DoneBytes: 1024,
			PercentBytes: 50, BytesPerSec: 512,
		},
		LagSeconds: -1,
	})
	return r
}

func TestHealthz(t *testing.T) {
	h := Handler(testRegistry(), nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("healthz code = %d", rec.Code)
	}
}

func TestReadyz(t *testing.T) {
	reg := NewRegistry()
	h := Handler(reg, nil)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("not-ready readyz code = %d, want 503", rec.Code)
	}

	reg.SetReady(true)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("ready readyz code = %d, want 200", rec.Code)
	}
}

func TestPrometheusExposition(t *testing.T) {
	h := Handler(testRegistry(), nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		"mongodb_migration_up 1",
		`mongodb_migration_docs_done{job="job-1",database="shop",collection="orders",phase="initial"} 400`,
		"mongodb_migration_bytes_per_second{",
		"# TYPE mongodb_migration_percent gauge",
		"mongodb_migration_lag_seconds{",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics missing %q\n---\n%s", want, body)
		}
	}
}

func TestStatusJSON(t *testing.T) {
	h := Handler(testRegistry(), nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/status", nil))
	var st Status
	if err := json.Unmarshal(rec.Body.Bytes(), &st); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !st.Ready || len(st.Collections) != 1 || st.Collections[0].Snapshot.DoneDocs != 400 {
		t.Errorf("unexpected status JSON: %+v", st)
	}
	// The UI relies on lowercase snapshot keys.
	if !strings.Contains(rec.Body.String(), `"doneDocs":400`) {
		t.Errorf("expected lowercase snapshot json keys, got %s", rec.Body.String())
	}
}

type fakeControl struct{ got []string }

func (f *fakeControl) Command(job, action string) error {
	f.got = append(f.got, job+":"+action)
	return nil
}

func TestControlEndpoint(t *testing.T) {
	fc := &fakeControl{}
	h := Handler(testRegistry(), fc)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/control", strings.NewReader(`{"job":"job-1","action":"pause"}`))
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("control code = %d", rec.Code)
	}
	if len(fc.got) != 1 || fc.got[0] != "job-1:pause" {
		t.Errorf("control not applied: %v", fc.got)
	}

	// Bad action rejected.
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/control", strings.NewReader(`{"job":"j","action":"explode"}`))
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("bad action code = %d, want 400", rec.Code)
	}
}

func TestControlEndpointNotImplemented(t *testing.T) {
	h := Handler(testRegistry(), nil)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/control", strings.NewReader(`{"job":"j","action":"pause"}`))
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotImplemented {
		t.Errorf("nil control code = %d, want 501", rec.Code)
	}
}

func TestUIServed(t *testing.T) {
	h := Handler(testRegistry(), nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "迁移控制台") {
		t.Errorf("UI not served, code=%d", rec.Code)
	}
}
