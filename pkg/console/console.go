// Package console serves a browser-based configuration and control UI for the
// migration tool. It turns the same steps the CLI wizard walks through —
// detect the source, pick databases/collections, point at a Firestore target,
// choose a mode — into a web form, then launches the migration in-process and
// streams live progress through the shared metrics control plane.
package console

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"

	"firestore-dr-migration/pkg/assess"
	"firestore-dr-migration/pkg/config"
	"firestore-dr-migration/pkg/db"
	"firestore-dr-migration/pkg/logger"
	"firestore-dr-migration/pkg/metrics"
	"firestore-dr-migration/pkg/migration"
	"firestore-dr-migration/pkg/remediate"
	"firestore-dr-migration/pkg/util"
	"firestore-dr-migration/pkg/verify"
	"firestore-dr-migration/pkg/wizard"
)

//go:embed ui/console.html
var uiFS embed.FS

// Server holds the console's shared metrics registry and the single migration
// job it runs at a time (mirroring the CLI's one-job-per-process model).
type Server struct {
	log *logger.Logger
	reg *metrics.Registry

	mu      sync.Mutex
	control *metrics.Control
	cancel  context.CancelFunc
	jobID   string
	running bool
	// migrator is the currently-running job's engine, retained so /api/reconfig can
	// hot-adjust its concurrency/tuning live. nil when no job is running.
	migrator *migration.Migrator
	// jobSeq makes every launched job id unique. time.Now().Unix() alone is
	// second-resolution, so a stop-then-restart within the same second would mint
	// the SAME id — UpsertJob would then update the old job row instead of
	// creating a new one, and the operator's "brand-new task" would silently reuse
	// the previous one. The monotonic suffix rules that out.
	jobSeq uint64
	// done is closed by the launch goroutine when the current job has FULLY
	// unwound (migrator.Start returned and s.running was cleared). Stop waits on
	// it so a subsequent Start doesn't race the previous job's teardown — the
	// modern live path takes a moment to tear down the change stream, workers and
	// connections after cancellation, and until it returns s.running stays true.
	done chan struct{}

	// planMu guards the active remediation plan, which is shared between the
	// assessment endpoint (in-memory simulation) and the /api/remediate endpoint
	// (add/remove a fix). It is persisted to planFile so it survives restarts and
	// is picked up by the CLI/migrator.
	planMu   sync.Mutex
	plan     *remediate.Plan
	planFile string
}

// Serve starts the console HTTP server and blocks until it exits.
func Serve(log *logger.Logger, addr string) error {
	s := &Server{log: log, reg: metrics.NewRegistry(), planFile: remediate.DefaultPlanFile}
	s.reg.SetReady(true)

	// Load any persisted remediation plan so applied fixes survive a restart.
	plan, err := remediate.Load(s.planFile)
	if err != nil {
		log.Warnf("Could not load remediation plan %s: %v", s.planFile, err)
		plan = &remediate.Plan{}
	}
	s.plan = plan

	mux := http.NewServeMux()
	// Share the dashboard's status/stream/control API (everything but "/").
	metrics.RegisterAPI(mux, s.reg, s)
	mux.HandleFunc("/api/detect", s.handleDetect)
	mux.HandleFunc("/api/firestore/databases", s.handleFirestoreList)
	mux.HandleFunc("/api/firestore/locations", s.handleFirestoreLocations)
	mux.HandleFunc("/api/firestore/create", s.handleFirestoreCreate)
	mux.HandleFunc("/api/assess", s.handleAssess)
	mux.HandleFunc("/api/remediate", s.handleRemediate)
	mux.HandleFunc("/api/verify", s.handleVerify)
	mux.HandleFunc("/api/dlq", s.handleDLQ)
	mux.HandleFunc("/api/reset", s.handleReset)
	mux.HandleFunc("/api/start", s.handleStart)
	mux.HandleFunc("/api/reconfig", s.handleReconfig)
	mux.HandleFunc("/", s.handleIndex)

	log.Infof("Migration console listening on %s — open http://localhost%s/ in a browser", addr, addr)
	return http.ListenAndServe(addr, mux)
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	data, err := uiFS.ReadFile("ui/console.html")
	if err != nil {
		http.Error(w, "console UI unavailable", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(data)
}

// detectResponse is the JSON returned to the config page after probing a source.
type detectResponse struct {
	Version           string              `json:"version"`
	IsReplicaSet      bool                `json:"isReplicaSet"`
	ModernDriver      bool                `json:"modernDriver"`
	ReplicationMethod string              `json:"replicationMethod"`
	Warning           string              `json:"warning,omitempty"`
	Databases         map[string][]string `json:"databases"`
	InventoryError    string              `json:"inventoryError,omitempty"`
	// PlanReset is true when detecting this source invalidated a remediation plan
	// that had been built against a DIFFERENT source (fingerprint mismatch). The
	// prior plan is backed up to PlanBackup first. This stops one source's fixes
	// from silently carrying over to another that shares db/collection names.
	PlanReset  bool   `json:"planReset,omitempty"`
	PlanBackup string `json:"planBackup,omitempty"`
}

func (s *Server) handleDetect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Source string `json:"source"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if strings.TrimSpace(req.Source) == "" {
		writeError(w, http.StatusBadRequest, "source connection string is required")
		return
	}

	info, err := db.DetectSourceServer(req.Source, "admin")
	if err != nil {
		writeError(w, http.StatusBadGateway, fmt.Sprintf("cannot connect to source: %v", err))
		return
	}
	decision := migration.ResolveReplicationMethod(info)

	resp := detectResponse{
		Version:           info.Version,
		IsReplicaSet:      info.IsReplicaSet,
		ModernDriver:      info.ModernDriver,
		ReplicationMethod: decision.Method,
		Warning:           decision.Warning,
	}
	inv, invErr := db.ListInventory(req.Source, info.ModernDriver)
	if invErr != nil {
		resp.InventoryError = invErr.Error()
	} else {
		resp.Databases = inv
	}

	// Auto-invalidate a stale remediation plan when the operator points at a
	// different source. The plan is keyed only by (rule, db, collection) with no
	// source identity, so without this a plan built for source A would silently
	// apply to source B if they share database/collection names. We fingerprint
	// the source (host:port + database set, never credentials) and clear+back up
	// the plan on mismatch. We only fingerprint from a successful inventory so a
	// transient inventory error can't wipe a valid plan.
	if invErr == nil {
		fp := sourceFingerprint(req.Source, inv)
		s.planMu.Lock()
		if s.plan == nil {
			s.plan = &remediate.Plan{}
		}
		if len(s.plan.Items) > 0 && s.plan.Source != fp {
			if bak, err := backupFile(s.planFile); err != nil {
				s.log.Warnf("Could not back up remediation plan before source-change reset: %v", err)
			} else if bak != "" {
				resp.PlanBackup = bak
			}
			s.plan = &remediate.Plan{}
			resp.PlanReset = true
			s.log.Infof("Source changed (fingerprint mismatch) — cleared stale remediation plan (backup: %s)", resp.PlanBackup)
		}
		// Stamp the current source's fingerprint so later detects compare against it
		// (also upgrades pre-fingerprint plans in place).
		s.plan.Source = fp
		if err := s.plan.Save(s.planFile); err != nil {
			s.log.Warnf("Could not persist remediation plan fingerprint: %v", err)
		}
		s.planMu.Unlock()
	}
	writeJSON(w, http.StatusOK, resp)
}

// sourceFingerprint derives a stable, credential-free identifier for a source:
// the sorted host:port list from the connection string plus the sorted set of
// database names discovered on it, hashed. Two connection strings that reach the
// same servers and see the same databases fingerprint identically (so a same-
// source restart keeps its plan); pointing at a different server or a different
// set of databases changes it (so a stale plan is dropped).
func sourceFingerprint(connStr string, inv map[string][]string) string {
	var hosts []string
	if cs, err := connstring.Parse(connStr); err == nil {
		hosts = append(hosts, cs.Hosts...)
	}
	sort.Strings(hosts)
	dbs := make([]string, 0, len(inv))
	for name := range inv {
		dbs = append(dbs, name)
	}
	sort.Strings(dbs)
	h := sha256.Sum256([]byte(strings.Join(hosts, ",") + "|" + strings.Join(dbs, ",")))
	return hex.EncodeToString(h[:])
}

// handleFirestoreList returns the project's Firestore databases so the config
// page can offer them as mapping targets.
func (s *Server) handleFirestoreList(w http.ResponseWriter, r *http.Request) {
	dbs, err := ListFirestoreDatabases(r.Context())
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"databases": dbs})
}

// handleFirestoreLocations returns the single-region Firestore locations the
// operator can choose from when creating a new target database.
func (s *Server) handleFirestoreLocations(w http.ResponseWriter, r *http.Request) {
	locs, err := ListFirestoreLocations(r.Context())
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"locations": locs})
}

// handleFirestoreCreate provisions a new MongoDB-compatible Firestore database
// (Enterprise edition, with the operator's chosen location, PITR, and backup
// schedule) and returns it with its endpoint. Slow and billed — only invoked
// when the operator explicitly chooses "create" for a mapping.
func (s *Server) handleFirestoreCreate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var opts CreateOptions
	if err := json.NewDecoder(r.Body).Decode(&opts); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if strings.TrimSpace(opts.ID) == "" {
		writeError(w, http.StatusBadRequest, "database ID is required")
		return
	}
	s.log.Infof("Provisioning Firestore database %q in %q (pitr=%v, backups=%q) — this may take minutes...",
		opts.ID, opts.Location, opts.EnablePITR, opts.BackupSchedule)
	db, err := CreateFirestoreDatabase(r.Context(), opts)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, db)
}

// startRequest mirrors the config form the browser submits. Each selected
// source database carries its own Firestore target (endpoint + db ID), because
// one Firestore database maps to exactly one MongoDB database.
type startRequest struct {
	Source            string `json:"source"`
	ReplicationMethod string `json:"replicationMethod"`
	Mode              string `json:"mode"`
	Auth              struct {
		Type string `json:"type"` // "oidc" | "scram"
		User string `json:"user"`
		Pass string `json:"pass"`
	} `json:"auth"`
	Selections []struct {
		Database       string   `json:"database"`
		AllCollections bool     `json:"allCollections"`
		Collections    []string `json:"collections"`
		TargetEndpoint string   `json:"targetEndpoint"`
		TargetDBID     string   `json:"targetDBID"`
		// CollectionTuning carries optional per-collection partition-knob overrides,
		// keyed by source collection name. Independent of Collections, so it also
		// applies in whole-database mode. Pre-filled by the UI from the assessment's
		// straggler recommendations, editable before start.
		CollectionTuning map[string]config.CollectionTuning `json:"collectionTuning,omitempty"`
	} `json:"selections"`
	// Tuning carries optional concurrency overrides from the UI. Nil / zero
	// fields fall back to the engine defaults (config.ApplyDefaults).
	Tuning *tuningRequest `json:"tuning,omitempty"`
	// SyncSecondaryIndexes controls whether the source's secondary indexes are
	// replicated to the target. Pointer so an absent field (older client) defaults
	// to true rather than silently off. The _id index is ALWAYS built regardless
	// of this flag — see the wiring in handleStart.
	SyncSecondaryIndexes *bool `json:"syncSecondaryIndexes,omitempty"`
	// Sample tunes the assessment's per-collection document sampling. Only used
	// by /api/assess; ignored by /api/start.
	Sample *sampleRequest `json:"sample,omitempty"`
}

// sampleRequest are the assessment sampling knobs the UI exposes. Zero fields
// fall back to assess.DefaultSampleConfig. RatePct is a percent (0.1 = 0.1%),
// which is friendlier in the UI than a raw fraction.
type sampleRequest struct {
	Floor   int     `json:"floor"`
	RatePct float64 `json:"ratePct"`
	Cap     int     `json:"cap"`
}

// tuningRequest are the concurrency knobs the UI exposes (pre-filled from the
// assessment recommendation). Zero values mean "use the engine default".
type tuningRequest struct {
	InitialMigrationWorkers int  `json:"initialMigrationWorkers"`
	ConcurrentCollections   int  `json:"concurrentCollections"`
	IncrementalWorkerCount  int  `json:"incrementalWorkerCount"`
	ParallelReadsEnabled    bool `json:"parallelReadsEnabled"`
	MaxReadPartitions       int  `json:"maxReadPartitions"`
	WorkersPerPartition     int  `json:"workersPerPartition"`
	MinDocsPerPartition     int  `json:"minDocsPerPartition"`
	MinDocsForParallelReads int  `json:"minDocsForParallelReads"`
}

// applyTuning overlays UI concurrency overrides onto an already-defaulted config.
// Only non-zero fields override, so a partially-filled form keeps sane defaults.
func applyTuning(cfg *config.Config, t *tuningRequest) {
	if t == nil {
		return
	}
	if t.InitialMigrationWorkers > 0 {
		cfg.InitialMigrationWorkers = t.InitialMigrationWorkers
	}
	if t.ConcurrentCollections > 0 {
		cfg.ConcurrentCollections = t.ConcurrentCollections
	}
	if t.IncrementalWorkerCount > 0 {
		cfg.IncrementalWorkerCount = t.IncrementalWorkerCount
	}
	cfg.ParallelReadsEnabled = t.ParallelReadsEnabled
	if t.MaxReadPartitions > 0 {
		cfg.MaxReadPartitions = t.MaxReadPartitions
	}
	if t.WorkersPerPartition > 0 {
		cfg.WorkersPerPartition = t.WorkersPerPartition
	}
	if t.MinDocsPerPartition > 0 {
		cfg.MinDocsPerPartition = t.MinDocsPerPartition
	}
	if t.MinDocsForParallelReads > 0 {
		cfg.MinDocsForParallelReads = t.MinDocsForParallelReads
	}
}

// applyIndexOptions wires the UI's "replicate secondary indexes" toggle into each
// pair's target. SyncAllIndexes controls ONLY the source's secondary indexes; the
// _id index is built unconditionally by the migrator (MX1), independent of this
// flag. A nil syncSecondary (absent from an older client's payload) defaults to
// true so we never silently skip index creation.
//
// Without this the console never sets SyncAllIndexes, so the legacy full-load
// path's index sync (gated on SyncAllIndexes || len(Indexes)>0) never runs and the
// target ends up with no indexes at all.
func applyIndexOptions(cfg *config.Config, syncSecondary *bool) {
	sync := syncSecondary == nil || *syncSecondary
	for i := range cfg.DatabasePairs {
		cfg.DatabasePairs[i].Target.SyncAllIndexes = sync
	}
}

func (s *Server) handleStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var req startRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}

	if strings.TrimSpace(req.Source) == "" {
		writeError(w, http.StatusBadRequest, "source connection string is required")
		return
	}
	if req.Mode != "migrate" && req.Mode != "live" {
		writeError(w, http.StatusBadRequest, "mode must be 'migrate' or 'live'")
		return
	}
	if len(req.Selections) == 0 {
		writeError(w, http.StatusBadRequest, "select at least one database to migrate")
		return
	}
	if req.ReplicationMethod == "" {
		req.ReplicationMethod = "changestream"
	}
	scram := req.Auth.Type == "scram"
	if scram && (req.Auth.User == "" || req.Auth.Pass == "") {
		writeError(w, http.StatusBadRequest, "SCRAM auth requires username and password")
		return
	}

	// Each source database maps to its own Firestore database (its own endpoint).
	sels, err := s.buildSelections(req, true)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	cfg := wizard.BuildConfig(req.Source, req.ReplicationMethod, sels)
	applyTuning(cfg, req.Tuning)
	applyIndexOptions(cfg, req.SyncSecondaryIndexes)

	jobID, err := s.launch(cfg, req.Mode)
	if err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"jobID": jobID})
}

// buildSelections converts the request's per-database selections into wizard
// selections. When requireTarget is false (assessment, which only reads the
// source), missing target mappings are tolerated and left blank.
func (s *Server) buildSelections(req startRequest, requireTarget bool) ([]wizard.Selection, error) {
	scram := req.Auth.Type == "scram"
	sels := make([]wizard.Selection, 0, len(req.Selections))
	for _, sc := range req.Selections {
		if strings.TrimSpace(sc.Database) == "" {
			continue
		}
		var targetConn string
		hasTarget := strings.TrimSpace(sc.TargetEndpoint) != "" && strings.TrimSpace(sc.TargetDBID) != ""
		if hasTarget {
			if scram {
				targetConn = util.BuildFirestoreURI(sc.TargetEndpoint, sc.TargetDBID, req.Auth.User, req.Auth.Pass)
			} else {
				targetConn = util.BuildFirestoreURIOIDC(sc.TargetEndpoint, sc.TargetDBID)
			}
		} else if requireTarget {
			return nil, fmt.Errorf("source database %q has no Firestore target mapped", sc.Database)
		}
		sels = append(sels, wizard.Selection{
			SourceDB:         sc.Database,
			AllCollections:   sc.AllCollections || len(sc.Collections) == 0,
			Collections:      sc.Collections,
			TargetConn:       targetConn,
			TargetDB:         sc.TargetDBID,
			CollectionTuning: sc.CollectionTuning,
		})
	}
	if len(sels) == 0 {
		return nil, fmt.Errorf("no valid database selections")
	}
	return sels, nil
}

// handleAssess runs the pre-migration assessment (DESIGN §评估) over the selected
// source databases/collections and returns the findings plus a recommended
// concurrency tuning derived from the data volume and host CPU count.
func (s *Server) handleAssess(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var req startRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if strings.TrimSpace(req.Source) == "" {
		writeError(w, http.StatusBadRequest, "source connection string is required")
		return
	}
	if req.ReplicationMethod == "" {
		req.ReplicationMethod = "changestream"
	}
	sels, err := s.buildSelections(req, false)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	cfg := wizard.BuildConfig(req.Source, req.ReplicationMethod, sels)

	var sc assess.SampleConfig
	if req.Sample != nil {
		sc = assess.SampleConfig{
			Floor: req.Sample.Floor,
			Rate:  req.Sample.RatePct / 100.0,
			Cap:   req.Sample.Cap,
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
	defer cancel()
	report, err := assess.Run(ctx, cfg, sc, s.snapshotPlan(), s.log)
	if err != nil {
		writeError(w, http.StatusBadGateway, fmt.Sprintf("assessment failed: %v", err))
		return
	}
	tuning := assess.RecommendTuning(report)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"report": report,
		"tuning": tuning,
	})
}

// snapshotPlan returns a copy of the active remediation plan safe to use without
// holding planMu (assessment can run for minutes; a fix must not block on it).
func (s *Server) snapshotPlan() *remediate.Plan {
	s.planMu.Lock()
	defer s.planMu.Unlock()
	if s.plan == nil {
		return &remediate.Plan{}
	}
	items := make([]remediate.Remediation, len(s.plan.Items))
	copy(items, s.plan.Items)
	return &remediate.Plan{Items: items}
}

// remediateRequest is one add/remove of a fix, keyed by (rule, database,
// collection). action is "apply" (register the default fix for the rule) or
// "revert" (remove it).
type remediateRequest struct {
	Action     string `json:"action"`     // "apply" | "revert"
	Rule       string `json:"rule"`       // assess rule name, e.g. "value-size"
	Database   string `json:"database"`   // source database the finding is in
	Collection string `json:"collection"` // collection the finding is in
}

// handleRemediate registers or removes a remediation for a single finding, then
// persists the plan. The UI re-runs /api/assess afterwards to confirm the finding
// is cleared (the re-check applies this same plan in-memory — the source is never
// touched). Returns the updated plan so the UI can render ✅ badges immediately.
func (s *Server) handleRemediate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var req remediateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if strings.TrimSpace(req.Rule) == "" || strings.TrimSpace(req.Collection) == "" {
		writeError(w, http.StatusBadRequest, "rule and collection are required")
		return
	}

	s.planMu.Lock()
	if s.plan == nil {
		s.plan = &remediate.Plan{}
	}
	switch req.Action {
	case "apply":
		if _, _, _, ok := remediate.DefaultStrategy(req.Rule); !ok {
			s.planMu.Unlock()
			writeError(w, http.StatusBadRequest, fmt.Sprintf("rule %q has no auto-fix", req.Rule))
			return
		}
		s.plan.Add(remediate.Remediation{Rule: req.Rule, Database: req.Database, Collection: req.Collection})
	case "revert":
		s.plan.Remove(req.Rule, req.Database, req.Collection)
	default:
		s.planMu.Unlock()
		writeError(w, http.StatusBadRequest, "action must be apply or revert")
		return
	}
	if err := s.plan.Save(s.planFile); err != nil {
		s.log.Warnf("Could not persist remediation plan: %v", err)
	}
	items := make([]remediate.Remediation, len(s.plan.Items))
	copy(items, s.plan.Items)
	s.planMu.Unlock()

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"remediations": items,
	})
}

// handleVerify runs post-migration validation (DESIGN §验证): a document-count
// comparison for every selected source→target collection, plus an optional
// content hash when ?hash=1 is set. Requires target mappings.
func (s *Server) handleVerify(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var req startRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if strings.TrimSpace(req.Source) == "" {
		writeError(w, http.StatusBadRequest, "source connection string is required")
		return
	}
	if req.ReplicationMethod == "" {
		req.ReplicationMethod = "changestream"
	}
	sels, err := s.buildSelections(req, true)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	cfg := wizard.BuildConfig(req.Source, req.ReplicationMethod, sels)

	opts := verify.Options{Hash: r.URL.Query().Get("hash") == "1"}
	if opts.Hash {
		// The migrator writes converted-_id mappings here; feed them so converted
		// documents reconcile instead of showing as mismatches.
		opts.IDMapPath = "id-mapping.jsonl"
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Minute)
	defer cancel()
	report, err := verify.Run(ctx, cfg, s.log, opts)
	if err != nil {
		writeError(w, http.StatusBadGateway, fmt.Sprintf("verification failed: %v", err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"report": report,
		"ok":     report.OK(),
	})
}

// handleDLQ returns the read-only, aggregated view of the dead-letter queue for
// the current working directory. It surfaces unresolved failed documents grouped
// by source collection so the operator can see, on the UI, exactly what did not
// replicate — the key cutover-safety question — without scrolling the text log.
// It never writes; a job may be running or stopped when it is called.
func (s *Server) handleDLQ(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET required", http.StatusMethodNotAllowed)
		return
	}
	// Cap embedded entries per collection so a large DLQ can't bloat the payload;
	// the true count is still reported for each collection.
	const maxEntriesPerCollection = 200
	report, err := migration.ReadDLQReport(".", maxEntriesPerCollection)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to read DLQ: %v", err))
		return
	}
	writeJSON(w, http.StatusOK, report)
}

// resetResponse summarizes what a session reset cleared, so the UI can show a
// concrete confirmation instead of a silent success.
type resetResponse struct {
	Backup       string   `json:"backup,omitempty"`  // path the prior plan was backed up to (if any)
	PlanCleared  bool     `json:"planCleared"`       // remediation plan emptied
	JobsPurged   int      `json:"jobsPurged"`        // finished jobs dropped from the dashboard
	FilesRemoved []string `json:"filesRemoved"`      // residual state files deleted
}

// handleReset clears the console's session state so a new test starts from a
// genuinely clean slate: it empties the remediation plan (backing up the old one
// first), drops finished jobs and their rows from the dashboard, and wipes the
// per-run residual state files (DLQ, checkpoints, id-maps, audit). It never
// touches the connection config or any cloud/Firestore data, and it refuses to
// run while a job is active so an in-flight migration's state can't be pulled out
// from under it.
func (s *Server) handleReset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	s.mu.Lock()
	running := s.running
	s.mu.Unlock()
	if running {
		writeError(w, http.StatusConflict, "a migration is running — stop it before resetting the session")
		return
	}

	resp := resetResponse{FilesRemoved: []string{}}

	// 1) Back up then empty the remediation plan (in-memory + on disk), so stale
	//    ✅ selections from a previous test do not reappear on the next assess.
	s.planMu.Lock()
	if s.plan != nil && len(s.plan.Items) > 0 {
		if bak, err := backupFile(s.planFile); err != nil {
			s.log.Warnf("Could not back up remediation plan before reset: %v", err)
		} else if bak != "" {
			resp.Backup = bak
		}
	}
	s.plan = &remediate.Plan{}
	if err := s.plan.Save(s.planFile); err != nil {
		s.log.Warnf("Could not persist emptied remediation plan: %v", err)
	}
	resp.PlanCleared = true
	s.planMu.Unlock()

	// 2) Drop finished jobs and their rows from the dashboard.
	resp.JobsPurged = s.reg.PurgeInactive()

	// 3) Wipe per-run residual state files (DLQ, checkpoints, id-maps, audit).
	if removed, err := migration.CleanResidualState(".", s.log); err != nil {
		s.log.Warnf("Residual state cleanup during reset incomplete: %v", err)
	} else if removed != nil {
		resp.FilesRemoved = removed
	}

	s.log.Infof("Session reset: plan cleared, %d job(s) purged, %d residual file(s) removed",
		resp.JobsPurged, len(resp.FilesRemoved))
	writeJSON(w, http.StatusOK, resp)
}

// backupFile copies path to a timestamped ".bak-<unix>" sibling and returns the
// backup path. It returns ("", nil) when path does not exist (nothing to back
// up) so callers can proceed without treating that as an error.
func backupFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	bak := fmt.Sprintf("%s.bak-%d", path, time.Now().Unix())
	if err := os.WriteFile(bak, data, 0644); err != nil {
		return "", err
	}
	return bak, nil
}

// launch starts a migrator in a background goroutine and wires it to the shared
// registry/control so the dashboard reflects its progress. Only one job runs at
// a time. Every console "Start" is a fresh run — residual per-pair state files
// are wiped first.
func (s *Server) launch(cfg *config.Config, mode string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		return "", fmt.Errorf("a migration is already running (%s); stop it before starting another", s.jobID)
	}

	// Per-run state files are named by pair INDEX (dlq-pair0.jsonl,
	// initialMigrationState-pair1.json, …), so if this run's database→index
	// mapping differs from a previous run's, inherited files would abort a pair
	// (stale DLQ / safety violation) or make it skip the initial load and jump
	// straight to incremental. Wipe them first so the run starts clean. (The
	// console never resumes a checkpoint.)
	if _, err := migration.CleanResidualState(".", s.log); err != nil {
		s.log.Warnf("Residual state cleanup incomplete (previous run's files may interfere): %v", err)
	}

	migrator := migration.NewMigrator(cfg, s.log)
	control := metrics.NewControl()
	// Monotonic suffix guarantees uniqueness even for a stop-then-restart within the
	// same wall-clock second (see jobSeq); without it UpsertJob would reuse the old
	// job row instead of creating a fresh one.
	jobID := fmt.Sprintf("%s-%d-%d", mode, time.Now().Unix(), atomic.AddUint64(&s.jobSeq, 1))
	migrator.AttachControlPlane(s.reg, control, jobID)
	s.reg.UpsertJob(jobID, mode, metrics.StateInitialLoad)
	// Echo this run's cutover thresholds to the dashboard so the readiness signal
	// uses the same numbers the engine does.
	s.reg.SetCutoverThresholds(float64(cfg.CutoverLagThresholdSeconds), cfg.CutoverStableChecks)

	s.control = control
	s.jobID = jobID
	s.running = true
	s.migrator = migrator
	done := make(chan struct{})
	s.done = done

	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	go func() {
		defer cancel()
		defer func() {
			migrator.Close()
			s.mu.Lock()
			s.running = false
			s.migrator = nil
			s.mu.Unlock()
			// Signal (to a waiting Stop) that this job has fully unwound. Closed
			// last, after running is cleared, so a Stop that returns here is
			// guaranteed to see running == false.
			close(done)
		}()
		// A panic in the migrator must not take down the whole console process
		// (it serves other requests and can run further jobs).
		defer func() {
			if p := recover(); p != nil {
				err := fmt.Errorf("migration job panicked: %v", p)
				s.reg.FailJob(jobID, err)
				s.log.Errorf("Migration job %s panicked: %v", jobID, p)
			}
		}()

		err := migrator.Start(ctx, mode)
		switch {
		case err == nil, err == context.Canceled, err == metrics.ErrStopped:
			s.reg.UpsertJob(jobID, mode, metrics.StateDone)
		default:
			s.reg.FailJob(jobID, err)
			s.log.Errorf("Migration job %s failed: %v", jobID, err)
		}
	}()

	s.log.Infof("Started %s job %s from web console", mode, jobID)
	return jobID, nil
}

// handleReconfig applies a live tuning change to the running job (Layer A of the
// runtime-adjustable-concurrency feature): raise/lower how many collections load
// at once, or bump the partition/worker knobs for the global default or one named
// collection. It never restarts the job or wipes state; changes affect collections
// dispatched from now on, so a table already loading keeps its current tuning.
func (s *Server) handleReconfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "POST required")
		return
	}
	var req struct {
		Job                   string `json:"job"`
		Database              string `json:"database"`
		Collection            string `json:"collection"`
		ConcurrentCollections int    `json:"concurrentCollections"`
		MaxReadPartitions     int    `json:"maxReadPartitions"`
		WorkersPerPartition   int    `json:"workersPerPartition"`
		MinDocsPerPartition   int    `json:"minDocsPerPartition"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	s.mu.Lock()
	mig, running, jobID := s.migrator, s.running, s.jobID
	s.mu.Unlock()
	if !running || mig == nil {
		writeError(w, http.StatusConflict, "no running job to reconfigure")
		return
	}
	if req.Job != "" && req.Job != jobID {
		writeError(w, http.StatusConflict, fmt.Sprintf("unknown job %q", req.Job))
		return
	}

	clamp := func(v, lo, hi int) int {
		if v < lo {
			return lo
		}
		if v > hi {
			return hi
		}
		return v
	}
	rc := migration.ReconfigRequest{Database: req.Database, Collection: req.Collection}
	if req.ConcurrentCollections > 0 {
		rc.ConcurrentCollections = clamp(req.ConcurrentCollections, 1, 64)
	}
	if req.MaxReadPartitions > 0 {
		rc.MaxReadPartitions = clamp(req.MaxReadPartitions, 1, 64)
	}
	if req.WorkersPerPartition > 0 {
		rc.WorkersPerPartition = clamp(req.WorkersPerPartition, 1, 32)
	}
	if req.MinDocsPerPartition > 0 {
		rc.MinDocsPerPartition = clamp(req.MinDocsPerPartition, 1000, 100000000)
	}

	if err := mig.Reconfig(rc); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"ok":   true,
		"note": "Applied. Takes effect for collections not yet started (collections already loading keep their launch-time config — in-flight speedup is Layer B).",
	})
}

// Command implements metrics.ControlHandler, routing pause/resume/stop from the
// dashboard buttons to the active job.
func (s *Server) Command(job, action string) error {
	s.mu.Lock()
	control, jobID, cancel, done := s.control, s.jobID, s.cancel, s.done
	s.mu.Unlock()

	if control == nil {
		return fmt.Errorf("no active job to control")
	}
	if job != "" && job != jobID {
		return fmt.Errorf("unknown job %q", job)
	}
	switch action {
	case "pause":
		control.Pause()
		s.reg.UpsertJob(jobID, "", metrics.StatePaused)
	case "resume":
		control.Resume()
		s.reg.UpsertJob(jobID, "", metrics.StateInitialLoad)
	case "stop":
		// Stop must work for every migration path. control.Stop() only trips a
		// cooperative flag that the modern backfill workers poll; the legacy
		// (mgo, pre-3.6) replicator instead unwinds on ctx cancellation. So we
		// do both: signal the flag AND cancel the job's context, guaranteeing an
		// actual stop regardless of which engine path is running.
		control.Stop()
		if cancel != nil {
			cancel()
		}
		s.reg.UpsertJob(jobID, "", metrics.StateDone)
		// Block until the job goroutine has FULLY torn down (change stream,
		// workers, connections) and cleared s.running — otherwise the next
		// Start/Reset would see a stale running==true and reject the operator's
		// brand-new task with "a migration is already running". The modern live
		// path unwinds within a few seconds of cancellation; cap the wait so a
		// pathologically stuck teardown reports back instead of hanging forever.
		if done != nil {
			select {
			case <-done:
			case <-time.After(30 * time.Second):
				return fmt.Errorf("stop signalled but job %s is still shutting down after 30s; wait a moment and retry", jobID)
			}
		}
	default:
		return fmt.Errorf("unknown action %q", action)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
