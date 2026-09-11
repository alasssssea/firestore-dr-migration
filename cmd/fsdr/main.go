package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"firestore-dr-migration/pkg/assess"
	"firestore-dr-migration/pkg/config"
	"firestore-dr-migration/pkg/console"
	"firestore-dr-migration/pkg/logger"
	"firestore-dr-migration/pkg/metrics"
	"firestore-dr-migration/pkg/migration"
	"firestore-dr-migration/pkg/remediate"
	"firestore-dr-migration/pkg/verify"
	"firestore-dr-migration/pkg/wizard"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func main() {
	// Parse command-line flags
	configPath := flag.String("config", "mongodb_replication_config.json", "Path to configuration file")
	mode := flag.String("mode", "migrate", "Operation mode: 'migrate', 'live', 'live-only', 'retry-dlq', 'wizard', 'console', 'assess', or 'verify'")
	logLevel := flag.String("log-level", "info", "Log level: debug, info, warn, error")
	logFile := flag.String("log-file", "", "Path to log file (logs to both stdout and file when specified)")
	liveStartTimeStr := flag.String("live-start-timestamp", "", "Start timestamp for live-only replication (Unix epoch seconds or RFC3339 format)")
	dryRun := flag.Bool("dry-run", false, "Dry run mode (skips writes, outputs partitioning recommendations on backfill)")
	verifyHash := flag.Bool("verify-hash", false, "In verify mode, also compare document content hashes (not just counts)")
	dlqResyncFromSource := flag.Bool("dlq-resync-from-source", false, "In retry-dlq mode, re-read each failed document fresh from the SOURCE by _id (picking up a source-side fix) instead of replaying the stored DLQ snapshot; source-deleted docs are treated as resolved")
	metricsAddr := flag.String("metrics-addr", "", "If set (e.g. \":9090\"), serve the control plane: /healthz, /readyz, /metrics, status API, and dashboard UI. In console mode, the listen address (default :9090)")
	help := flag.Bool("help", false, "Display help information")
	flag.Parse()

	// Display help if requested
	if *help {
		displayUsage()
		os.Exit(0)
	}

	// Create logger
	log := logger.New()
	log.SetLevel(*logLevel)

	// Maximize open file descriptor resource limits (ulimit -n 65536)
	maximizeOpenFileLimit(log)

	// Set up log file if specified
	if *logFile != "" {
		file, err := log.SetOutputFile(*logFile)
		if err != nil {
			log.Fatalf("Failed to open log file %s: %v", *logFile, err)
		}
		defer file.Close()
		log.Infof("Logging to file: %s", *logFile)
	}

	// Wizard mode generates a config file interactively, then exits. It runs
	// before config loading because it is what creates the config.
	if *mode == "wizard" {
		if err := wizard.Run(log, *configPath); err != nil {
			log.Fatalf("Configuration wizard failed: %v", err)
		}
		os.Exit(0)
	}

	// Console mode serves a browser-based UI to configure and launch migrations.
	// Like wizard, it needs no pre-existing config file — it builds the config
	// from the form and runs the migration in-process. Reuses -metrics-addr for
	// the listen address (default :9090).
	if *mode == "console" {
		addr := *metricsAddr
		if addr == "" {
			addr = ":9090"
		}
		if err := console.Serve(log, addr); err != nil {
			log.Fatalf("Console server failed: %v", err)
		}
		os.Exit(0)
	}

	// Validate mode. Note: wizard/console short-circuit above (they need no
	// config); migrate/live/live-only/retry-dlq run the migrator; assess/verify
	// short-circuit below after config load.
	if !isValidMode(*mode) {
		log.Fatalf("Invalid mode: %s. Please choose 'migrate', 'live', 'live-only', 'retry-dlq', 'wizard', 'console', 'assess', or 'verify'", *mode)
	}

	// Load configuration
	log.Info("Loading configuration...")
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}
	// The -dlq-resync-from-source flag overrides the config's retryConfig for this
	// run. It only applies to retry-dlq mode (other modes never re-read the DLQ).
	if *dlqResyncFromSource {
		if *mode != "retry-dlq" {
			log.Fatal("Error: -dlq-resync-from-source can only be specified when -mode is 'retry-dlq'")
		}
		cfg.RetryConfig.ResyncFromSource = true
	}

	// Display and log the loaded configuration with sensitive values masked
	log.Infof("Active Configuration:\n%s", getSanitizedConfigJSON(cfg))

	// Parse and validate -live-start-timestamp option.
	// This flag specifies a custom historical starting point (Unix epoch seconds or RFC3339 date)
	// from which the incremental change stream/oplog replication should begin.
	// Note: This is only valid in "live-only" mode because standard migration modes always
	// automatically capture the starting position prior to performing the initial copy phase.
	var liveStartTime *primitive.Timestamp
	if *liveStartTimeStr != "" {
		if *mode != "live-only" {
			log.Fatal("Error: -live-start-timestamp can only be specified when -mode is 'live-only'")
		}
		ts, err := parseStartTimestamp(*liveStartTimeStr)
		if err != nil {
			log.Fatalf("Failed to parse -live-start-timestamp: %v", err)
		}
		liveStartTime = ts
	}

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle interrupt signals
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-signalChan
		log.Info("Received interrupt signal. Shutting down...")
		cancel()
		// Give some time for graceful shutdown

		log.Infof("Waiting for 15 seconds for workers to finish...")
		time.Sleep(15 * time.Second)
		os.Exit(0)
	}()

	// Assess mode: pre-migration compatibility check. Exits non-zero if any
	// hard-blocking (B) issue is found.
	if *mode == "assess" {
		log.Info("Running pre-migration assessment...")
		// Honor any saved remediation plan so the CLI re-check matches the console.
		plan, perr := remediate.Load(remediate.DefaultPlanFile)
		if perr != nil {
			log.Warnf("Could not load remediation plan: %v", perr)
			plan = nil
		}
		report, err := assess.Run(ctx, cfg, assess.SampleConfig{}, plan, log)
		if err != nil {
			log.Fatalf("Assessment failed: %v", err)
		}
		fmt.Print(report.Format())
		if report.Blocking() {
			os.Exit(1)
		}
		os.Exit(0)
	}

	// Verify mode: post-migration count (and optional content-hash) comparison.
	// Exits non-zero if any discrepancy is found.
	if *mode == "verify" {
		log.Info("Running post-migration verification...")
		// This fork is Firestore -> Firestore: source _ids are already Firestore-legal,
		// so there is no _id conversion and therefore no id-mapping.jsonl to reconcile.
		// Verification is pure count (+ optional content-hash) comparison.
		report, err := verify.Run(ctx, cfg, log, verify.Options{Hash: *verifyHash})
		if err != nil {
			log.Fatalf("Verification failed: %v", err)
		}
		fmt.Print(report.Format())
		if !report.OK() {
			os.Exit(1)
		}
		os.Exit(0)
	}

	// Create migrator
	migrator := migration.NewMigrator(cfg, log)
	migrator.LiveStartTime = liveStartTime
	migrator.DryRun = *dryRun
	defer migrator.Close()

	// Optional control plane (metrics/lag/health + dashboard). When -metrics-addr
	// is set, expose /healthz, /readyz, /metrics, a JSON/SSE status API, and the
	// embedded UI, and wire cooperative pause/resume/stop into the migrator.
	if *metricsAddr != "" {
		registry := metrics.NewRegistry()
		control := metrics.NewControl()
		jobID := fmt.Sprintf("%s-%d", *mode, time.Now().Unix())
		migrator.AttachControlPlane(registry, control, jobID)
		registry.UpsertJob(jobID, *mode, metrics.StateInitialLoad)
		metricsJobID = jobID

		handler := metrics.Handler(registry, jobControl{id: jobID, control: control, reg: registry})
		srv := &http.Server{Addr: *metricsAddr, Handler: handler}
		go func() {
			log.Infof("Control plane listening on %s (UI at http://%s/)", *metricsAddr, *metricsAddr)
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Errorf("Control plane server error: %v", err)
			}
		}()
		defer srv.Close()
		registry.SetReady(true)
		defer func() {
			registry.UpsertJob(jobID, *mode, metrics.StateDone)
		}()
	}

	// Start migration/replication
	startTime := time.Now()

	if err := migrator.Start(ctx, *mode); err != nil {
		// Check if the error is due to context cancellation (Ctrl+C)
		if err == context.Canceled || err == metrics.ErrStopped {
			log.Info("Process stopped due to user interrupt (Ctrl+C)")
		} else {
			log.Fatalf("Error during %s process: %v", *mode, err)
		}
	}

	// Log completion for non-continuous modes (live mode keeps running)
	if *mode == "migrate" || *mode == "retry-dlq" {
		duration := time.Since(startTime)
		log.Infof("Process completed in %.2f seconds", duration.Seconds())
		os.Exit(0) // Explicitly exit after completion
	}
}

// metricsJobID holds the current job's ID for status updates after Start returns.
var metricsJobID string

// jobControl adapts the single running job to the metrics.ControlHandler
// interface so the HTTP control endpoint can pause/resume/stop it.
type jobControl struct {
	id      string
	control *metrics.Control
	reg     *metrics.Registry
}

func (j jobControl) Command(job, action string) error {
	if job != "" && job != j.id {
		return fmt.Errorf("unknown job %q", job)
	}
	switch action {
	case "pause":
		j.control.Pause()
		j.reg.UpsertJob(j.id, "", metrics.StatePaused)
	case "resume":
		j.control.Resume()
		j.reg.UpsertJob(j.id, "", metrics.StateInitialLoad)
	case "stop":
		j.control.Stop()
	default:
		return fmt.Errorf("unknown action %q", action)
	}
	return nil
}

// displayUsage displays usage information
func displayUsage() {
	fmt.Println("\nFirestore DR Migration Tool (fsdr)")
	fmt.Println("Regional Firestore -> Multi-region Firestore, with live CDC and cutover")
	fmt.Println("=====================================================================")
	fmt.Println("Usage: fsdr [options]")
	fmt.Println("Options:")
	fmt.Println("  -config string")
	fmt.Println("        Path to configuration file (default \"mongodb_replication_config.json\")")
	fmt.Println("  -mode string")
	fmt.Println("        Operation mode: 'migrate', 'live', 'live-only', 'retry-dlq', 'console', 'wizard', 'assess', or 'verify' (default \"migrate\")")
	fmt.Println("        Modes:")
	fmt.Println("          migrate:")
	fmt.Println("            Perform a one-time full copy. Copies all data and indexes from the")
	fmt.Println("            source (regional) Firestore to the target (multi-region) Firestore,")
	fmt.Println("            then exits immediately.")
	fmt.Println("          live:")
	fmt.Println("            Perform a full copy followed by real-time replication. Captures the")
	fmt.Println("            change-stream resume token FIRST, copies all existing data and indexes,")
	fmt.Println("            then automatically transitions to streaming live changes (CDC) so the")
	fmt.Println("            target catches up to the source. Runs continuously until cutover.")
	fmt.Println("          live-only:")
	fmt.Println("            Perform real-time incremental replication only. Skips the initial copy")
	fmt.Println("            phase. Streams live changes from the last saved resume token (or current")
	fmt.Println("            moment if none exists), or from a custom position when")
	fmt.Println("            -live-start-timestamp is specified.")
	fmt.Println("          retry-dlq:")
	fmt.Println("            Reprocess the Dead Letter Queue (DLQ). Reads previous failed records from")
	fmt.Println("            the DLQ files, retries writing them to the target, and writes any subsequent")
	fmt.Println("            failures to new DLQ files.")
	fmt.Println("          console:")
	fmt.Println("            Serve a browser-based control console (default :9090, override with")
	fmt.Println("            -metrics-addr) to assess, configure, launch, monitor, and verify a")
	fmt.Println("            migration end-to-end. Needs no pre-existing config file — it builds the")
	fmt.Println("            config from the form and runs the migration in-process.")
	fmt.Println("          wizard:")
	fmt.Println("            Interactively generate a config file at -config, then exit.")
	fmt.Println("          assess:")
	fmt.Println("            Run a read-only pre-migration check against the source and target")
	fmt.Println("            (connectivity, change-stream enablement, index inventory), print the")
	fmt.Println("            report, and exit non-zero if any hard-blocking issue is found.")
	fmt.Println("          verify:")
	fmt.Println("            Run a post-migration verification comparing source vs target document counts")
	fmt.Println("            (add -verify-hash to also compare content hashes), print the report, and exit")
	fmt.Println("            non-zero on any discrepancy.")
	fmt.Println("  -log-level string")
	fmt.Println("        Log level: debug, info, warn, error (default \"info\")")
	fmt.Println("  -log-file string")
	fmt.Println("        Path to log file (logs to both stdout and file when specified)")
	fmt.Println("  -live-start-timestamp string")
	fmt.Println("        Start timestamp for live-only replication (Unix epoch seconds or RFC3339 format)")
	fmt.Println("        Debian command-line examples to get 'now':")
	fmt.Printf("          * Unix epoch seconds:             date +%%s\n")
	fmt.Println("          * RFC3339 format:                 date --rfc-3339=seconds   (or: date -Iseconds)")
	fmt.Println("  -metrics-addr string")
	fmt.Println("        If set (e.g. \":9090\"), serve the control plane: /healthz, /readyz, /metrics,")
	fmt.Println("        the status API, and the dashboard UI. In console mode this is the listen")
	fmt.Println("        address (default \":9090\").")
	fmt.Println("  -verify-hash")
	fmt.Println("        In verify mode, also compare document content hashes (not just counts).")
	fmt.Println("  -dlq-resync-from-source")
	fmt.Println("        In retry-dlq mode, re-read each failed document fresh from the SOURCE by _id")
	fmt.Println("        (picking up a source-side fix) instead of replaying the stored DLQ snapshot;")
	fmt.Println("        source-deleted docs are treated as resolved.")
	fmt.Println("  -dry-run")
	fmt.Println("        Dry run mode (skips writes).")
	fmt.Println("        - In backfill modes ('migrate' or 'live' initial phase): connects to the source")
	fmt.Println("          and target, runs target compatibility validations, samples source collections,")
	fmt.Println("          and exits without reading full collections.")
	fmt.Println("        - In incremental modes ('live-only' or 'live' incremental phase): connects to the")
	fmt.Println("          source, starts the change stream to ingest live events, and prints real-time")
	fmt.Println("          ingestion lag statistics while discarding writes. Useful to test read")
	fmt.Println("          performance.")
	fmt.Println("  -help")
	fmt.Println("        Display this help information")
	fmt.Println("Examples:")
	fmt.Println("  fsdr -mode=console")
	fmt.Println("  fsdr -mode=console -metrics-addr=:8080")
	fmt.Println("  fsdr -mode=wizard")
	fmt.Println("  fsdr -mode=assess")
	fmt.Println("  fsdr -mode=verify -verify-hash")
	fmt.Println("  fsdr -mode=live")
	fmt.Println("  fsdr -mode=live-only")
	fmt.Println("  fsdr -mode=live-only -dry-run")
	fmt.Println("  fsdr -mode=live-only -live-start-timestamp=1716234000")
	fmt.Println("  fsdr -mode=live-only -live-start-timestamp=2026-05-20T21:00:00Z")
	fmt.Println("  fsdr -mode=live -log-file=migration.log")
	fmt.Println("  fsdr -mode=retry-dlq")
	fmt.Println("  fsdr -config=custom_config.json -mode=retry-dlq")
}

// isValidMode returns whether the given mode string is a recognized operation mode.
func isValidMode(mode string) bool {
	return mode == "migrate" || mode == "live" || mode == "live-only" || mode == "retry-dlq" ||
		mode == "assess" || mode == "verify" ||
		mode == "wizard" || mode == "console"
}

// parseStartTimestamp parses a user-provided timestamp string as either a raw Unix epoch
// timestamp in seconds (e.g. 1716234000) or a standard RFC3339 formatted string (e.g. 2026-05-20T21:00:00Z).
// It constructs and returns a BSON primitive.Timestamp structure with the scanned seconds component (T)
// and an initial increment (I) set to 1, suitable for MongoDB change streams and oplog tailing.
func parseStartTimestamp(s string) (*primitive.Timestamp, error) {
	if s == "" {
		return nil, nil
	}

	// Try parsing the string strictly as an integer. We use strconv.ParseInt rather than
	// fmt.Sscan to ensure the entire string is a valid base-10 number (avoiding partial matches
	// such as extracting "2026" from "2026-05-20...").
	if secs, err := strconv.ParseInt(s, 10, 64); err == nil {
		return &primitive.Timestamp{T: uint32(secs), I: 1}, nil
	}

	// If it is not a plain integer, fall back to parsing as an RFC3339 date string.
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return nil, fmt.Errorf("invalid timestamp format, must be Unix epoch seconds or RFC3339 (e.g. 2026-05-20T21:00:00Z): %w", err)
	}
	return &primitive.Timestamp{T: uint32(t.Unix()), I: 1}, nil
}

// sanitizeConnectionString masks sensitive user credentials in MongoDB URIs.
func sanitizeConnectionString(uri string) string {
	if uri == "" {
		return ""
	}
	var prefix string
	if strings.HasPrefix(uri, "mongodb://") {
		prefix = "mongodb://"
	} else if strings.HasPrefix(uri, "mongodb+srv://") {
		prefix = "mongodb+srv://"
	} else {
		return uri
	}

	remaining := uri[len(prefix):]
	atIndex := strings.LastIndex(remaining, "@")
	if atIndex == -1 {
		return uri
	}

	credentials := remaining[:atIndex]
	hostAndParams := remaining[atIndex:]

	colonIndex := strings.Index(credentials, ":")
	if colonIndex == -1 {
		return prefix + credentials + hostAndParams
	}

	username := credentials[:colonIndex]
	return prefix + username + ":*****" + hostAndParams
}

// getSanitizedConfigJSON generates an indented JSON string representation of Config with credentials masked.
func getSanitizedConfigJSON(cfg *config.Config) string {
	if cfg == nil {
		return "{}"
	}

	sanitizedCfg := *cfg
	sanitizedCfg.DatabasePairs = make([]config.DatabasePair, len(cfg.DatabasePairs))
	for i, pair := range cfg.DatabasePairs {
		sanitizedPair := pair
		sanitizedPair.Source.ConnectionString = sanitizeConnectionString(pair.Source.ConnectionString)
		sanitizedPair.Target.ConnectionString = sanitizeConnectionString(pair.Target.ConnectionString)
		sanitizedCfg.DatabasePairs[i] = sanitizedPair
	}

	data, err := json.MarshalIndent(sanitizedCfg, "", "  ")
	if err != nil {
		return fmt.Sprintf("error marshalling config: %v", err)
	}
	return string(data)
}

// maximizeOpenFileLimit programmatically adjusts the maximum open files resource limit (ulimit -n) to 65536.
func maximizeOpenFileLimit(log *logger.Logger) {
	var rLimit syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		log.Warnf("Failed to get current open file resource limit: %v", err)
		return
	}

	targetLimit := uint64(65536)
	if rLimit.Max < targetLimit {
		log.Warnf("System hard limit for open file descriptors is %d, which is lower than requested 65536. Setting limit to system maximum.", rLimit.Max)
		targetLimit = rLimit.Max
	}

	rLimit.Cur = targetLimit
	rLimit.Max = targetLimit

	err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		log.Warnf("Failed to set open file resource limit (ulimit) to %d: %v", targetLimit, err)
	} else {
		log.Infof("Successfully adjusted open file descriptor resource limit (ulimit) to %d", targetLimit)
	}
}
