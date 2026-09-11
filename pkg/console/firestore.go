package console

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

// FirestoreDB describes a Firestore database as a possible migration target.
// Endpoint is the MongoDB-compatibility host "<uid>.<location>.firestore.goog";
// only ENTERPRISE databases with MongoDB-compatible data access enabled can
// actually receive a migration (MongoCompatible == true).
type FirestoreDB struct {
	ID              string `json:"id"`
	Endpoint        string `json:"endpoint"`
	Location        string `json:"location"`
	Edition         string `json:"edition"`
	MongoCompatible bool   `json:"mongoCompatible"`
	Warning         string `json:"warning,omitempty"` // non-fatal issue, e.g. backup schedule failed
}

// CreateOptions carries the operator-chosen settings for a new Firestore
// database. Edition (enterprise) and MongoDB compatibility are fixed — an
// Enterprise Firestore database is MongoDB-compatible by definition — so only
// the choices that actually vary are surfaced.
type CreateOptions struct {
	ID             string `json:"id"`
	Location       string `json:"location"`       // single-region location id
	EnablePITR     bool   `json:"enablePitr"`     // point-in-time recovery
	BackupSchedule string `json:"backupSchedule"` // "", "daily", or "weekly"
}

// multiRegionLocations are the Firestore multi-region locations we exclude from
// the console's single-region picker (the operator asked for single-region only).
var multiRegionLocations = map[string]bool{"eur3": true, "nam5": true, "nam7": true}

// firestoreRaw is the subset of `gcloud firestore databases (list|describe)`
// JSON that we consume.
type firestoreRaw struct {
	Name            string `json:"name"`
	UID             string `json:"uid"`
	LocationID      string `json:"locationId"`
	DatabaseEdition string `json:"databaseEdition"`
	MongoMode       string `json:"mongodbCompatibleDataAccessMode"`
}

func (r firestoreRaw) toDB(id string) FirestoreDB {
	if id == "" {
		id = r.Name[strings.LastIndex(r.Name, "/")+1:]
	}
	return FirestoreDB{
		ID:              id,
		Endpoint:        fmt.Sprintf("%s.%s.firestore.goog", r.UID, r.LocationID),
		Location:        r.LocationID,
		Edition:         r.DatabaseEdition,
		MongoCompatible: r.DatabaseEdition == "ENTERPRISE" && r.MongoMode == "DATA_ACCESS_MODE_ENABLED",
	}
}

// ListFirestoreDatabases returns every Firestore database in the active gcloud
// project, each with its computed MongoDB-compatibility endpoint.
func ListFirestoreDatabases(ctx context.Context) ([]FirestoreDB, error) {
	out, err := runGcloud(ctx, 60*time.Second, "firestore", "databases", "list", "--format=json")
	if err != nil {
		return nil, err
	}
	var raw []firestoreRaw
	if err := json.Unmarshal(out, &raw); err != nil {
		return nil, fmt.Errorf("parse gcloud output: %w", err)
	}
	dbs := make([]FirestoreDB, 0, len(raw))
	for _, r := range raw {
		dbs = append(dbs, r.toDB(""))
	}
	return dbs, nil
}

// ListFirestoreLocations returns the single-region Firestore locations offered
// as targets (multi-region locations are excluded per the console's scope).
func ListFirestoreLocations(ctx context.Context) ([]string, error) {
	out, err := runGcloud(ctx, 60*time.Second,
		"firestore", "locations", "list", "--format=value(locationId)")
	if err != nil {
		return nil, err
	}
	var locs []string
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		id := strings.TrimSpace(line)
		if id == "" || multiRegionLocations[id] {
			continue
		}
		locs = append(locs, id)
	}
	return locs, nil
}

// CreateFirestoreDatabase provisions a new ENTERPRISE (MongoDB-compatible)
// Firestore database with the chosen location, PITR, and backup schedule, then
// returns it (with its endpoint) once ready. Slow and billed. A backup-schedule
// failure is non-fatal: the database is usable, so it is returned with Warning
// set rather than erroring out.
func CreateFirestoreDatabase(ctx context.Context, opts CreateOptions) (FirestoreDB, error) {
	if opts.Location == "" {
		opts.Location = "asia-northeast1"
	}
	args := []string{
		"firestore", "databases", "create",
		"--database=" + opts.ID,
		"--location=" + opts.Location,
		"--edition=enterprise", // Enterprise ⇒ MongoDB-compatible
	}
	if opts.EnablePITR {
		args = append(args, "--enable-pitr")
	}
	if _, err := runGcloud(ctx, 10*time.Minute, args...); err != nil {
		return FirestoreDB{}, err
	}

	out, err := runGcloud(ctx, 60*time.Second,
		"firestore", "databases", "describe", "--database="+opts.ID, "--format=json")
	if err != nil {
		return FirestoreDB{}, err
	}
	var r firestoreRaw
	if err := json.Unmarshal(out, &r); err != nil {
		return FirestoreDB{}, fmt.Errorf("parse gcloud describe: %w", err)
	}
	db := r.toDB(opts.ID)

	if warn := createBackupSchedule(ctx, opts.ID, opts.BackupSchedule); warn != "" {
		db.Warning = warn
	}
	return db, nil
}

// createBackupSchedule adds a daily/weekly backup schedule to a freshly created
// database. Returns a non-empty warning string on failure (non-fatal).
func createBackupSchedule(ctx context.Context, id, schedule string) string {
	switch schedule {
	case "", "none":
		return ""
	case "daily":
		if _, err := runGcloud(ctx, 90*time.Second,
			"firestore", "backups", "schedules", "create",
			"--database="+id, "--retention=7d", "--recurrence=daily"); err != nil {
			return fmt.Sprintf("database created, but daily backup schedule failed: %v", err)
		}
	case "weekly":
		if _, err := runGcloud(ctx, 90*time.Second,
			"firestore", "backups", "schedules", "create",
			"--database="+id, "--retention=14w", "--recurrence=weekly", "--day-of-week=MON"); err != nil {
			return fmt.Sprintf("database created, but weekly backup schedule failed: %v", err)
		}
	default:
		return fmt.Sprintf("database created, but unknown backup schedule %q was ignored", schedule)
	}
	return ""
}

func runGcloud(ctx context.Context, timeout time.Duration, args ...string) ([]byte, error) {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(cctx, "gcloud", args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = err.Error()
		}
		return nil, fmt.Errorf("gcloud %s failed: %s", strings.Join(args, " "), msg)
	}
	return stdout.Bytes(), nil
}
