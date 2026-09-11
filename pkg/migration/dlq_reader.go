package migration

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// This file provides a read-only reader over the DLQ JSONL files the writer
// produces (dlq-pairN.jsonl / dlq-global.jsonl). The console uses it to surface
// unresolved failures on the UI — aggregated by source collection — so an
// operator can judge, at cutover time, exactly what did NOT replicate without
// scrolling through the text log. It never writes; it only parses what the
// engine already wrote.

// DLQEntry is a single unresolved failed-document record surfaced to the UI.
type DLQEntry struct {
	SourceDB         string      `json:"sourceDB"`
	SourceCollection string      `json:"sourceCollection"`
	DocumentID       string      `json:"documentID"`
	Error            string      `json:"error"`
	Phase            string      `json:"phase"`
	OpType           string      `json:"opType,omitempty"`
	Timestamp        string      `json:"timestamp"`
	EventTime        string      `json:"eventTime,omitempty"`
	Document         interface{} `json:"document,omitempty"`
	File             string      `json:"file"`
}

// DLQCollectionSummary aggregates unresolved DLQ entries for one source
// collection (the "db.collection" the failure originated from).
type DLQCollectionSummary struct {
	SourceDB         string     `json:"sourceDB"`
	SourceCollection string     `json:"sourceCollection"`
	Count            int        `json:"count"`     // unresolved entries for this collection
	Returned         int        `json:"returned"`  // entries actually included (may be < Count if capped)
	Truncated        bool       `json:"truncated"` // true when Returned < Count
	Entries          []DLQEntry `json:"entries"`
}

// DLQReport is the aggregated, read-only view over every DLQ file in a directory.
type DLQReport struct {
	Total       int                    `json:"total"`       // total unresolved entries across all collections
	Collections []DLQCollectionSummary `json:"collections"` // sorted by db.collection
	Files       []string               `json:"files"`       // DLQ files that were read (base names)
}

// dlqKey uniquely identifies a document across the DLQ so a later "resolved"
// tombstone cancels the earlier "failed" record for the same id.
func dlqKey(db, coll, id string) string { return db + "\x00" + coll + "\x00" + id }

// stringifyDLQID renders a DLQ document id for display and de-duplication.
func stringifyDLQID(id interface{}) string {
	switch v := id.(type) {
	case nil:
		return ""
	case primitive.ObjectID:
		return v.Hex()
	case string:
		return v
	default:
		return fmt.Sprintf("%v", v)
	}
}

// ReadDLQReport scans dir for DLQ files (dlq-*.jsonl), parses each JSONL record,
// cancels failures that a later tombstone marks resolved, and returns the
// remaining unresolved entries aggregated by source collection.
//
// maxEntriesPerCollection caps how many entries are embedded per collection to
// keep the JSON payload bounded (<=0 means "no cap"); the true Count is always
// reported so the UI can show "N failures (showing M)".
func ReadDLQReport(dir string, maxEntriesPerCollection int) (DLQReport, error) {
	report := DLQReport{Collections: []DLQCollectionSummary{}, Files: []string{}}

	matches, err := filepath.Glob(filepath.Join(dir, "dlq-*.jsonl"))
	if err != nil {
		return report, err
	}
	sort.Strings(matches)

	// key -> failed entry (latest failure wins); resolved -> set of cancelled keys.
	failed := make(map[string]DLQEntry)
	// order preserves first-seen ordering so output is stable across reads.
	order := make([]string, 0)
	resolved := make(map[string]struct{})

	for _, path := range matches {
		f, err := os.Open(path)
		if err != nil {
			// A file we cannot read is skipped rather than failing the whole
			// report; the operator still sees the rest.
			continue
		}
		base := filepath.Base(path)
		report.Files = append(report.Files, base)

		scanner := bufio.NewScanner(f)
		// DLQ documents may embed the full source doc; allow large lines.
		scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" || strings.Contains(line, "\"dlqVersion\"") {
				continue // blank line or the version header
			}
			var rec DLQRecord
			if err := bson.UnmarshalExtJSON([]byte(line), false, &rec); err != nil {
				continue // skip unparseable lines rather than aborting
			}

			// A tombstone carries a ResolvedID and no failure error; it cancels
			// the matching failed record for the same document id.
			if rec.ResolvedID != nil {
				resolved[dlqKey(rec.SourceDB, rec.SourceCollection, stringifyDLQID(rec.ResolvedID))] = struct{}{}
				continue
			}
			if rec.DocumentID == nil {
				continue // not a failure and not a tombstone; ignore
			}
			id := stringifyDLQID(rec.DocumentID)
			key := dlqKey(rec.SourceDB, rec.SourceCollection, id)
			if _, seen := failed[key]; !seen {
				order = append(order, key)
			}
			failed[key] = DLQEntry{
				SourceDB:         rec.SourceDB,
				SourceCollection: rec.SourceCollection,
				DocumentID:       id,
				Error:            rec.Error,
				Phase:            rec.Phase,
				OpType:           rec.OpType,
				Timestamp:        rec.Timestamp,
				EventTime:        rec.EventTime,
				Document:         rec.Document,
				File:             base,
			}
		}
		f.Close()
	}

	// Group unresolved failures by source collection, preserving insertion order.
	type group struct {
		db, coll string
		entries  []DLQEntry
	}
	groups := make(map[string]*group)
	groupOrder := make([]string, 0)
	for _, key := range order {
		if _, gone := resolved[key]; gone {
			continue // cancelled by a later tombstone
		}
		e := failed[key]
		gk := e.SourceDB + "\x00" + e.SourceCollection
		g, ok := groups[gk]
		if !ok {
			g = &group{db: e.SourceDB, coll: e.SourceCollection}
			groups[gk] = g
			groupOrder = append(groupOrder, gk)
		}
		g.entries = append(g.entries, e)
	}

	for _, gk := range groupOrder {
		g := groups[gk]
		count := len(g.entries)
		report.Total += count
		entries := g.entries
		truncated := false
		if maxEntriesPerCollection > 0 && count > maxEntriesPerCollection {
			entries = entries[:maxEntriesPerCollection]
			truncated = true
		}
		report.Collections = append(report.Collections, DLQCollectionSummary{
			SourceDB:         g.db,
			SourceCollection: g.coll,
			Count:            count,
			Returned:         len(entries),
			Truncated:        truncated,
			Entries:          entries,
		})
	}

	sort.Slice(report.Collections, func(i, j int) bool {
		if report.Collections[i].SourceDB != report.Collections[j].SourceDB {
			return report.Collections[i].SourceDB < report.Collections[j].SourceDB
		}
		return report.Collections[i].SourceCollection < report.Collections[j].SourceCollection
	})

	return report, nil
}
