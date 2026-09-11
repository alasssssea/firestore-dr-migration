package wizard

import "testing"

func TestBuildConfig_AllCollections(t *testing.T) {
	sels := []Selection{
		{SourceDB: "sales", TargetDB: "sales", TargetConn: "mongodb://t1/sales", AllCollections: true},
	}
	cfg := BuildConfig("mongodb://src/admin", "changestream", sels)

	if len(cfg.DatabasePairs) != 1 {
		t.Fatalf("expected 1 pair, got %d", len(cfg.DatabasePairs))
	}
	p := cfg.DatabasePairs[0]
	if p.Source.ReplicationMethod != "changestream" {
		t.Errorf("replicationMethod = %q, want changestream", p.Source.ReplicationMethod)
	}
	if p.Source.Database != "sales" || p.Target.Database != "sales" {
		t.Errorf("db names wrong: src=%q tgt=%q", p.Source.Database, p.Target.Database)
	}
	if len(p.Target.Collections) != 0 {
		t.Errorf("AllCollections should leave Collections empty, got %v", p.Target.Collections)
	}
}

func TestBuildConfig_SpecificCollections(t *testing.T) {
	sels := []Selection{
		{SourceDB: "app", TargetDB: "app-fs", TargetConn: "mongodb://t/app", Collections: []string{"users", "orders"}},
	}
	cfg := BuildConfig("mongodb://src/admin", "oplog-legacy", sels)
	p := cfg.DatabasePairs[0]
	if len(p.Target.Collections) != 2 {
		t.Fatalf("expected 2 collection mappings, got %d", len(p.Target.Collections))
	}
	for _, c := range p.Target.Collections {
		if c.SourceCollection != c.TargetCollection {
			t.Errorf("expected same-name mapping, got %q -> %q", c.SourceCollection, c.TargetCollection)
		}
	}
}

func TestBuildConfig_MultipleDatabases(t *testing.T) {
	sels := []Selection{
		{SourceDB: "a", TargetDB: "a", TargetConn: "c1", AllCollections: true},
		{SourceDB: "b", TargetDB: "b", TargetConn: "c2", AllCollections: true},
	}
	cfg := BuildConfig("src", "auto", sels)
	if len(cfg.DatabasePairs) != 2 {
		t.Fatalf("expected 2 pairs, got %d", len(cfg.DatabasePairs))
	}
}
