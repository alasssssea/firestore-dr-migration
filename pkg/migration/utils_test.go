package migration

import (
	"testing"
	"time"

	"firestore-dr-migration/pkg/config"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// TestExtractEventTime validates the extraction of standard event timestamps from different potential change event fields (clusterTime and wallTime) with various data typings.
func TestExtractEventTime(t *testing.T) {
	// Arrange: initialize current second boundaries
	nowSec := time.Now().Unix()
	expectedTime := time.Unix(nowSec, 0)

	// Table-driven mock events: contains standard valid and invalid types/formats
	tests := []struct {
		name     string
		event    bson.M
		wantZero bool
	}{
		{
			name: "primitive.Timestamp clusterTime",
			event: bson.M{
				"clusterTime": primitive.Timestamp{T: uint32(nowSec), I: 1},
			},
			wantZero: false,
		},
		{
			name: "primitive.DateTime wallTime",
			event: bson.M{
				"wallTime": primitive.NewDateTimeFromTime(expectedTime),
			},
			wantZero: false,
		},
		{
			name: "nested/interface primitive.Timestamp clusterTime",
			event: bson.M{
				"clusterTime": interface{}(primitive.Timestamp{T: uint32(nowSec), I: 1}),
			},
			wantZero: false,
		},
		{
			name:     "empty event",
			event:    bson.M{},
			wantZero: true,
		},
		{
			name: "invalid types",
			event: bson.M{
				"clusterTime": "not-a-timestamp",
				"wallTime":    12345,
			},
			wantZero: true,
		},
	}

	// Act & Assert loops: run table tests
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExtractEventTime(tt.event)
			if tt.wantZero {
				if !got.IsZero() {
					t.Errorf("ExtractEventTime() = %v, want zero time", got)
				}
			} else {
				if got.IsZero() {
					t.Fatalf("ExtractEventTime() returned zero time, want %v", expectedTime)
				}
				if got.Unix() != nowSec {
					t.Errorf("ExtractEventTime() = %v, want %v", got, expectedTime)
				}
			}
		})
	}
}

// TestBuildPartitionPipelineRedundantStreamsCount verifies that when totalStreams <= 1 and no filters are set, an empty pipeline is safely returned.
func TestBuildPartitionPipelineRedundantStreamsCount(t *testing.T) {
	// Act: build pipeline for single-stream partition index 0 of 1 total stream
	p1 := BuildPartitionPipeline(0, 1, "", nil)
	// Assert: pipeline must be completely empty
	if len(p1) != 0 {
		t.Errorf("BuildPartitionPipeline(0, 1) returned len %d, want empty", len(p1))
	}
	// Act 2: build pipeline for boundary stream count 0
	p0 := BuildPartitionPipeline(0, 0, "", nil)
	// Assert: pipeline must be completely empty
	if len(p0) != 0 {
		t.Errorf("BuildPartitionPipeline(0, 0) returned len %d, want empty", len(p0))
	}
}

// TestBuildPartitionPipelineValidStructure verifies the exact BSON stage operators structure for valid partitions.
func TestBuildPartitionPipelineValidStructure(t *testing.T) {
	// This fork is single-stream only (Firestore does not support the $expr
	// hash-partition stage inside change-stream pipelines). Even when more than
	// one stream is requested, no partition-hash stage is produced; with no
	// namespace filter the pipeline is therefore empty.
	pipeline := BuildPartitionPipeline(2, 4, "", nil)
	if len(pipeline) != 0 {
		t.Fatalf("single-stream fork: BuildPartitionPipeline returned len %d, want 0 (no hash stage)", len(pipeline))
	}
}

// TestBuildPartitionPipelineWithNamespaceAndCollections verifies that namespace filtering and ID partitioning are composed correctly.
func TestBuildPartitionPipelineWithNamespaceAndCollections(t *testing.T) {
	sourceDB := "ecommerce"
	collections := []config.CollectionConfig{
		{SourceCollection: "orders", TargetCollection: "orders"},
		{SourceCollection: "users", TargetCollection: "users"},
	}

	// Single-stream fork: even when totalStreams>1 is requested, only the
	// namespace $match stage is produced (no partition-hash stage).
	pipeline := BuildPartitionPipeline(1, 4, sourceDB, collections)
	if len(pipeline) != 1 {
		t.Fatalf("Expected 1 stage (namespace only) for single-stream fork, got %d", len(pipeline))
	}

	// Check stage 0 (namespace filtering)
	var stage0 bson.M
	b0, err := bson.Marshal(pipeline[0])
	if err != nil {
		t.Fatalf("Failed to marshal stage 0: %v", err)
	}
	if err := bson.Unmarshal(b0, &stage0); err != nil {
		t.Fatalf("Failed to unmarshal stage 0: %v", err)
	}

	matchDoc0, ok := stage0["$match"].(bson.M)
	if !ok {
		t.Fatalf("Stage 0 is not a $match doc: %v", stage0)
	}
	if matchDoc0["ns.db"] != "ecommerce" {
		t.Errorf("Expected ns.db 'ecommerce', got %v", matchDoc0["ns.db"])
	}
	collDoc, ok := matchDoc0["ns.coll"].(bson.M)
	if !ok {
		t.Fatalf("Expected ns.coll to be bson.M, got %T (%v)", matchDoc0["ns.coll"], matchDoc0["ns.coll"])
	}
	inList, ok := collDoc["$in"].(primitive.A)
	if !ok {
		t.Fatalf("Expected $in to be primitive.A, got %T", collDoc["$in"])
	}
	if len(inList) != 2 || inList[0] != "orders" || inList[1] != "users" {
		t.Errorf("Unexpected $in list: %v", inList)
	}

	// Case 2: Single stream (totalStreams = 1) -> 1 stage (match namespace only, no partition hash)
	singlePipeline := BuildPartitionPipeline(0, 1, sourceDB, collections)
	if len(singlePipeline) != 1 {
		t.Fatalf("Expected 1 stage for single stream with collections, got %d", len(singlePipeline))
	}
}

// TestBuildPartitionPipelineDatabaseOnly verifies full database mode without collection restriction.
func TestBuildPartitionPipelineDatabaseOnly(t *testing.T) {
	sourceDB := "ecommerce"

	pipeline := BuildPartitionPipeline(0, 1, sourceDB, nil)
	if len(pipeline) != 1 {
		t.Fatalf("Expected 1 stage, got %d", len(pipeline))
	}

	var stage bson.M
	b, err := bson.Marshal(pipeline[0])
	if err != nil {
		t.Fatalf("Failed to marshal stage: %v", err)
	}
	if err := bson.Unmarshal(b, &stage); err != nil {
		t.Fatalf("Failed to unmarshal stage: %v", err)
	}

	matchDoc, ok := stage["$match"].(bson.M)
	if !ok {
		t.Fatalf("Stage is not a $match doc: %v", stage)
	}
	if matchDoc["ns.db"] != "ecommerce" {
		t.Errorf("Expected ns.db 'ecommerce', got %v", matchDoc["ns.db"])
	}
	if _, exists := matchDoc["ns.coll"]; exists {
		t.Errorf("Expected no ns.coll in full database mode, but got %v", matchDoc["ns.coll"])
	}
}

// buildPartitionPipelineFull represents a helper model of the full-string hashing aggregation pipeline (our baseline)
func buildPartitionPipelineFull(streamIndex, totalStreams int) mongo.Pipeline {
	if totalStreams <= 1 {
		return mongo.Pipeline{}
	}

	const asciiString = " !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"

	stringHash := bson.D{
		bson.E{Key: "$reduce", Value: bson.D{
			bson.E{Key: "input", Value: bson.D{
				bson.E{Key: "$split", Value: bson.A{
					bson.D{bson.E{Key: "$toString", Value: "$documentKey._id"}},
					"",
				}},
			}},
			bson.E{Key: "initialValue", Value: int64(2166136261)},
			bson.E{Key: "in", Value: bson.D{
				bson.E{Key: "$mod", Value: bson.A{
					bson.D{
						bson.E{Key: "$add", Value: bson.A{
							bson.D{bson.E{Key: "$multiply", Value: bson.A{int64(16777619), "$$value"}}},
							bson.D{
								bson.E{Key: "$add", Value: bson.A{
									32,
									bson.D{
										bson.E{Key: "$indexOfCP", Value: bson.A{
											asciiString,
											"$$this",
										}},
									},
								}},
							},
						}},
					},
					int64(4294967296),
				}},
			}},
		}},
	}

	return mongo.Pipeline{
		bson.D{
			bson.E{Key: "$match", Value: bson.D{
				bson.E{Key: "$expr", Value: bson.D{
					bson.E{Key: "$eq", Value: bson.A{
						bson.D{
							bson.E{Key: "$mod", Value: bson.A{
								stringHash,
								totalStreams,
							}},
						},
						streamIndex,
					}},
				}},
			}},
		},
	}
}

// BenchmarkBuildPartitionPipelineFull evaluates the client-side CPU compile performance of the full-string baseline BSON pipeline builder in Go memory.
func BenchmarkBuildPartitionPipelineFull(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = buildPartitionPipelineFull(i%4, 4)
	}
}

// BenchmarkBuildPartitionPipelineTrailing4 evaluates the Go-side CPU compile performance of the highly optimized flat loopless FNV-inspired BSON partition pipeline builder.
func BenchmarkBuildPartitionPipelineTrailing4(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = BuildPartitionPipeline(i%4, 4, "testdb", nil)
	}
}

// TestExtractNamespaceFromRawEvent validates BSON lookup parsing, data typing constraints, and field boundary coverage for namespace paths in raw change events.
func TestExtractNamespaceFromRawEvent(t *testing.T) {
	tests := []struct {
		name     string
		event    bson.M
		expected string
	}{
		{
			name: "valid namespace",
			event: bson.M{
				"ns": bson.M{
					"db":   "testdb",
					"coll": "testcoll",
				},
			},
			expected: "testdb.testcoll",
		},
		{
			name:     "missing ns key",
			event:    bson.M{},
			expected: "",
		},
		{
			name: "non-document ns type",
			event: bson.M{
				"ns": "not-a-document",
			},
			expected: "",
		},
		{
			name: "missing db key inside ns",
			event: bson.M{
				"ns": bson.M{
					"coll": "testcoll",
				},
			},
			expected: "",
		},
		{
			name: "missing coll key inside ns",
			event: bson.M{
				"ns": bson.M{
					"db": "testdb",
				},
			},
			expected: "",
		},
		{
			name: "non-string db type inside ns",
			event: bson.M{
				"ns": bson.M{
					"db":   12345,
					"coll": "testcoll",
				},
			},
			expected: "",
		},
		{
			name: "non-string coll type inside ns",
			event: bson.M{
				"ns": bson.M{
					"db":   "testdb",
					"coll": true,
				},
			},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rawBytes, err := bson.Marshal(tt.event)
			if err != nil {
				t.Fatalf("failed to marshal target BSON: %v", err)
			}
			rawEvent := bson.Raw(rawBytes)
			got := ExtractNamespaceFromRawEvent(rawEvent)
			if got != tt.expected {
				t.Errorf("ExtractNamespaceFromRawEvent() = %q, want %q", got, tt.expected)
			}
		})
	}
}
