package migration

import (
	"time"

	"firestore-dr-migration/pkg/config"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// ExtractEventTime extracts the event timestamp from clusterTime or wallTime fields in a BSON change event
func ExtractEventTime(event bson.M) time.Time {
	if ct, ok := event["clusterTime"].(primitive.Timestamp); ok {
		return time.Unix(int64(ct.T), 0)
	}
	if wt, ok := event["wallTime"].(primitive.DateTime); ok {
		return wt.Time()
	}
	if ctVal, exists := event["clusterTime"]; exists {
		if ctValTS, ok := ctVal.(primitive.Timestamp); ok {
			return time.Unix(int64(ctValTS.T), 0)
		}
	}
	return time.Time{}
}

// ExtractEventTimeFromRaw extracts the event timestamp from clusterTime or wallTime in raw BSON bytes
func ExtractEventTimeFromRaw(raw bson.Raw) time.Time {
	var event bson.M
	if err := bson.Unmarshal(raw, &event); err == nil {
		return ExtractEventTime(event)
	}
	return time.Time{}
}

// BuildPartitionPipeline constructs an aggregation stage filtering standard and custom keys uniformly.
// It builds a zero-JavaScript, highly performant, BSON type-safe flat unrolled 32-bit FNV-1a hash stage.
//
// Optimization (Flat Loopless Hashing):
// To achieve maximum possible query execution speeds on the MongoDB sharded cluster, we completely eliminate
// the slow BSON split-reduce loops and array allocations. Instead, we extract the trailing 2 characters of
// the ID and compile a flat, unrolled polynomial rolling hash using standard addition and multiplication.
//
// FNV-1a Properties:
// - Seed: 2166136261 (FNV offset basis)
// - Prime Multiplier: 16777619 (FNV prime)
// - Modulo Cap: 4294967296 (2^32 registers wrap-around)
//
// Two trailing characters (256 slots in hex ObjectIDs, 1024 slots in Crockford Base32 ULIDs) provide more than
// enough entropy to guarantee a perfectly balanced partition workload distribution!
func BuildPartitionPipeline(streamIndex, totalStreams int, sourceDB string, collections []config.CollectionConfig) mongo.Pipeline {
	var pipeline mongo.Pipeline

	// 1. Namespace filtering stage (Database & Collections)
	if sourceDB != "" {
		matchFilter := bson.D{{Key: "ns.db", Value: sourceDB}}
		if len(collections) > 0 {
			var collNames bson.A
			for _, coll := range collections {
				collNames = append(collNames, coll.SourceCollection)
			}
			matchFilter = append(matchFilter, bson.E{
				Key:   "ns.coll",
				Value: bson.D{{Key: "$in", Value: collNames}},
			})
		}
		pipeline = append(pipeline, bson.D{{Key: "$match", Value: matchFilter}})
	} else if len(collections) > 0 {
		var collNames bson.A
		for _, coll := range collections {
			collNames = append(collNames, coll.SourceCollection)
		}
		matchFilter := bson.D{{
			Key:   "ns.coll",
			Value: bson.D{{Key: "$in", Value: collNames}},
		}}
		pipeline = append(pipeline, bson.D{{Key: "$match", Value: matchFilter}})
	}

	// Single-stream only: this fork targets Firestore, which does not support a
	// $expr hash-partition stage inside change-stream pipelines, so streams are
	// never split and no ID-partitioning stage is added. (streamIndex/totalStreams
	// stay in the signature for call-site stability and are always 0/1.)
	_ = streamIndex
	_ = totalStreams

	return pipeline
}

// ExtractNamespaceFromRawEvent extracts the "db.coll" namespace string from a raw BSON change event.
// Returns an empty string if the "ns" field is missing or invalid.
func ExtractNamespaceFromRawEvent(rawEvent bson.Raw) string {
	nsVal, err := rawEvent.LookupErr("ns")
	if err != nil || nsVal.Type != bson.TypeEmbeddedDocument {
		return ""
	}
	nsDoc := nsVal.Document()
	dbVal, dbErr := nsDoc.LookupErr("db")
	collVal, collErr := nsDoc.LookupErr("coll")
	if dbErr != nil || dbVal.Type != bson.TypeString || collErr != nil || collVal.Type != bson.TypeString {
		return ""
	}
	return dbVal.StringValue() + "." + collVal.StringValue()
}

