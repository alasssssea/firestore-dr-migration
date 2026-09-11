package migration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// BSONType represents a canonical MongoDB BSON type name used for _id partitioning and filtering.
type BSONType string

const (
	BSONTypeNumber    BSONType = "number"    // Encompasses int32, int64, double, decimal128
	BSONTypeObjectID  BSONType = "objectId"  // 12-byte BSON ObjectId
	BSONTypeString    BSONType = "string"    // UTF-8 string
	BSONTypeBinary    BSONType = "binData"   // Binary data / UUID
	BSONTypeDate      BSONType = "date"      // UTC DateTime
	BSONTypeTimestamp BSONType = "timestamp" // BSON Timestamp
	BSONTypeBool      BSONType = "bool"      // Boolean
)

// TypeRangeBoundary represents the range boundaries and progress bookmark for a specific BSON _id type.
// Bounds semantics:
//   - RangeStartID: Lower boundary for the partition (inclusive: $gte). Nil for the first partition (unbounded start).
//   - RangeEndID: Upper boundary for the partition (exclusive: $lt). Nil for the last partition (unbounded end).
//   - SavedLastID: The progress bookmark for this type. The document with this ID has been migrated, but to keep the logic easy, we use the same semantics as the partition boundaries (inclusive resume: $gte). Nil if no documents have been migrated yet.
type TypeRangeBoundary struct {
	BSONType     BSONType `bson:"bsonType" json:"bsonType"`
	RangeStartID any      `bson:"rangeStartId,omitempty" json:"rangeStartId,omitempty"`
	RangeEndID   any      `bson:"rangeEndId,omitempty" json:"rangeEndId,omitempty"`
	SavedLastID  any      `bson:"savedLastId,omitempty" json:"savedLastId,omitempty"`
}

// PartitionCheckpoint represents the checkpoint state for a single reader partition.
type PartitionCheckpoint struct {
	Database                string                          `bson:"database" json:"database"`
	Collection              string                          `bson:"collection" json:"collection"`
	PartitionIndex          int                             `bson:"partitionIndex" json:"partitionIndex"`
	TotalSplits             int                             `bson:"totalSplits" json:"totalSplits"`
	TypeProgress            map[BSONType]*TypeRangeBoundary `bson:"typeProgress" json:"typeProgress"`
	ApproximateDocsMigrated int64                           `bson:"approximateDocsMigrated" json:"approximateDocsMigrated"`
	UpdatedAt               time.Time                       `bson:"updatedAt" json:"updatedAt"`
}

// getCollectionPartitionPrefix returns the standard filename prefix for a given database and collection partition checkpoint.
func getCollectionPartitionPrefix(db, collection string) string {
	return fmt.Sprintf("backfillCheckpoint-%s-%s-partition-", db, collection)
}

// GetPartitionCheckpointPath formats the standard file path for a partition checkpoint file.
func GetPartitionCheckpointPath(dir, db, collection string, partitionIndex, totalSplits int) string {
	filename := fmt.Sprintf("%s%d-of-%d.json", getCollectionPartitionPrefix(db, collection), partitionIndex, totalSplits)
	return filepath.Join(dir, filename)
}

// SavePartitionCheckpoint saves a partition checkpoint atomically using Extended JSON.
func SavePartitionCheckpoint(filePath string, checkpoint *PartitionCheckpoint) error {
	if checkpoint == nil {
		return fmt.Errorf("partition checkpoint should not be nil")
	}

	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	if checkpoint.UpdatedAt.IsZero() {
		checkpoint.UpdatedAt = time.Now().UTC()
	}

	data, err := bson.MarshalExtJSON(checkpoint, true, false)
	if err != nil {
		return fmt.Errorf("failed to marshal partition checkpoint with extended JSON: %w", err)
	}

	var formatted bytes.Buffer
	if err := json.Indent(&formatted, data, "", "  "); err != nil {
		return fmt.Errorf("failed to format partition checkpoint JSON: %w", err)
	}
	formatted.WriteByte('\n')

	return writeAtomic(filePath, formatted.Bytes(), 0644)
}

// LoadPartitionCheckpoint loads a partition checkpoint from disk using Extended JSON.
func LoadPartitionCheckpoint(filePath string) (*PartitionCheckpoint, error) {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil, nil
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read partition checkpoint file: %w", err)
	}

	var cp PartitionCheckpoint
	if err := bson.UnmarshalExtJSON(data, false, &cp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal partition checkpoint with extended JSON: %w", err)
	}

	return &cp, nil
}

// ListPartitionCheckpoints returns all partition checkpoints for a given database and collection sorted by partition index.
func ListPartitionCheckpoints(dir, db, collection string) ([]*PartitionCheckpoint, error) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return []*PartitionCheckpoint{}, nil
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory %s: %w", dir, err)
	}

	prefix := getCollectionPartitionPrefix(db, collection)
	var checkpoints []*PartitionCheckpoint

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, prefix) && strings.HasSuffix(name, ".json") {
			cpPath := filepath.Join(dir, name)
			cp, err := LoadPartitionCheckpoint(cpPath)
			if err != nil {
				return nil, fmt.Errorf("failed to load partition checkpoint from %s: %w", cpPath, err)
			}
			if cp != nil {
				checkpoints = append(checkpoints, cp)
			}
		}
	}

	sort.Slice(checkpoints, func(i, j int) bool {
		return checkpoints[i].PartitionIndex < checkpoints[j].PartitionIndex
	})

	return checkpoints, nil
}

// DeletePartitionCheckpoints deletes all partition checkpoint files and temporary files for the specified database and collection.
func DeletePartitionCheckpoints(dir, db, collection string) error {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return nil
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("failed to read directory %s: %w", dir, err)
	}

	prefix := getCollectionPartitionPrefix(db, collection)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, prefix) && (strings.HasSuffix(name, ".json") || strings.HasSuffix(name, ".tmp")) {
			targetPath := filepath.Join(dir, name)
			if err := os.Remove(targetPath); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("failed to delete checkpoint file %s: %w", targetPath, err)
			}
		}
	}
	return nil
}

// ParseBSONType maps a MongoDB $type string name (e.g. "objectId", "int", "double") to its canonical BSONType.
func ParseBSONType(typeName string) BSONType {
	switch typeName {
	case "int", "long", "double", "decimal":
		return BSONTypeNumber
	case "objectId":
		return BSONTypeObjectID
	case "string":
		return BSONTypeString
	case "binData":
		return BSONTypeBinary
	case "date":
		return BSONTypeDate
	case "timestamp":
		return BSONTypeTimestamp
	case "bool":
		return BSONTypeBool
	default:
		return BSONType(typeName)
	}
}

// GetBSONType maps a runtime BSON _id value to its canonical BSONType using the official MongoDB driver's bson.MarshalValue.
func GetBSONType(val any) BSONType {
	if val == nil {
		return ""
	}
	t, _, err := bson.MarshalValue(val)
	if err != nil {
		return BSONTypeString
	}
	switch t {
	case bson.TypeObjectID:
		return BSONTypeObjectID
	case bson.TypeString:
		return BSONTypeString
	case bson.TypeDouble, bson.TypeInt32, bson.TypeInt64, bson.TypeDecimal128:
		return BSONTypeNumber
	case bson.TypeDateTime:
		return BSONTypeDate
	case bson.TypeTimestamp:
		return BSONTypeTimestamp
	case bson.TypeBinary:
		return BSONTypeBinary
	case bson.TypeBoolean:
		return BSONTypeBool
	default:
		return BSONTypeString
	}
}

// CandidateBSONTypes lists the candidate BSON types checked during _id type discovery.
var CandidateBSONTypes = []BSONType{
	BSONTypeObjectID,
	BSONTypeString,
	BSONTypeNumber,
	BSONTypeDate,
	BSONTypeBinary,
	BSONTypeBool,
	BSONTypeTimestamp,
}

// DiscoverPresentBSONTypeCounts probes candidate types using index-covered CountDocuments queries with a limit.
func DiscoverPresentBSONTypeCounts(ctx context.Context, collection *mongo.Collection, limit int64) (map[BSONType]int64, error) {
	if collection == nil {
		return nil, fmt.Errorf("collection cannot be nil")
	}
	if limit <= 0 {
		limit = 2000
	}

	countOpts := options.Count().SetLimit(limit)
	discoverCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	typeCounts := make(map[BSONType]int64)

	for _, bType := range CandidateBSONTypes {
		filter := bson.D{{Key: "_id", Value: bson.D{{Key: "$type", Value: string(bType)}}}}
		cnt, err := collection.CountDocuments(discoverCtx, filter, countOpts)
		if err != nil {
			return nil, fmt.Errorf("failed to probe BSON type '%s': %w", bType, err)
		}
		if cnt > 0 {
			typeCounts[bType] = cnt
		}
	}

	return typeCounts, nil
}

// DiscoverPresentBSONTypes returns the unique canonical BSONTypes of the _id field present in the collection.
func DiscoverPresentBSONTypes(ctx context.Context, collection *mongo.Collection) ([]BSONType, error) {
	counts, err := DiscoverPresentBSONTypeCounts(ctx, collection, 1)
	if err != nil {
		return nil, err
	}

	var types []BSONType
	for _, bType := range CandidateBSONTypes {
		if counts[bType] > 0 {
			types = append(types, bType)
		}
	}
	return types, nil
}
