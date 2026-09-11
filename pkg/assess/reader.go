package assess

import (
	"context"
	"fmt"
	"sort"
	"time"

	"firestore-dr-migration/pkg/db"
	"firestore-dr-migration/pkg/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// sourceReader abstracts reading a source database for assessment so the same
// document-level rules run against both a modern-driver source (MongoDB ≥ 3.6)
// and a legacy mgo source (MongoDB 3.0–3.4, which the modern Go driver refuses
// to talk to — it rejects wire version < 6). Sampled documents are always
// returned as mongo-driver bson.M so the shared rule walker sees uniform types.
type sourceReader interface {
	// listCollections returns the user collections in the database.
	listCollections(ctx context.Context) ([]string, error)
	// indexCount returns the number of indexes on a collection.
	indexCount(ctx context.Context, coll string) (int, error)
	// countDocs returns the true document count of a collection.
	countDocs(ctx context.Context, coll string) (int64, error)
	// sample returns up to want documents (all of them when the collection is at
	// or below want), normalized to mongo-driver bson.M.
	sample(ctx context.Context, coll string, total, want int64) ([]bson.M, error)
	close(ctx context.Context)
}

// newSourceReader returns a modern-driver source reader (MongoDB ≥ 3.6).
func newSourceReader(connectionString, database, replicationMethod string, log *logger.Logger) (sourceReader, error) {
	return newModernReader(connectionString, database, log)
}

/* ---------- modern (mongo-driver) reader ---------- */

type modernReader struct{ m *db.MongoDB }

func newModernReader(connectionString, database string, log *logger.Logger) (sourceReader, error) {
	m, err := db.NewMongoDB(connectionString, database, 0, 4, 30*time.Second, nil, log)
	if err != nil {
		return nil, err
	}
	return &modernReader{m: m}, nil
}

func (r *modernReader) close(ctx context.Context) { r.m.Close(ctx) }

func (r *modernReader) listCollections(ctx context.Context) ([]string, error) {
	names, err := r.m.GetClient().Database(r.m.GetDatabaseName()).ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("list collections: %w", err)
	}
	sort.Strings(names)
	return names, nil
}

func (r *modernReader) indexCount(ctx context.Context, coll string) (int, error) {
	idxs, err := r.m.ListIndexes(ctx, coll)
	if err != nil {
		return 0, err
	}
	return len(idxs), nil
}

func (r *modernReader) countDocs(ctx context.Context, coll string) (int64, error) {
	return r.m.GetCollection(coll).CountDocuments(ctx, bson.D{})
}

func (r *modernReader) sample(ctx context.Context, coll string, total, want int64) ([]bson.M, error) {
	c := r.m.GetCollection(coll)
	cctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	var cursor interface {
		Next(context.Context) bool
		Decode(interface{}) error
		Close(context.Context) error
	}
	if total > want {
		cur, err := c.Aggregate(cctx, bson.A{bson.D{{Key: "$sample", Value: bson.D{{Key: "size", Value: want}}}}})
		if err != nil {
			return nil, err
		}
		cursor = cur
	} else {
		cur, err := c.Find(cctx, bson.D{}, options.Find())
		if err != nil {
			return nil, err
		}
		cursor = cur
	}
	defer cursor.Close(cctx)

	var docs []bson.M
	for cursor.Next(cctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			continue
		}
		docs = append(docs, doc)
	}
	return docs, nil
}

