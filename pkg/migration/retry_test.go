package migration

import (
	"context"
	"errors"
	"testing"
	"time"

	"firestore-dr-migration/pkg/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestClassifyError(t *testing.T) {
	log := logger.New()
	r := NewRetryManager(3, 10*time.Millisecond, 100*time.Millisecond, true, 2, true, log)

	// Connection errors
	if r.ClassifyError(errors.New("socket was unexpectedly closed")) != ErrorTypeConnection {
		t.Error("expected ErrorTypeConnection")
	}
	if r.ClassifyError(errors.New("EOF")) != ErrorTypeConnection {
		t.Error("expected ErrorTypeConnection")
	}
	if r.ClassifyError(errors.New("i/o timeout")) != ErrorTypeConnection {
		t.Error("expected ErrorTypeConnection")
	}

	// Contention errors
	if r.ClassifyError(errors.New("too much contention")) != ErrorTypeContention {
		t.Error("expected ErrorTypeContention")
	}
	if r.ClassifyError(errors.New("TransientTransactionError")) != ErrorTypeContention {
		t.Error("expected ErrorTypeContention")
	}
	if r.ClassifyError(errors.New("request was aborted due to a schema change involving the indexes used in the request. Retry the request to run against the updated schema")) != ErrorTypeContention {
		t.Error("expected ErrorTypeContention for Firestore schema change index error")
	}

	// Invalid _id type error
	if r.ClassifyError(errors.New("_id must be an objectId, string, long")) != ErrorTypeInvalidIdType {
		t.Error("expected ErrorTypeInvalidIdType")
	}

	// Other errors
	if r.ClassifyError(errors.New("some other random database error")) != ErrorTypeOther {
		t.Error("expected ErrorTypeOther")
	}
}

func TestRetryWithBackoff(t *testing.T) {
	log := logger.New()
	r := NewRetryManager(3, 5*time.Millisecond, 20*time.Millisecond, false, 1, false, log)
	ctx := context.Background()

	// 1. Successful on second attempt
	attempts := 0
	err := r.RetryWithBackoff(ctx, func() error {
		attempts++
		if attempts < 2 {
			return errors.New("temporary connection error")
		}
		return nil
	})

	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if attempts != 2 {
		t.Errorf("expected 2 attempts, got %d", attempts)
	}

	// 2. Exceed max retries
	attempts = 0
	err = r.RetryWithBackoff(ctx, func() error {
		attempts++
		return errors.New("persistent failure")
	})

	if err == nil || err.Error() != "persistent failure" {
		t.Errorf("expected persistent failure error, got %v", err)
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}

	// 3. Context canceled before retrying
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel() // Cancel immediately

	err = r.RetryWithBackoff(cancelCtx, func() error {
		return errors.New("should not be called multiple times")
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled error, got %v", err)
	}
}

func TestRetryWithSplit(t *testing.T) {
	log := logger.New()
	r := NewRetryManager(3, 5*time.Millisecond, 20*time.Millisecond, true, 1, true, log)
	ctx := context.Background()

	batch := []interface{}{
		bson.M{"_id": 1.23, "name": "invalid float _id"}, // Invalid type (float64)
		bson.M{"_id": "valid_str", "name": "valid string _id"},
	}

	// Test _id conversion capability
	err := r.RetryWithSplit(ctx, batch, "test_coll", func(b []interface{}) error {
		for _, doc := range b {
			m := doc.(bson.M)
			id := m["_id"]
			switch id.(type) {
			case string, int64, primitive.ObjectID:
				// Acceptable types
			default:
				return errors.New("_id must be an objectId, string, long")
			}
		}
		return nil
	})

	if err != nil {
		t.Fatalf("expected success after string conversion, got %v", err)
	}

	// Verify first document was converted to string
	res := r.convertInvalidIds(batch, nil, "test_coll")
	firstID := res[0].(bson.M)["_id"]
	if _, ok := firstID.(string); !ok {
		t.Errorf("expected first _id to be converted to string, got %T (%v)", firstID, firstID)
	}
}
