package migration

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"time"

	"firestore-dr-migration/pkg/config"
	"firestore-dr-migration/pkg/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// ErrorType categorizes different types of errors
type ErrorType int

const (
	ErrorTypeConnection ErrorType = iota
	ErrorTypeContention
	ErrorTypeInvalidIdType // New error type for invalid _id type errors
	ErrorTypeOther
)

// RetryManager handles retrying operations with different strategies
type RetryManager struct {
	MaxRetries        int
	BaseDelay         time.Duration
	MaxDelay          time.Duration
	EnableBatchSplit  bool
	MinBatchSize      int
	ConvertInvalidIds bool // New field to control _id conversion
	Logger            *logger.Logger
}

// NewRetryManagerFromConfig creates a RetryManager from config settings
func NewRetryManagerFromConfig(cfg *config.Config, log *logger.Logger) *RetryManager {
	return NewRetryManager(
		cfg.RetryConfig.MaxRetries,
		time.Duration(cfg.RetryConfig.BaseDelayMs)*time.Millisecond,
		time.Duration(cfg.RetryConfig.MaxDelayMs)*time.Millisecond,
		cfg.RetryConfig.EnableBatchSplitting,
		cfg.RetryConfig.MinBatchSize,
		cfg.RetryConfig.ConvertInvalidIds,
		log,
	)
}

// NewRetryManager creates a new retry manager
func NewRetryManager(maxRetries int, baseDelay, maxDelay time.Duration, enableBatchSplit bool, minBatchSize int, convertInvalidIds bool, log *logger.Logger) *RetryManager {
	return &RetryManager{
		MaxRetries:        maxRetries,
		BaseDelay:         baseDelay,
		MaxDelay:          maxDelay,
		EnableBatchSplit:  enableBatchSplit,
		MinBatchSize:      minBatchSize,
		ConvertInvalidIds: convertInvalidIds,
		Logger:            log,
	}
}

// ClassifyError determines the type of error
func (r *RetryManager) ClassifyError(err error) ErrorType {
	if err == nil {
		return ErrorTypeOther
	}

	errStr := err.Error()

	// Check for connection / transient-unavailable errors. This bucket gets the
	// longest backoff (regular exponential + an extra 5s per attempt), so brief
	// server-side unavailability windows are ridden out inline instead of burning
	// through the short default budget and dumping the doc into the DLQ.
	//
	// Firestore's MongoDB-compat endpoint returns "ShutdownInProgress" ("The
	// service is temporarily unavailable. Please retry with exponential backoff.")
	// during backend instance reschedules — an explicitly retryable, transient
	// condition, NOT a permanent failure. Treat it (and the generic "Unavailable"
	// server code) as a connection-class transient so it retries with a real budget.
	if strings.Contains(errStr, "socket was unexpectedly closed") ||
		strings.Contains(errStr, "EOF") ||
		strings.Contains(errStr, "connection reset by peer") ||
		strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "i/o timeout") ||
		strings.Contains(errStr, "DeadlineExceeded") ||
		strings.Contains(errStr, "Deadline exceeded") ||
		strings.Contains(errStr, "deadline exceeded") ||
		strings.Contains(errStr, "ShutdownInProgress") ||
		strings.Contains(errStr, "service is temporarily unavailable") ||
		strings.Contains(errStr, "Please retry with exponential backoff") ||
		strings.Contains(errStr, "(Unavailable)") ||
		strings.Contains(errStr, "Service Unavailable") {
		return ErrorTypeConnection
	}

	// Check for contention errors (includes Firestore "schema change" during concurrent index builds)
	if strings.Contains(errStr, "too much contention") ||
		strings.Contains(errStr, "lock timeout") ||
		strings.Contains(errStr, "OperationFailed") && strings.Contains(errStr, "Aborted") ||
		strings.Contains(errStr, "TransientTransactionError") ||
		strings.Contains(errStr, "WriteConflict") ||
		strings.Contains(errStr, "schema change") ||
		strings.Contains(errStr, "exceeded time limit") {
		return ErrorTypeContention
	}

	// Check for invalid _id type errors
	if strings.Contains(errStr, "_id must be an objectId, string, long") {
		r.Logger.Debugf("Invalid _id type error detected: %s", errStr)
		return ErrorTypeInvalidIdType
	}

	return ErrorTypeOther
}

// RetryWithBackoff retries an operation with exponential backoff
func (r *RetryManager) RetryWithBackoff(ctx context.Context, operation func() error) error {
	var err error

	for attempt := 0; attempt < r.MaxRetries; attempt++ {
		err = operation()
		if err == nil {
			return nil
		}

		// Check for context cancellation
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Special handling for contention errors - use a fixed 5-second delay
		if r.ClassifyError(err) == ErrorTypeContention {
			r.Logger.Infof("Contention error detected. Waiting 5 seconds before retry %d/%d...",
				attempt+1, r.MaxRetries)

			// Wait for 5 seconds or until context is canceled
			select {
			case <-time.After(5 * time.Second):
				// Continue to next attempt
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}

		// Special handling for connection errors - add extra 5 seconds to the backoff
		if r.ClassifyError(err) == ErrorTypeConnection {
			// Calculate regular backoff
			regularDelay := r.calculateBackoff(attempt)
			// Add extra 5 seconds
			totalDelay := regularDelay + (5 * time.Second)

			r.Logger.Infof("Connection error detected. Adding extra 5 seconds to backoff. Waiting %v before retry %d/%d...",
				totalDelay, attempt+1, r.MaxRetries)

			// Wait for the total delay or until context is canceled
			select {
			case <-time.After(totalDelay):
				// Continue to next attempt
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}

		// Calculate delay with exponential backoff and jitter for non-contention errors
		delay := r.calculateBackoff(attempt)

		r.Logger.Debugf("Operation failed (attempt %d/%d): %v. Retrying in %v...",
			attempt+1, r.MaxRetries, err, delay)

		// Wait for the delay or until context is canceled
		select {
		case <-time.After(delay):
			// Continue to next attempt
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return err
}

// RetryWithSplit retries a batch operation with progressive splitting
func (r *RetryManager) RetryWithSplit(ctx context.Context, batch []interface{},
	collectionName string, processBatch func([]interface{}) error) error {

	// Try processing the full batch first
	err := processBatch(batch)
	if err == nil {
		return nil
	}

	// If it's an invalid _id type error and conversion is enabled
	if r.ClassifyError(err) == ErrorTypeInvalidIdType && r.ConvertInvalidIds {
		r.Logger.Infof("Collection '%s': Invalid _id type error detected. Converting problematic _ids to strings.",
			collectionName)

		// Extract failed indices if possible
		failedIndices, extractErr := r.extractFailedIndicesFromError(err)
		if extractErr != nil {
			r.Logger.Debugf("Could not extract failed indices: %v. Will check all documents.", extractErr)
		}

		// Convert invalid _ids, passing collection name
		convertedBatch := r.convertInvalidIds(batch, failedIndices, collectionName)

		// Retry with the converted batch
		retryErr := processBatch(convertedBatch)
		if retryErr == nil {
			r.Logger.Infof("Collection '%s': Retry with converted _id fields succeeded",
				collectionName)
		} else {
			r.Logger.Warnf("Collection '%s': Retry with converted _id fields failed: %v",
				collectionName, retryErr)
		}

		return retryErr
	}

	// If batch splitting is not enabled or batch is already small, use backoff
	if !r.EnableBatchSplit || len(batch) <= r.MinBatchSize {
		return r.RetryWithBackoff(ctx, func() error {
			return processBatch(batch)
		})
	}

	// If it's a contention error, split the batch and retry
	if r.ClassifyError(err) == ErrorTypeContention {
		r.Logger.Debugf("Contention error detected. Splitting batch of size %d", len(batch))

		// Split the batch in half
		mid := len(batch) / 2
		firstHalf := batch[:mid]
		secondHalf := batch[mid:]

		// Process each half
		err1 := r.RetryWithSplit(ctx, firstHalf, collectionName, processBatch)
		if ctx.Err() != nil {
			return ctx.Err()
		}

		err2 := r.RetryWithSplit(ctx, secondHalf, collectionName, processBatch)

		// If both halves succeeded, return nil
		if err1 == nil && err2 == nil {
			return nil
		}

		// Otherwise, return the first non-nil error
		if err1 != nil {
			return err1
		}
		return err2
	}

	// For connection errors, use backoff with the full batch
	if r.ClassifyError(err) == ErrorTypeConnection {
		r.Logger.Debugf("Connection error detected. Retrying batch of size %d with backoff", len(batch))
		return r.RetryWithBackoff(ctx, func() error {
			return processBatch(batch)
		})
	}

	// For duplicate key errors, use upsert instead of retrying
	var bulkWriteErr mongo.BulkWriteException
	if errors.As(err, &bulkWriteErr) {
		// Check if all errors are duplicate key errors
		allDuplicateKeyErrors := true
		for _, writeErr := range bulkWriteErr.WriteErrors {
			if writeErr.Code != 11000 { // 11000 is the code for duplicate key error
				allDuplicateKeyErrors = false
				break
			}
		}

		if allDuplicateKeyErrors {
			r.Logger.Debugf("Bulk write error detected with %d duplicate key errors. Using upsert instead.",
				len(bulkWriteErr.WriteErrors))

			// Extract the documents that failed due to duplicate keys
			failedIndices := make(map[int]bool)
			for _, writeErr := range bulkWriteErr.WriteErrors {
				failedIndices[writeErr.Index] = true
			}

			// Create a new batch with only the failed documents
			var failedBatch []interface{}
			for i, doc := range batch {
				if failedIndices[i] {
					failedBatch = append(failedBatch, doc)
				}
			}

			// Use upsert for the failed documents
			return processBatch(failedBatch)
		}

		// For other bulk write errors, retry with backoff
		r.Logger.Debugf("Bulk write error detected with %d non-duplicate key errors. Retrying with backoff.",
			len(bulkWriteErr.WriteErrors))
	}

	// For other errors, use backoff with the full batch
	return r.RetryWithBackoff(ctx, func() error {
		return processBatch(batch)
	})
}

// calculateBackoff calculates the backoff delay with jitter
func (r *RetryManager) calculateBackoff(attempt int) time.Duration {
	// Calculate exponential backoff: baseDelay * 2^attempt
	backoff := float64(r.BaseDelay) * math.Pow(2, float64(attempt))

	// Add jitter: random value between 0.5 and 1.5 of the calculated backoff
	jitter := 0.5 + rand.Float64()
	backoff = backoff * jitter

	// Cap at max delay
	if backoff > float64(r.MaxDelay) {
		backoff = float64(r.MaxDelay)
	}

	return time.Duration(backoff)
}

// extractFailedIndicesFromError extracts the indices of documents that failed due to invalid _id type
func (r *RetryManager) extractFailedIndicesFromError(err error) (map[int]bool, error) {
	// This is a simplified implementation that doesn't extract specific indices
	// In a real implementation, we would parse the error message to get specific indices
	// For now, we'll return nil to indicate that all documents should be checked
	return nil, nil
}

// convertInvalidIds converts invalid _id types to strings for specific documents
func (r *RetryManager) convertInvalidIds(batch []interface{}, failedIndices map[int]bool, collectionName string) []interface{} {
	// If no specific indices are provided, check all documents
	checkAll := failedIndices == nil || len(failedIndices) == 0

	// Create a new batch with converted _ids where needed
	result := make([]interface{}, len(batch))
	convertedCount := 0

	for i, doc := range batch {
		// Only process documents that failed or check all if no specific indices
		if checkAll || failedIndices[i] {
			switch d := doc.(type) {
			case bson.D:
				// Create a copy of the document
				newDoc := make(bson.D, len(d))
				copy(newDoc, d)

				// Find and possibly convert the _id field
				for j, elem := range newDoc {
					if elem.Key == "_id" {
						// Check if _id is not an ObjectId, string, or int64
						switch elem.Value.(type) {
						case primitive.ObjectID, string, int64:
							// These types are acceptable, no conversion needed
						default:
							// Log with collection and _id information
							r.Logger.Infof("Collection '%s': Converting _id %v (type: %T) to string",
								collectionName, elem.Value, elem.Value)

							// Convert to string
							newDoc[j].Value = fmt.Sprintf("%v", elem.Value)
							convertedCount++
						}
						break
					}
				}
				result[i] = newDoc
			case bson.M:
				// Create a copy of the document
				newDoc := make(bson.M, len(d))
				for k, v := range d {
					newDoc[k] = v
				}

				// Check if _id needs conversion
				if id, ok := newDoc["_id"]; ok {
					switch id.(type) {
					case primitive.ObjectID, string, int64:
						// These types are acceptable, no conversion needed
					default:
						// Log with collection and _id information
						r.Logger.Infof("Collection '%s': Converting _id %v (type: %T) to string",
							collectionName, id, id)

						// Convert to string
						newDoc["_id"] = fmt.Sprintf("%v", id)
						convertedCount++
					}
				}
				result[i] = newDoc
			default:
				// Unknown document type, keep as is
				result[i] = doc
			}
		} else {
			// Document didn't fail, keep as is
			result[i] = doc
		}
	}

	// Summary log
	if convertedCount > 0 {
		r.Logger.Infof("Collection '%s': Converted %d _id fields to strings",
			collectionName, convertedCount)
	}

	return result
}
