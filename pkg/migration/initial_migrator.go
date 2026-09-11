package migration

import (
	"context"
	"fmt"
	"sync"
	"time"

	"firestore-dr-migration/pkg/config"
	"firestore-dr-migration/pkg/db"
	"firestore-dr-migration/pkg/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// InitialMigrator handles the shared initial backfill migration for modern MongoDB drivers
type InitialMigrator struct {
	sourceDB          *db.MongoDB
	targetDB          *db.MongoDB
	config            *config.Config
	log               *logger.Logger
	collectionConfigs map[string]map[string]config.CollectionConfig
	dlq               DLQ
	retryManager      *RetryManager
	transformer       *FieldTransformer
	DryRun            bool
}

// NewInitialMigrator creates a new shared initial migrator
func NewInitialMigrator(sourceDB, targetDB *db.MongoDB, cfg *config.Config, log *logger.Logger, collectionConfigs map[string]map[string]config.CollectionConfig, dlq DLQ, retryManager *RetryManager) *InitialMigrator {
	return &InitialMigrator{
		sourceDB:          sourceDB,
		targetDB:          targetDB,
		config:            cfg,
		log:               log,
		collectionConfigs: collectionConfigs,
		dlq:               dlq,
		retryManager:      retryManager,
		transformer:       NewFieldTransformer(cfg.DropEmptyFieldNames, cfg.ConvertLongFieldNamesInNestedDocs, cfg.RetryConfig.ConvertInvalidIds, log),
	}
}

// Run executes the shared initial migration process for all registered collections
func (r *InitialMigrator) Run(ctx context.Context, pair config.DatabasePair, migrator *Migrator) (int64, int64, error) {
	initialMigrationStart := time.Now()
	r.log.Info("Performing initial migration for all collections (shared pipeline)")

	// Sync indexes before migrating data. Always runs: syncIndexes creates the
	// _id index unconditionally (Firestore does not auto-create it) and only the
	// secondary indexes are gated on SyncAllIndexes / explicit Indexes.
	{
		r.log.Info("Syncing indexes before initial migration")
		var collections []config.CollectionConfig
		for _, colls := range r.collectionConfigs {
			for _, collConfig := range colls {
				collections = append(collections, collConfig)
			}
		}
		if err := migrator.syncIndexes(ctx, r.sourceDB, r.targetDB, pair, collections); err != nil {
			r.log.Warnf("Index sync encountered issues: %v (continuing with migration)", err)
		}

		if pair.Target.IndexOnly {
			r.log.Info("IndexOnly mode enabled. Waiting for all async index creation to complete...")
			r.targetDB.WaitForIndexCreation()
			r.log.Info("IndexOnly mode: all indexes synced successfully. Skipping data migration.")
			return 0, 0, nil
		}
	}

	concurrentCollections := r.config.ConcurrentCollections
	if concurrentCollections <= 0 {
		concurrentCollections = 4
	}
	r.log.Infof("Processing up to %d collections concurrently", concurrentCollections)
	semaphore := make(chan struct{}, concurrentCollections)
	var wg sync.WaitGroup

	var totalMigratedCount int64
	var totalFailedCount int64
	var completedCollections int64
	var mu sync.Mutex

	totalCollections := 0
	for _, colls := range r.collectionConfigs {
		totalCollections += len(colls)
	}

	for sourceDB, collections := range r.collectionConfigs {
		for sourceCollection, collConfig := range collections {
			wg.Add(1)
			semaphore <- struct{}{}

			go func(sourceDB, sourceCollection string, collConfig config.CollectionConfig) {
				defer wg.Done()
				defer func() { <-semaphore }()

				targetCollection := collConfig.TargetCollection
				// Apply a rename-collection remediation (reserved __x__ → _x_) so the
				// initial load lands in the sanitized collection the assessment
				// simulated. No-op when no such fix is registered.
				if r.transformer != nil {
					renamed := r.transformer.SanitizeTargetName(sourceDB, sourceCollection, targetCollection)
					if renamed != targetCollection {
						recordCollectionRename(r.log, sourceDB, sourceCollection, renamed)
					}
					targetCollection = renamed
				}
				r.log.Infof("Starting initial migration for %s.%s to %s (UpsertMode: %t)",
					sourceDB, sourceCollection, targetCollection, collConfig.UpsertMode)

				sourceDBCollection := r.sourceDB.GetCollection(sourceCollection)
				targetDBCollection := r.targetDB.GetCollection(targetCollection)

				count, err := sourceDBCollection.EstimatedDocumentCount(ctx)
				if err != nil {
					r.log.Errorf("Error counting documents in %s.%s: %v", sourceDB, sourceCollection, err)
					return
				}

				r.log.Infof("Found %d documents to migrate in %s.%s", count, sourceDB, sourceCollection)

				if count == 0 {
					r.log.Infof("No documents to migrate for %s.%s", sourceDB, sourceCollection)
					return
				}

				successCount, failedCount := r.migrateCollection(ctx, sourceDBCollection, targetDBCollection, count, sourceDB, sourceCollection)

				mu.Lock()
				totalMigratedCount += successCount
				totalFailedCount += failedCount
				completedCollections++
				r.log.Infof("Overall progress: %d/%d collections completed", completedCollections, totalCollections)
				mu.Unlock()
			}(sourceDB, sourceCollection, collConfig)
		}
	}

	wg.Wait()

	initialMigrationDuration := time.Since(initialMigrationStart)
	totalAttempted := totalMigratedCount + totalFailedCount
	var failurePercentage float64
	if totalAttempted > 0 {
		failurePercentage = (float64(totalFailedCount) * 100.0) / float64(totalAttempted)
	}
	r.log.Infof("Initial migration completed in %.2f seconds. Total collections: %d, Total documents: %d (Success: %d, Failed: %d, Failure Rate: %.2f%%)",
		initialMigrationDuration.Seconds(), totalCollections, totalAttempted, totalMigratedCount, totalFailedCount, failurePercentage)

	return totalMigratedCount, totalFailedCount, nil
}

// migrateCollection migrates a single collection from modern source to target
func (r *InitialMigrator) migrateCollection(ctx context.Context, sourceCol, targetCol *mongo.Collection, count int64, sourceDB, sourceCollection string) (int64, int64) {
	if r.DryRun {
		r.log.Infof("[Dry Run] Skipping actual data migration for collection %s.%s", sourceDB, sourceCollection)
		// Perform sampling analysis to recommend ID partitioning strategy
		partitioner := NewCollectionPartitioner(
			sourceCol,
			r.log,
			r.config.MaxReadPartitions,
			r.config.MinDocsPerPartition,
			r.config.SampleSize,
			r.config.IDTypeForPartition,
		)
		recType, err := partitioner.RecommendIDPartitioning(ctx)
		if err != nil {
			r.log.Warnf("[Dry Run] Failed to recommend ID partitioning for collection %s.%s: %v", sourceDB, sourceCollection, err)
		} else {
			r.log.Infof("[Dry Run] Recommended ID partitioning strategy for collection %s.%s: %s", sourceDB, sourceCollection, recType)
		}
		return 0, 0
	}

	readBatchSize := r.config.InitialReadBatchSize
	writeBatchSize := r.config.InitialWriteBatchSize

	const maxCursorResumes = 10

	var batch []interface{}
	var successCount int64
	var failedCount int64
	var lastLoggedPercentage int = -1
	var lastID interface{}
	var cursorResumeCount int

	for {
		var filter bson.D
		if lastID == nil {
			filter = bson.D{}
		} else {
			r.log.Infof("[%s.%s] Resuming cursor from _id=%v (resume attempt %d/%d)",
				sourceDB, sourceCollection, lastID, cursorResumeCount, maxCursorResumes)
			filter = bson.D{{Key: "_id", Value: bson.D{{Key: "$gt", Value: lastID}}}}
		}

		// [Firestore compat] noCursorTimeout is rejected by Firestore's MongoDB-compatible endpoint
		// ("Unsupported fields in find request: [noCursorTimeout]"); omit it. Cursor resumption is
		// handled explicitly via the _id $gt filter above on cursor loss.
		findOpts := options.Find().
			SetBatchSize(int32(readBatchSize)).
			SetSort(bson.D{{Key: "_id", Value: 1}})

		cursor, err := sourceCol.Find(ctx, filter, findOpts)
		if err != nil {
			r.log.Errorf("Error creating cursor for %s.%s: %v", sourceDB, sourceCollection, err)
			return successCount, failedCount
		}

		cursorFailed := false

		for cursor.Next(ctx) {
			var doc bson.D
			if err := cursor.Decode(&doc); err != nil {
				r.log.Errorf("[%s.%s] Error decoding document: %v", sourceDB, sourceCollection, err)
				continue
			}

			lastID = extractDocID(doc)
			batch = append(batch, doc)

			if len(batch) >= writeBatchSize {
				batchSize := int64(len(batch))
				succeeded := r.insertBatchWithRetry(ctx, targetCol, batch, sourceDB, sourceCollection)
				successCount += succeeded
				failedCount += batchSize - succeeded
				batch = nil

				if count > 0 {
					currentCount := successCount + failedCount
					currentPercentage := int(float64(currentCount) / float64(count) * 10)
					if currentPercentage > lastLoggedPercentage {
						lastLoggedPercentage = currentPercentage
						r.log.Infof("Collection %s.%s progress: %d/%d documents (%.0f%%) - Successful: %d, Failed: %d",
							sourceDB, sourceCollection, currentCount, count, float64(currentPercentage)*10, successCount, failedCount)
					}
				}
			}
		}

		if err := cursor.Err(); err != nil {
			currentCount := successCount + failedCount
			r.log.Warnf("[%s.%s] Cursor error after %d documents: %v", sourceDB, sourceCollection, currentCount, err)
			cursor.Close(ctx)

			if ctx.Err() != nil {
				r.log.Infof("[%s.%s] Context canceled, stopping migration", sourceDB, sourceCollection)
				break
			}

			cursorResumeCount++
			if lastID != nil && cursorResumeCount <= maxCursorResumes {
				r.log.Infof("[%s.%s] Will attempt cursor resumption from last _id=%v", sourceDB, sourceCollection, lastID)

				if len(batch) > 0 {
					batchSize := int64(len(batch))
					succeeded := r.insertBatchWithRetry(ctx, targetCol, batch, sourceDB, sourceCollection)
					successCount += succeeded
					failedCount += batchSize - succeeded
					batch = nil
				}

				cursorFailed = true
			} else {
				currentCount = successCount + failedCount
				if cursorResumeCount > maxCursorResumes {
					r.log.Errorf("[%s.%s] Exceeded maximum cursor resume attempts (%d). Stopping migration at %d documents.",
						sourceDB, sourceCollection, maxCursorResumes, currentCount)
				} else {
					r.log.Errorf("[%s.%s] Cursor error with no last _id to resume from. Stopping migration at %d documents.",
						sourceDB, sourceCollection, currentCount)
				}
				break
			}
		} else {
			cursor.Close(ctx)
		}

		if !cursorFailed {
			break
		}
	}

	if len(batch) > 0 {
		batchSize := int64(len(batch))
		succeeded := r.insertBatchWithRetry(ctx, targetCol, batch, sourceDB, sourceCollection)
		successCount += succeeded
		failedCount += batchSize - succeeded
	}

	totalCount := successCount + failedCount
	if failedCount > 0 {
		r.log.Warnf("Migration for %s.%s completed with %d failures! Successful: %d, Failed: %d, Total: %d",
			sourceDB, sourceCollection, failedCount, successCount, failedCount, totalCount)
	} else {
		r.log.Infof("Migration for %s.%s completed successfully! Total documents: %d",
			sourceDB, sourceCollection, totalCount)
	}
	return successCount, failedCount
}

// insertBatchWithRetry handles batch insertion / upserting with retries and DLQ fallback
func (r *InitialMigrator) insertBatchWithRetry(ctx context.Context, targetCol *mongo.Collection, batch []interface{}, sourceDB, sourceCollection string) int64 {
	transformedBatch, err := r.transformer.TransformBatch(batch, sourceDB, sourceCollection)
	if err != nil {
		r.log.Errorf("Field name transformation failed for batch in %s.%s: %v", sourceDB, sourceCollection, err)
		for _, doc := range batch {
			docID := extractDocID(doc)
			if r.dlq != nil {
				r.dlq.WriteFailed(sourceDB, sourceCollection, docID, err, "initial", "insert", doc, time.Time{})
			}
		}
		return 0
	}
	batch = transformedBatch

	var successCount int64

	collConfig, exists := r.collectionConfigs[sourceDB][sourceCollection]
	useUpsert := exists && collConfig.UpsertMode

	if useUpsert {
		models := BuildUpsertReplaceModels(batch)

		if len(models) > 0 {
			if _, err := targetCol.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false)); err != nil {
				bulkWriteException, ok := err.(mongo.BulkWriteException)
				if ok {
					successCount = int64(len(batch) - len(bulkWriteException.WriteErrors))
					r.log.Errorf("Bulk upsert partially failed for %s.%s: %d succeeded, %d failed",
						sourceDB, sourceCollection, successCount, len(bulkWriteException.WriteErrors))

					for _, writeErr := range bulkWriteException.WriteErrors {
						var errDocID interface{}
						if writeErr.Index < len(batch) {
							errDocID = extractDocID(batch[writeErr.Index])
						}
						r.log.Errorf("[%s.%s] Upsert error at index %d, _id=%v: %v", sourceDB, sourceCollection, writeErr.Index, errDocID, writeErr.Message)
						if r.dlq != nil && writeErr.Index < len(batch) {
							r.dlq.WriteFailed(sourceDB, sourceCollection, errDocID, fmt.Errorf("upsert failed: %s", writeErr.Message), "initial", "insert", batch[writeErr.Index], time.Time{})
						}
					}
				} else {
					if err == context.Canceled {
						r.log.Debugf("Bulk upsert canceled for %s.%s due to context cancellation", sourceDB, sourceCollection)
					} else {
						r.log.Errorf("Error performing bulk upsert for %s.%s: %v", sourceDB, sourceCollection, err)
					}
					for _, doc := range batch {
						docID := extractDocID(doc)
						if docID != nil {
							filter := bson.M{"_id": docID}
							if _, err := targetCol.ReplaceOne(ctx, filter, doc, options.Replace().SetUpsert(true)); err != nil {
								if err != context.Canceled {
									r.log.Errorf("Error fallback upserting document %v in %s.%s: %v", docID, sourceDB, sourceCollection, err)
									if r.dlq != nil {
										r.dlq.WriteFailed(sourceDB, sourceCollection, docID, err, "initial", "insert", doc, time.Time{})
									}
								}
							} else {
								successCount++
							}
						}
					}
				}
			} else {
				successCount = int64(len(batch))
				r.log.Debugf("Bulk upserted %d documents successfully in %s.%s", len(batch), sourceDB, sourceCollection)
			}
		}
		return successCount
	}

	if _, err := targetCol.InsertMany(ctx, batch, options.InsertMany().SetOrdered(false)); err != nil {
		bulkWriteException, ok := err.(mongo.BulkWriteException)
		if ok {
			successCount = int64(len(batch) - len(bulkWriteException.WriteErrors))
			if len(bulkWriteException.WriteErrors) > 0 {
				r.log.Debugf("Bulk insert partially failed for %s.%s: %d succeeded, %d failed",
					sourceDB, sourceCollection, successCount, len(bulkWriteException.WriteErrors))
			}

			for _, writeErr := range bulkWriteException.WriteErrors {
				if writeErr.Code == 11000 {
					if writeErr.Index < len(batch) {
						doc := batch[writeErr.Index]
						docID := extractDocID(doc)
						if docID != nil {
							filter := bson.M{"_id": docID}
							if _, err := targetCol.ReplaceOne(ctx, filter, doc, options.Replace().SetUpsert(true)); err != nil {
								r.log.Debugf("Upsert fallback failed for document %v in %s.%s: %v",
									docID, sourceDB, sourceCollection, err)
								if r.dlq != nil {
									r.dlq.WriteFailed(sourceDB, sourceCollection, docID, err, "initial", "insert", doc, time.Time{})
								}
							} else {
								r.log.Debugf("Successfully upserted document %v in %s.%s after duplicate key error",
									docID, sourceDB, sourceCollection)
								successCount++
							}
						}
					}
				} else {
					r.log.Debugf("Insert error at index %d in %s.%s: %v",
						writeErr.Index, sourceDB, sourceCollection, writeErr.Message)

					if writeErr.Index < len(batch) {
						retryDocID := extractDocID(batch[writeErr.Index])
						if _, err := targetCol.InsertOne(ctx, batch[writeErr.Index]); err != nil {
							r.log.Errorf("[%s.%s] Retry insert failed for document _id=%v: %v",
								sourceDB, sourceCollection, retryDocID, err)
							if r.dlq != nil {
								r.dlq.WriteFailed(sourceDB, sourceCollection, retryDocID, err, "initial", "insert", batch[writeErr.Index], time.Time{})
							}
						} else {
							successCount++
						}
					}
				}
			}
		} else {
			if err == context.Canceled {
				r.log.Debugf("Bulk insert canceled for %s.%s due to context cancellation", sourceDB, sourceCollection)
			} else {
				r.log.Errorf("Error performing bulk insert for %s.%s: %v", sourceDB, sourceCollection, err)
			}

			bulkRetrySucceeded := false
			if r.retryManager != nil && err != context.Canceled {
				errType := r.retryManager.ClassifyError(err)
				if errType == ErrorTypeConnection || errType == ErrorTypeContention {
					r.log.Infof("Transient error detected for %s.%s. Retrying bulk insert with backoff...", sourceDB, sourceCollection)
					retryErr := r.retryManager.RetryWithBackoff(ctx, func() error {
						_, retryInsertErr := targetCol.InsertMany(ctx, batch, options.InsertMany().SetOrdered(false))
						return retryInsertErr
					})
					if retryErr == nil {
						r.log.Infof("Bulk insert for %s.%s succeeded after retry", sourceDB, sourceCollection)
						bulkRetrySucceeded = true
						successCount = int64(len(batch))
					} else {
						r.log.Warnf("Bulk insert for %s.%s still failed after retries: %v. Falling back to individual operations.", sourceDB, sourceCollection, retryErr)
					}
				}
			}

			if !bulkRetrySucceeded {
				for _, doc := range batch {
					if _, err := targetCol.InsertOne(ctx, doc); err != nil {
						docID := extractDocID(doc)
						if docID != nil {
							filter := bson.M{"_id": docID}
							if _, err := targetCol.ReplaceOne(ctx, filter, doc, options.Replace().SetUpsert(true)); err != nil {
								if err == context.Canceled {
									r.log.Debugf("Upserting document %v in %s.%s canceled due to context cancellation",
										docID, sourceDB, sourceCollection)
								} else {
									r.log.Errorf("Error upserting document %v in %s.%s: %v",
										docID, sourceDB, sourceCollection, err)
									if r.dlq != nil {
										r.dlq.WriteFailed(sourceDB, sourceCollection, docID, err, "initial", "insert", doc, time.Time{})
									}
								}
							} else {
								r.log.Debugf("Successfully upserted document %v in %s.%s after insert failed",
									docID, sourceDB, sourceCollection)
								successCount++
							}
						}
					} else {
						successCount++
					}
				}
			}
		}
	} else {
		successCount = int64(len(batch))
		r.log.Debugf("Bulk inserted %d documents successfully in %s.%s", len(batch), sourceDB, sourceCollection)
	}

	return successCount
}

// BuildUpsertReplaceModels converts a slice of BSON documents into a slice of mongo.WriteModel replacements with upsert=true
func BuildUpsertReplaceModels(batch []interface{}) []mongo.WriteModel {
	var models []mongo.WriteModel
	for _, doc := range batch {
		docID := extractDocID(doc)
		if docID != nil {
			filter := bson.M{"_id": docID}
			model := mongo.NewReplaceOneModel().
				SetFilter(filter).
				SetReplacement(doc).
				SetUpsert(true)
			models = append(models, model)
		}
	}
	return models
}
