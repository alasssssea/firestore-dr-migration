package migration

import (
	"context"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"firestore-dr-migration/pkg/config"
	"firestore-dr-migration/pkg/db"
	"firestore-dr-migration/pkg/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// CompatibilityTestCase represents a test case for target validation
type CompatibilityTestCase struct {
	Category string
	TestName string
	Document bson.D
	ValueDesc string
}

// RunCompatibilityTest runs compatibility tests for various _id types and long field names on all target databases
func RunCompatibilityTest(ctx context.Context, cfg *config.Config, dryRun bool, log *logger.Logger) error {
	log.Info("================================================================================")
	log.Info("                STARTING TARGET SYSTEM COMPATIBILITY TEST")
	log.Info("================================================================================")

	for pairIdx, pair := range cfg.DatabasePairs {
		log.Infof("[Pair %d] Connecting to target database: %s", pairIdx+1, pair.Target.Database)
		
		// Establish connection to target
		targetDB, err := db.NewMongoDB(pair.Target.ConnectionString, pair.Target.Database, uint64(cfg.TargetMinPoolSize), uint64(cfg.TargetMaxPoolSize), 0, nil, log)
		if err != nil {
			return fmt.Errorf("[Pair %d] failed to connect to target database: %w", pairIdx+1, err)
		}
		defer targetDB.Close(ctx)

		client := targetDB.GetClient()
		database := client.Database(pair.Target.Database)
		
		longFieldName := strings.Repeat("a", 1001)

		// Define compatibility test cases (IDs and Document Schema Limits)
		testCases := []CompatibilityTestCase{
			// 1. ID Datatype Tests
			{Category: "ID Type Support", TestName: "ObjectID as _id", Document: bson.D{{Key: "_id", Value: primitive.NewObjectID()}}, ValueDesc: "standard BSON ObjectID"},
			{Category: "ID Type Support", TestName: "String as _id", Document: bson.D{{Key: "_id", Value: "test_string_id"}}, ValueDesc: "\"test_string_id\""},
			{Category: "ID Type Support", TestName: "Int64 (Long) as _id", Document: bson.D{{Key: "_id", Value: int64(1234567890)}}, ValueDesc: "1234567890 (int64)"},
			{Category: "ID Type Support", TestName: "Int32 as _id", Document: bson.D{{Key: "_id", Value: int32(12345)}}, ValueDesc: "12345 (int32)"},
			{Category: "ID Type Support", TestName: "Int as _id", Document: bson.D{{Key: "_id", Value: int(9876)}}, ValueDesc: "9876 (int)"},
			{Category: "ID Type Support", TestName: "Float64 (Double) as _id", Document: bson.D{{Key: "_id", Value: float64(3.14159)}}, ValueDesc: "3.14159 (double)"},
			{Category: "ID Type Support", TestName: "Boolean as _id", Document: bson.D{{Key: "_id", Value: true}}, ValueDesc: "true (boolean)"},
			{Category: "ID Type Support", TestName: "DateTime as _id", Document: bson.D{{Key: "_id", Value: primitive.NewDateTimeFromTime(time.Now())}}, ValueDesc: "current datetime"},
			{Category: "ID Type Support", TestName: "Binary (Bytes) as _id", Document: bson.D{{Key: "_id", Value: primitive.Binary{Subtype: 0, Data: []byte{1, 2, 3, 4}}}}, ValueDesc: "[1, 2, 3, 4] bytes"},
			{Category: "ID Type Support", TestName: "Array as _id", Document: bson.D{{Key: "_id", Value: bson.A{1, 2, 3}}}, ValueDesc: "[1, 2, 3] (array)"},
			{Category: "ID Type Support", TestName: "Empty String as _id", Document: bson.D{{Key: "_id", Value: ""}}, ValueDesc: "\"\""},
			{Category: "ID Type Support", TestName: "Empty Array as _id", Document: bson.D{{Key: "_id", Value: bson.A{}}}, ValueDesc: "[]"},
			{Category: "ID Type Support", TestName: "Empty Subdocument as _id", Document: bson.D{{Key: "_id", Value: bson.D{}}}, ValueDesc: "{}"},
			{Category: "ID Type Support", TestName: "Null as _id", Document: bson.D{{Key: "_id", Value: nil}}, ValueDesc: "null"},

			// 2. Key Length & Name Validation
			{Category: "Document Key Support", TestName: "Long Nested Field Name", Document: bson.D{{Key: "_id", Value: "test_long_field_key"}, {Key: "nested", Value: bson.D{{Key: longFieldName, Value: "value"}}}}, ValueDesc: "field name length = 1001 chars"},
			{Category: "Document Key Support", TestName: "Empty Field Name", Document: bson.D{{Key: "_id", Value: "test_empty_field_key"}, {Key: "", Value: "empty-value"}}, ValueDesc: "field name length = 0 chars"},
		}

		if dryRun {
			log.Infof("[Dry Run] [Pair %d] Connection to target database succeeded. Skipping writes.", pairIdx+1)
			fmt.Printf("\n--- TARGET DATABASE COMPATIBILITY REPORT - DRY RUN (Pair %d: %s) ---\n\n", pairIdx+1, pair.Target.Database)
			w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
			fmt.Fprintln(w, "CATEGORY\tFEATURE TO TEST\tVALUE DESCRIPTION")
			fmt.Fprintln(w, "--------\t---------------\t-----------------")
			for _, tc := range testCases {
				fmt.Fprintf(w, "%s\t%s\t%s\n", tc.Category, tc.TestName, tc.ValueDesc)
			}
			w.Flush()
			fmt.Println()

			printSchemaTransformationConfigReference()
			continue
		}

		// Create a temporary collection name
		tempCollName := fmt.Sprintf("_compatibility_test_%d", time.Now().UnixNano())
		coll := database.Collection(tempCollName)
		defer func() {
			// Clean up the temporary collection
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := coll.Drop(cleanupCtx); err != nil {
				log.Errorf("Failed to drop temporary collection %s: %v", tempCollName, err)
			}
		}()

		type resultRow struct {
			Category  string
			TestName  string
			Supported bool
			ErrorMsg  string
		}
		var results []resultRow
		hasUnsupportedIDs := false
		hasUnsupportedLongKeys := false
		hasUnsupportedEmptyKeys := false

		log.Infof("[Pair %d] Executing writes for %d compatibility test cases on collection '%s'...", pairIdx+1, len(testCases), tempCollName)
		for _, tc := range testCases {
			// Try inserting
			_, insertErr := coll.InsertOne(ctx, tc.Document)
			if insertErr != nil {
				results = append(results, resultRow{
					Category:  tc.Category,
					TestName:  tc.TestName,
					Supported: false,
					ErrorMsg:  insertErr.Error(),
				})
				if tc.Category == "ID Type Support" {
					hasUnsupportedIDs = true
				} else if tc.Category == "Document Key Support" {
					if tc.TestName == "Long Nested Field Name" {
						hasUnsupportedLongKeys = true
					} else if tc.TestName == "Empty Field Name" {
						hasUnsupportedEmptyKeys = true
					}
				}
			} else {
				results = append(results, resultRow{
					Category:  tc.Category,
					TestName:  tc.TestName,
					Supported: true,
					ErrorMsg:  "N/A",
				})
				// Clean up the document if insert succeeded
				var docID interface{}
				for _, elem := range tc.Document {
					if elem.Key == "_id" {
						docID = elem.Value
						break
					}
				}
				_, _ = coll.DeleteOne(ctx, bson.M{"_id": docID})
			}
		}

		// Print compatibility report in tabular form
		fmt.Printf("\n--- TARGET DATABASE COMPATIBILITY REPORT (Pair %d: %s) ---\n\n", pairIdx+1, pair.Target.Database)
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "CATEGORY\tFEATURE TESTED\tSUPPORTED?\tERROR / NOTES")
		fmt.Fprintln(w, "--------\t--------------\t----------\t-------------")
		for _, res := range results {
			statusSymbol := "YES"
			if !res.Supported {
				statusSymbol = "NO"
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", res.Category, res.TestName, statusSymbol, res.ErrorMsg)
		}
		w.Flush()
		fmt.Println()

		// Construct recommendation notices
		if hasUnsupportedIDs || hasUnsupportedLongKeys || hasUnsupportedEmptyKeys {
			fmt.Printf("! Target Compatibility Alerts:\n\n")
			if hasUnsupportedIDs {
				fmt.Printf("1. Unsupported _id Types detected:\n")
				fmt.Printf("   - Action: Enable `convertInvalidIds: true` under `retryConfig` to auto-serialize invalid types to strings.\n")
				fmt.Printf("   - Transformation Example:\n")
				fmt.Printf("     Source: _id: [1, 2] (Array)\n")
				fmt.Printf("     Target: _id: \"_converted:array:[1,2]\" (String)\n\n")
			}
			if hasUnsupportedLongKeys {
				fmt.Printf("2. Unsupported Long Nested Keys detected:\n")
				fmt.Printf("   - Action: Enable `convertLongFieldNamesInNestedDocs: true` in your main configuration to automatically stringify nested structures containing long field names (>1000 characters).\n")
				fmt.Printf("   - Transformation Example:\n")
				fmt.Printf("     Source: { \"nested\": { \"<key_1001_chars>\": \"value\" } }\n")
				fmt.Printf("     Target: { \"nested\": \"{\\\"<key_1001_chars>\\\":\\\"value\\\"}\" }\n\n")
			}
			if hasUnsupportedEmptyKeys {
				fmt.Printf("3. Unsupported Empty Field Names detected:\n")
				fmt.Printf("   - Action: Enable `dropEmptyFieldNames: true` in your main configuration to automatically drop fields with empty names (\"\").\n")
				fmt.Printf("   - Transformation Example:\n")
				fmt.Printf("     Source: { \"_id\": \"test\", \"\": \"empty-val\", \"name\": \"user\" }\n")
				fmt.Printf("     Target: { \"_id\": \"test\", \"name\": \"user\" }\n\n")
			}
		} else {
			fmt.Printf("* Success: Target database fully supports all tested ID datatypes, long field names, and empty field names!\n\n")
		}

		printSchemaTransformationConfigReference()
	}

	log.Info("================================================================================")
	log.Info("                TARGET SYSTEM COMPATIBILITY TEST COMPLETE")
	log.Info("================================================================================")
	return nil
}

func printSchemaTransformationConfigReference() {
	fmt.Printf("--- SCHEMA TRANSFORMATION CONFIGURATION REFERENCE ---\n\n")
	fmt.Printf("Below is a reference of available configuration flags and how they transform documents during migration:\n\n")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "CONFIGURATION FLAG\tACTION DESCRIPTION\tSOURCE DOCUMENT (BEFORE)\tTRANSFORMED DOCUMENT (AFTER)")
	fmt.Fprintln(w, "------------------\t------------------\t------------------------\t----------------------------")
	fmt.Fprintln(w, "convertInvalidIds: true\tConverts unsupported target _id datatypes to deterministic strings.\t_id: [1, 2] (Array)\t_id: \"_converted:array:[1,2]\" (String)")
	fmt.Fprintln(w, "convertLongFieldNamesInNestedDocs: true\tStringifies nested subdocuments containing keys exceeding 1000 characters.\t{ \"nested\": { \"<long_key>\": \"value\" } }\t{ \"nested\": \"{\\\"<long_key>\\\":\\\"value\\\"}\" }")
	fmt.Fprintln(w, "dropEmptyFieldNames: true\tRemoves fields with empty key names (\"\") from the document payload.\t{ \"\": \"empty-val\", \"name\": \"user\" }\t{ \"name\": \"user\" }")
	w.Flush()
	fmt.Println()
}
