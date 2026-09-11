package db

import (
	"context"
	"sort"
	"time"

	"firestore-dr-migration/pkg/logger"
	"go.mongodb.org/mongo-driver/bson"
)

// inventorySystemDBs are databases never offered for migration by the console.
var inventorySystemDBs = map[string]bool{"admin": true, "local": true, "config": true}

// ListInventory returns the user databases and their collections for a source
// server using the modern official driver. This lets the web console present a
// pick-and-click list of collections. The modern parameter is retained for
// signature stability and is ignored.
func ListInventory(connectionString string, modern bool) (map[string][]string, error) {
	return listInventoryModern(connectionString)
}

func listInventoryModern(connectionString string) (map[string][]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Read-only inventory listing: a tiny pool, no pool monitor, short idle.
	m, err := NewMongoDB(connectionString, "admin", 0, 4, 30*time.Second, nil, logger.New())
	if err != nil {
		return nil, err
	}
	defer m.Close(ctx)

	client := m.GetClient()
	dbNames, err := client.ListDatabaseNames(ctx, bson.D{})
	if err != nil {
		return nil, err
	}

	inv := make(map[string][]string)
	for _, dbn := range dbNames {
		if inventorySystemDBs[dbn] {
			continue
		}
		colls, err := client.Database(dbn).ListCollectionNames(ctx, bson.D{})
		if err != nil {
			return nil, err
		}
		inv[dbn] = filterAndSort(colls)
	}
	return inv, nil
}

// filterAndSort drops the legacy system.indexes/system.* catalog collections
// and returns the remainder sorted for stable UI rendering.
func filterAndSort(colls []string) []string {
	out := make([]string, 0, len(colls))
	for _, c := range colls {
		if len(c) >= 7 && c[:7] == "system." {
			continue
		}
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}
