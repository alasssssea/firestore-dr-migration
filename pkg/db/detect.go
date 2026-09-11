package db

import (
	"context"
	"time"

	"firestore-dr-migration/pkg/util"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// SourceServerInfo describes a detected source MongoDB server.
type SourceServerInfo struct {
	Version      string // raw version string, e.g. "3.2.22"
	VersionArray []int  // [major, minor, patch]
	IsReplicaSet bool   // true if the server reports a replica set name
	SetName      string // replica set name, if any
	ModernDriver bool   // true if reachable by the modern official driver
}

// DetectSourceServer connects to the source MongoDB using the modern official
// driver and reports its version and topology. Callers use the result to
// auto-select a replication method.
func DetectSourceServer(connectionString, database string) (*SourceServerInfo, error) {
	return detectModern(connectionString)
}

// detectModern uses the official go driver to read buildInfo and topology.
func detectModern(uri string) (*SourceServerInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}
	defer client.Disconnect(context.Background())

	admin := client.Database("admin")

	var buildInfo bson.M
	if err := admin.RunCommand(ctx, bson.D{{Key: "buildInfo", Value: 1}}).Decode(&buildInfo); err != nil {
		return nil, err
	}
	version, _ := buildInfo["version"].(string)
	verArr, err := util.ParseServerVersion(version)
	if err != nil {
		return nil, err
	}

	info := &SourceServerInfo{
		Version:      version,
		VersionArray: verArr,
		ModernDriver: true,
	}

	// "hello" (4.4+) with fallback to "isMaster" for older servers.
	var hello bson.M
	if err := admin.RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello); err != nil {
		_ = admin.RunCommand(ctx, bson.D{{Key: "isMaster", Value: 1}}).Decode(&hello)
	}
	if setName, ok := hello["setName"].(string); ok && setName != "" {
		info.IsReplicaSet = true
		info.SetName = setName
	}
	return info, nil
}
