package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	uri := flag.String("uri", "", "connection string")
	db := flag.String("db", "", "database")
	coll := flag.String("coll", "big_events", "collection")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	cl, err := mongo.Connect(ctx, options.Client().ApplyURI(*uri))
	if err != nil {
		panic(err)
	}
	defer cl.Disconnect(ctx)
	c := cl.Database(*db).Collection(*coll)
	name, err := c.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "seq", Value: 1}}})
	if err != nil {
		panic(err)
	}
	fmt.Println("created index:", name)
	cur, _ := c.Indexes().List(ctx)
	var idx []bson.M
	cur.All(ctx, &idx)
	fmt.Printf("indexes now: %v\n", idx)
}
