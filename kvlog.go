package kvlog

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const defaultDBName = "kvlog"

// First-pass implementation: mongodb
type KDB struct {
	Ctx    context.Context
	Uri    string
	Client *mongo.Client
	DB     *mongo.Database
	KC     *mongo.Collection // kvlog collection
	VC     *mongo.Collection // value collection
}

type KVLog struct {
	K   string `bson:"k"`
	TS  int64  `bson:"ts"`
	VID string `bson:"vid"`
}

type Value struct {
	ID string `bson:"_id"`
	V  string `bson:"v"`
}

func newKDBNamed(ctx context.Context, uri, dbname string) (*KDB, error) {
	client, err := mongo.NewClient(options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}

	err = client.Connect(ctx)
	if err != nil {
		return nil, err
	}

	db := client.Database(dbname)
	kc := db.Collection("kvlog")
	vc := db.Collection("value")

	kdb := KDB{Ctx: ctx, Uri: uri, Client: client, DB: db, KC: kc, VC: vc}
	return &kdb, nil
}

// NewKDB creates a new connection to the kvlog database at uri,
// using the ctx context, and returns *DB. The caller is responsible
// for doing a Disconnect on *DB.client (with ctx) when completed.
func NewKDB(ctx context.Context, uri string) (*KDB, error) {
	return newKDBNamed(ctx, uri, defaultDBName)
}

// findLatestKVLog finds the latest kvlog entry for key
func (kdb *KDB) findLatestKVLog(key string) (*KVLog, error) {
	kvlog := KVLog{}
	filter := bson.D{
		primitive.E{Key: "k", Value: key},
	}
	options := options.FindOne()
	options.SetSort(bson.M{"ts": -1})
	err := kdb.KC.FindOne(kdb.Ctx, filter, options).Decode(&kvlog)
	if err != nil {
		return nil, err
	}
	return &kvlog, nil
}

// findKVLogAfter finds the first kvlog entry for key after ts
func (kdb *KDB) findKVLogAfter(key string, ts int64) (*KVLog, error) {
	kvlog := KVLog{}
	filter := bson.D{
		primitive.E{Key: "k", Value: key},
		primitive.E{Key: "ts", Value: bson.D{
			primitive.E{Key: "$gte", Value: ts},
		}},
	}
	options := options.FindOne()
	options.SetSort(bson.M{"ts": 1})
	err := kdb.KC.FindOne(kdb.Ctx, filter, options).Decode(&kvlog)
	if err != nil {
		return nil, err
	}
	return &kvlog, nil
}

// findValue finds value where _id == kvlog.VID
func (kdb *KDB) findValue(vid string) (string, error) {
	value := Value{}
	filter := bson.D{primitive.E{Key: "_id", Value: vid}}
	err := kdb.VC.FindOne(kdb.Ctx, filter).Decode(&value)
	if err != nil {
		return "", err
	}
	return value.V, nil
}

// Set sets the current value for key to val (if not already val)
func (kdb *KDB) Set(key, val string) error {
	val = strings.TrimSpace(val)
	hash := sha1.Sum([]byte(val))
	vid := hex.EncodeToString(hash[:])

	// find or insert value record
	filter := bson.D{primitive.E{Key: "_id", Value: vid}}
	res := kdb.VC.FindOne(kdb.Ctx, filter)
	if res.Err() != nil {
		if res.Err() != mongo.ErrNoDocuments {
			return res.Err()
		}
		// vid not found - insert
		//fmt.Printf("value for %q not found - inserting\n", val)
		vrec := Value{ID: vid, V: val}
		_, err := kdb.VC.InsertOne(kdb.Ctx, vrec)
		if err != nil {
			return err
		}
	} else {
		//fmt.Printf("value for %q found\n", val)
	}

	// find or insert kvlog record
	kvlog, err := kdb.findLatestKVLog(key)
	if err != nil && err != mongo.ErrNoDocuments {
		return err
	}
	if err != mongo.ErrNoDocuments && kvlog.VID == vid {
		// latest kvlog record matches, we're done
		//fmt.Printf("latest kvlog for %q found and vid matches\n", key)
		return nil
	}

	// No kvlog record found, or VIDs don't match - do an insert
	//fmt.Printf("kvlog for %q not found or out of date - inserting\n", key)
	kvlog = &KVLog{K: key, TS: time.Now().UnixNano(), VID: vid}
	_, err = kdb.KC.InsertOne(kdb.Ctx, *kvlog)
	if err != nil {
		return err
	}

	return nil
}

// Get fetches the latest value for key
func (kdb *KDB) Get(key string) (string, error) {
	kvlog, err := kdb.findLatestKVLog(key)
	if err != nil {
		return "", err
	}
	return kdb.findValue(kvlog.VID)
}

// Get fetches the first value for key after ts
func (kdb *KDB) GetAt(key string, ts int64) (string, error) {
	kvlog, err := kdb.findKVLogAfter(key, ts)
	if err != nil {
		return "", err
	}
	return kdb.findValue(kvlog.VID)
}
