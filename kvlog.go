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

const (
	defaultURI           = "mongodb://localhost/"
	defaultDBName        = "kvlog"
	maxInlineValueLength = 200 // max number of characters in value to store inline in kvlog.v
)

// First-pass implementation: mongodb
type KDBOptions struct {
	URI    string
	DBName string
}

type KDB struct {
	ctx    context.Context
	client *mongo.Client
	db     *mongo.Database
	kc     *mongo.Collection // kvlog collection
	vc     *mongo.Collection // value collection
}

type KVLog struct {
	Key string `bson:"k"`
	TS  int64  `bson:"ts"`
	Val string `bson:"v"`
	vid string `bson:"vid"`
}

type Value struct {
	ID  string `bson:"_id"`
	Val string `bson:"v"`
}

type Iterator struct {
	kdb    *KDB
	cursor *mongo.Cursor
	err    error
}

func createIndexes(ctx context.Context, coll *mongo.Collection) error {
	// db.kvlog.createIndex({ k:1, ts:-1 }, { unique:true })
	model := mongo.IndexModel{
		Keys:    bson.D{{Key: "k", Value: 1}, {Key: "ts", Value: -1}},
		Options: options.Index().SetName("k_ts").SetUnique(true),
	}
	_, err := coll.Indexes().CreateOne(ctx, model, nil)
	if err != nil {
		return err
	}
	return nil
}

// NewKDBOptions creates a new connection to the kvlog database using
// the ctx context and the given options, and returns *KDB. The caller is
// responsible for calling KDB.Disconnect when completed.
func NewKDBOptions(ctx context.Context, opts KDBOptions) (*KDB, error) {
	if opts.URI == "" {
		opts.URI = defaultURI
	}
	if opts.DBName == "" {
		opts.DBName = defaultDBName
	}

	client, err := mongo.NewClient(options.Client().ApplyURI(opts.URI))
	if err != nil {
		return nil, err
	}

	err = client.Connect(ctx)
	if err != nil {
		return nil, err
	}

	db := client.Database(opts.DBName)
	kc := db.Collection("kvlog")
	vc := db.Collection("value")

	// Check required indexes exist
	err = createIndexes(ctx, kc)
	if err != nil {
		return nil, err
	}

	kdb := KDB{ctx: ctx, client: client, db: db, kc: kc, vc: vc}
	return &kdb, nil
}

// NewKDB creates a new connection to the kvlog database using the ctx
// context and default options, and returns *KDB. The caller is
// responsible for doing a Disconnect(ctx) on *KDB.client when completed.
func NewKDB(ctx context.Context) (*KDB, error) {
	return NewKDBOptions(ctx, KDBOptions{})
}

// findKVLogLatest finds the latest kvlog entry for key
func (kdb *KDB) findKVLogLatest(key string) (*KVLog, error) {
	kvlog := KVLog{}
	filter := bson.D{
		primitive.E{Key: "k", Value: key},
	}
	options := options.FindOne()
	options.SetSort(bson.M{"ts": -1})
	err := kdb.kc.FindOne(kdb.ctx, filter, options).Decode(&kvlog)
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
	err := kdb.kc.FindOne(kdb.ctx, filter, options).Decode(&kvlog)
	if err != nil {
		return nil, err
	}
	return &kvlog, nil
}

// findValue finds value where _id == kvlog.vid
func (kdb *KDB) findValue(vid string) (string, error) {
	value := Value{}
	filter := bson.D{primitive.E{Key: "_id", Value: vid}}
	err := kdb.vc.FindOne(kdb.ctx, filter).Decode(&value)
	if err != nil {
		return "", err
	}
	return value.Val, nil
}

// Set sets the current value for key to val (if not already val)
func (kdb *KDB) Set(key, val string) error {
	val = strings.TrimSpace(val)
	vlen := len(val)

	vid := ""
	if vlen > maxInlineValueLength {
		// find or insert value record
		hash := sha1.Sum([]byte(val))
		vid = hex.EncodeToString(hash[:])
		_, err := kdb.findValue(vid)
		if err != nil && err != mongo.ErrNoDocuments {
			return err
		}
		if err != nil {
			// vid not found - insert
			//fmt.Printf("value for %q not found - inserting\n", val)
			vrec := Value{ID: vid, Val: val}
			_, err := kdb.vc.InsertOne(kdb.ctx, vrec)
			if err != nil {
				return err
			}
			//} else {
			//fmt.Printf("value for %q found\n", val)
		}
	}

	// find or insert kvlog record
	kvlog, err := kdb.findKVLogLatest(key)
	if err != nil && err != mongo.ErrNoDocuments {
		return err
	}
	if err != mongo.ErrNoDocuments {
		if vid != "" && kvlog.vid == vid {
			// latest kvlog record matches, we're done
			//fmt.Printf("latest kvlog for %q found and vid matches\n", key)
			return nil
		} else if vid == "" && kvlog.Val == val {
			// latest kvlog record matches, we're done
			//fmt.Printf("latest kvlog for %q found and v matches\n", key)
			return nil
		}
	}

	// No kvlog record found, or vids don't match - do an insert
	//fmt.Printf("kvlog for %q not found or out of date - inserting\n", key)
	kvlog = &KVLog{Key: key, TS: time.Now().UnixNano()}
	if vid == "" {
		kvlog.Val = val
	} else {
		kvlog.vid = vid
	}
	_, err = kdb.kc.InsertOne(kdb.ctx, *kvlog)
	if err != nil {
		return err
	}

	return nil
}

// Get fetches the latest value for key
func (kdb *KDB) Get(key string) (string, error) {
	kvlog, err := kdb.findKVLogLatest(key)
	if err != nil {
		return "", err
	}
	if kvlog.vid == "" {
		return kvlog.Val, nil
	}
	return kdb.findValue(kvlog.vid)
}

// GetAt fetches the first value for key after ts
func (kdb *KDB) GetAt(key string, ts int64) (string, error) {
	kvlog, err := kdb.findKVLogAfter(key, ts)
	if err != nil {
		return "", err
	}
	if kvlog.vid == "" {
		return kvlog.Val, nil
	}
	return kdb.findValue(kvlog.vid)
}

// GetIterator returns an Interator to fetch successive KVLog records,
// in reverse timestamp order (i.e. latest first).
// The caller is responsible for calling Close() on the returned
// iterator once finished.
func (kdb *KDB) GetIterator(key string) (*Iterator, error) {
	filter := bson.D{
		primitive.E{Key: "k", Value: key},
	}
	options := options.Find()
	options.SetSort(bson.M{"ts": -1})
	cursor, err := kdb.kc.Find(kdb.ctx, filter, options)
	if err != nil {
		return nil, err
	}
	it := Iterator{kdb: kdb, cursor: cursor}
	return &it, nil
}

func (kdb *KDB) Disconnect() {
	kdb.client.Disconnect(kdb.ctx)
}

// Next returns the next KVLog record from the iterator, or nil
// if no records remain, or an error occurred (which will be
// available via Iterator.Err()).
func (it *Iterator) Next() *KVLog {
	if it.cursor.Next(it.kdb.ctx) {
		kvlog := KVLog{}
		err := it.cursor.Decode(&kvlog)
		if err != nil {
			it.err = err
			return nil
		}
		if kvlog.Val == "" {
			val, err := it.kdb.findValue(kvlog.vid)
			if err != nil {
				it.err = err
				return nil
			}
			kvlog.Val = val
		}
		return &kvlog
	}
	if err := it.cursor.Err(); err != nil {
		it.err = err
		return nil
	}
	return nil
}

// Close marks the iterator as closed. Next() should not be
// called again after the iterator has been closed.
func (it *Iterator) Close() {
	it.cursor.Close(it.kdb.ctx)
}

// Err returns the most recent error received from the iterator
func (it *Iterator) Err() error {
	return it.err
}
