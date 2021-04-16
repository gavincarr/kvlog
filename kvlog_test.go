package kvlog

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
)

const dbname = "kvlog_test"

func TestSetGet(t *testing.T) {
	ctx := context.Background()
	ts := time.Now().UnixNano()
	uri := "mongodb://localhost/"

	kdb, err := newKDBNamed(ctx, uri, dbname)
	if err != nil {
		t.Fatal("constructor error: ", err)
	}

	// Drop existing collections
	kdb.KC.Drop(ctx)
	kdb.VC.Drop(ctx)

	val, err := kdb.GetAt("foo", ts)
	if err != nil && err != mongo.ErrNoDocuments {
		t.Fatalf("error on GetAt: %s\n", err.Error())
	}
	if err == nil {
		t.Fatal("unexpected success error on initial GetAt - documents found?\n")
	}

	// Set1
	err = kdb.Set("foo", "bar")
	if err != nil {
		t.Errorf("error on Set: %s\n", err.Error())
	}

	val, err = kdb.Get("foo")
	expect := "bar"
	if err != nil {
		t.Errorf("error on Get: %s\n", err.Error())
	}
	if val != expect {
		t.Errorf("error on Get: expecting %q, got %q\n", expect, val)
	}

	// Set2
	err = kdb.Set("foo", "baz")
	if err != nil {
		t.Errorf("error on Set2: %s\n", err.Error())
	}

	val, err = kdb.Get("foo")
	expect = "baz"
	if err != nil {
		t.Errorf("error on Get2: %s\n", err.Error())
	}
	if val != expect {
		t.Errorf("error on Get2: expecting %q, got %q\n", expect, val)
	}

	// Set3
	err = kdb.Set("foo", "bog")
	if err != nil {
		t.Errorf("error on Set3: %s\n", err.Error())
	}

	val, err = kdb.Get("foo")
	expect = "bog"
	if err != nil {
		t.Errorf("error on Get3: %s\n", err.Error())
	}
	if val != expect {
		t.Errorf("error on Get3: expecting %q, got %q\n", expect, val)
	}

	// GetAt
	val, err = kdb.GetAt("foo", ts)
	expect = "bar"
	if err != nil {
		t.Errorf("error on Get2: %s\n", err.Error())
	}
	if val != expect {
		t.Errorf("error on Get2: expecting %q, got %q\n", expect, val)
	}
}
