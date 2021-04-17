package kvlog

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
)

const dbname = "kvlog_test"

func TestBasic(t *testing.T) {
	values := []string{"bar", "baz", "beg", "bet", "bit", "bog", "bot", "bug"}

	ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
	ts := time.Now().UnixNano()

	kdb, err := NewKDBOptions(ctx, KDBOptions{DBName: dbname})
	if err != nil {
		t.Fatal("constructor error: ", err)
	}
	defer kdb.Disconnect()

	// Drop existing collections to start clean
	kdb.kc.Drop(ctx)
	kdb.vc.Drop(ctx)

	// Recreate indexes (though not really required for testing)
	err = createIndexes(ctx, kdb.kc)
	if err != nil {
		t.Fatal(err)
	}

	val, err := kdb.GetAt("foo", ts)
	if err != nil && err != mongo.ErrNoDocuments {
		t.Fatalf("error on GetAt: %s\n", err.Error())
	}
	if err == nil {
		t.Fatal("unexpected success error on initial GetAt - documents found?\n")
	}

	for i, v := range values {
		err = kdb.Set("foo", v)
		if err != nil {
			t.Errorf("error on Set%d (%s): %s\n", i, v, err.Error())
		}

		val, err = kdb.Get("foo")
		if err != nil {
			t.Errorf("error on Get%d (%s): %s\n", i, v, err.Error())
		}
		if val != v {
			t.Errorf("error on Get%d: expecting %q, got %q\n", i, v, val)
		}
	}

	// GetAt
	val, err = kdb.GetAt("foo", ts)
	expect := values[0]
	if err != nil {
		t.Errorf("error on GetAt: %s\n", err.Error())
	}
	if val != expect {
		t.Errorf("error on GetAt: expecting %q, got %q\n", expect, val)
	}

	// GetIterator
	it, err := kdb.GetIterator("foo")
	if err != nil {
		t.Errorf("error on GetIterator: %s\n", err.Error())
	}
	defer it.Close()
	kvlog := it.Next()
	i := 1
	for kvlog != nil {
		expect = values[len(values)-i]
		if kvlog.Val != expect {
			t.Errorf("error on iterator %d: expecting %q, got %q\n", i, expect, kvlog.Val)
		}

		kvlog = it.Next()
		i += 1
	}
	if err = it.Err(); err != nil {
		t.Errorf("error from iterator: %s\n", err.Error())
	}
}
