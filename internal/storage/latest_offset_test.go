package storage

import (
	"path/filepath"
	"testing"
)

func TestLatestOffset(t *testing.T) {
	t.Run("empty log", func(t *testing.T) {
		log, err := OpenPartitionLog(filepath.Join(t.TempDir(), "empty.log"))
		if err != nil {
			t.Fatal(err)
		}
		defer ClosePartitionLog(log)
		got, err := LatestOffset(log)
		if err != nil || got != 0 {
			t.Errorf("LatestOffset() = (%d, %v), want (0, nil)", got, err)
		}
	})

	t.Run("next offset follows appended records", func(t *testing.T) {
		log := openTestPartitionLog(t, "")
		for _, value := range []string{"one", "two", "three"} {
			if _, err := AppendRecord(log, nil, value); err != nil {
				t.Fatal(err)
			}
		}
		got, err := LatestOffset(log)
		if err != nil || got != 3 {
			t.Errorf("LatestOffset() = (%d, %v), want (3, nil)", got, err)
		}
	})

	t.Run("invalid state", func(t *testing.T) {
		if got, err := LatestOffset(nil); got != 0 || err == nil {
			t.Errorf("LatestOffset(nil) = (%d, %v), want error", got, err)
		}
		if got, err := LatestOffset(&PartitionLog{}); got != 0 || err == nil {
			t.Errorf("LatestOffset(empty log) = (%d, %v), want error", got, err)
		}
	})
}
