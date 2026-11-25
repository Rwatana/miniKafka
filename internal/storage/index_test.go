package storage

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestRebuildIndex(t *testing.T) {
	tests := []struct {
		name string
		log  string
		want []int64
	}{
		{name: "empty log"},
		{name: "one line with newline", log: "one\n", want: []int64{0}},
		{name: "multiple lines with final newline", log: "one\ntwo\n", want: []int64{0, 4}},
		{name: "multiple lines without final newline", log: "one\ntwo", want: []int64{0, 4}},
		{name: "multibyte UTF-8 line", log: "one\n二\nthree\n", want: []int64{0, 4, 8}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := rebuildIndex(strings.NewReader(test.log))
			if err != nil {
				t.Fatalf("rebuildIndex() error = %v", err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Errorf("rebuildIndex() = %v, want %v", got, test.want)
			}
		})
	}

	t.Run("reader error", func(t *testing.T) {
		wantErr := errors.New("disk read failed")
		got, err := rebuildIndex(errorReader{err: wantErr})
		if !errors.Is(err, wantErr) {
			t.Errorf("rebuildIndex() error = %v, want wrapped %v", err, wantErr)
		}
		if got != nil {
			t.Errorf("partial index = %v, want nil", got)
		}
	})
}

type errorReader struct {
	err error
}

func (reader errorReader) Read([]byte) (int, error) {
	return 0, reader.err
}
