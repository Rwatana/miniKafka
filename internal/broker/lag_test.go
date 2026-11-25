package broker

import "testing"

func TestCalculateLag(t *testing.T) {
	for _, test := range []struct {
		name            string
		latestOffset    int64
		committedOffset int64
		want            int64
	}{
		{name: "consumer is behind", latestOffset: 25, committedOffset: 18, want: 7},
		{name: "consumer is caught up", latestOffset: 12, committedOffset: 12, want: 0},
		{name: "committed position is ahead", latestOffset: 4, committedOffset: 9, want: -5},
		{name: "empty partition", latestOffset: 0, committedOffset: 0, want: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := CalculateLag(test.latestOffset, test.committedOffset); got != test.want {
				t.Errorf("CalculateLag(%d, %d) = %d, want %d", test.latestOffset, test.committedOffset, got, test.want)
			}
		})
	}
}
