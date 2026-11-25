package broker

// CalculateLag returns how many records the committed position trails the log end.
func CalculateLag(latestOffset, committedOffset int64) int64 {
	return latestOffset - committedOffset
}
