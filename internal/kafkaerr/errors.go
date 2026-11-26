// Package kafkaerr defines shared Kafka broker errors used by storage-facing and protocol-facing code.
package kafkaerr

import "errors"

var ErrUnknownTopicOrPartition = errors.New("unknown topic or partition")
