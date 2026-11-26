package main

import (
	"reflect"
	"testing"

	"github.com/Rwatana/miniKafka/internal/broker"
	kafkaprotocol "github.com/Rwatana/miniKafka/internal/protocol/kafka"
)

func TestMetadataTopicsFromPartitions(t *testing.T) {
	tests := []struct {
		name       string
		partitions []broker.PartitionInfo
		brokerID   int32
		want       []kafkaprotocol.MetadataTopic
	}{
		{name: "empty inventory", partitions: []broker.PartitionInfo{}, brokerID: 7, want: []kafkaprotocol.MetadataTopic{}},
		{
			name: "groups and sorts topics and partitions",
			partitions: []broker.PartitionInfo{
				{Topic: "orders", Partition: 2},
				{Topic: "events", Partition: 1},
				{Topic: "orders", Partition: 0},
				{Topic: "orders", Partition: 2},
			},
			brokerID: 7,
			want: []kafkaprotocol.MetadataTopic{
				{Name: "events", Partitions: []kafkaprotocol.MetadataPartition{{PartitionIndex: 1, LeaderID: 7, ReplicaNodes: []int32{7}, ISRNodes: []int32{7}}}},
				{Name: "orders", Partitions: []kafkaprotocol.MetadataPartition{
					{PartitionIndex: 0, LeaderID: 7, ReplicaNodes: []int32{7}, ISRNodes: []int32{7}},
					{PartitionIndex: 2, LeaderID: 7, ReplicaNodes: []int32{7}, ISRNodes: []int32{7}},
				}},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := metadataTopicsFromPartitions(test.partitions, test.brokerID); !reflect.DeepEqual(got, test.want) {
				t.Errorf("metadataTopicsFromPartitions() = %+v, want %+v", got, test.want)
			}
		})
	}
}
