package main

import (
	"sort"

	"github.com/Rwatana/miniKafka/internal/broker"
	kafkaprotocol "github.com/Rwatana/miniKafka/internal/protocol/kafka"
)

// metadataTopicsFromPartitions maps file-backed partitions to single-broker Kafka metadata.
func metadataTopicsFromPartitions(partitions []broker.PartitionInfo, brokerID int32) []kafkaprotocol.MetadataTopic {
	partitionNumbersByTopic := make(map[string]map[int]struct{})
	for _, partition := range partitions {
		if partitionNumbersByTopic[partition.Topic] == nil {
			partitionNumbersByTopic[partition.Topic] = make(map[int]struct{})
		}
		partitionNumbersByTopic[partition.Topic][partition.Partition] = struct{}{}
	}

	topicNames := make([]string, 0, len(partitionNumbersByTopic))
	for topicName := range partitionNumbersByTopic {
		topicNames = append(topicNames, topicName)
	}
	sort.Strings(topicNames)

	metadata := make([]kafkaprotocol.MetadataTopic, 0, len(topicNames))
	for _, topicName := range topicNames {
		partitionNumbers := make([]int, 0, len(partitionNumbersByTopic[topicName]))
		for partitionNumber := range partitionNumbersByTopic[topicName] {
			partitionNumbers = append(partitionNumbers, partitionNumber)
		}
		sort.Ints(partitionNumbers)
		partitions := make([]kafkaprotocol.MetadataPartition, 0, len(partitionNumbers))
		for _, partitionNumber := range partitionNumbers {
			partitions = append(partitions, kafkaprotocol.MetadataPartition{
				PartitionIndex: int32(partitionNumber),
				LeaderID:       brokerID,
				ReplicaNodes:   []int32{brokerID},
				ISRNodes:       []int32{brokerID},
			})
		}
		metadata = append(metadata, kafkaprotocol.MetadataTopic{Name: topicName, Partitions: partitions})
	}
	return metadata
}
