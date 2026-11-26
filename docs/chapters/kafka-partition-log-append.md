# Appending Kafka record batches

## Kafka concept

The broker assigns offsets in each partition's ordered log. A batch's base
offset is chosen by the broker, while its record offset deltas remain inside
the batch. Durable append must complete before the broker reports success.

## Go design

`AppendKafkaRecordBatch` copies the validated batch, writes the recovered next
offset into its base-offset field, appends and syncs the bytes, then advances
the handle's next offset. A failed write or sync truncates back to the prior
file size and leaves offset state unchanged.

## Tests

Fixtures cover sequential offset assignment, input preservation, restart
recovery, malformed headers, overflow, closed handles, short writes, and sync
failures.

## Deliberate limits

The caller must decode and validate record contents and recover an existing log
before appending. This step does not yet connect the storage handle to a Kafka
request handler.
