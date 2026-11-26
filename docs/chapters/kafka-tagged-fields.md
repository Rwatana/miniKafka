# Skipping Kafka tagged fields

## Kafka concept

Flexible Kafka schemas append tagged fields to structs. A tag buffer carries a
count followed by sorted tag IDs, byte lengths, and payload bytes. A receiver
can skip tags it does not understand and continue parsing the enclosing
message.

## Kafka 4.4.0 source map

The [Kafka 4.4 protocol guide](https://kafka.apache.org/44/design/protocol/)
describes tagged fields as the extension section used by flexible versions.
Each tag's value is length-delimited so receivers can skip unknown fields.

## Go design

`skipTaggedFields` uses `decodeUnsignedVarint` for the count, tag IDs, and
payload lengths. It checks that tag IDs increase strictly, bounds each payload
against the remaining frame, and returns the total number of bytes consumed.
Unknown payloads remain opaque.

## Tests

Fixtures cover an empty section, one and multiple unknown tags, preservation of
following bytes, truncated count/tag/size/data, and duplicate or descending
tag IDs.

## Deliberate differences

The function validates tag ordering and section bounds but does not decode any
tag-specific payload. API schemas will decide which tags to interpret.
