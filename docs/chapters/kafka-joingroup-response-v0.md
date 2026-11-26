# Encoding JoinGroup v0 responses

## Kafka concept

JoinGroup returns the selected protocol, leader and member IDs, generation, and
the protocol metadata that each group member submitted. The leader uses this
metadata to compute assignments before sending SyncGroup.

## Go design

`encodeJoinGroupResponseV0` writes the fixed-width error and generation fields,
then the non-flexible strings and member array. Kafka strings carry signed
16-bit byte lengths; member metadata is opaque and uses a signed 32-bit length.

## Tests

An exact-byte test checks field order and lengths. Error tests reject invalid
UTF-8 and strings too long for Kafka's INT16 string length.

## Deliberate limits

This step encodes the response body only. JoinGroup still has no coordinator
state, response framing, network dispatch, or member assignment logic.
