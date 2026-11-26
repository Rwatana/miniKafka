# Handling FindCoordinator v1

## Kafka concept

Consumer groups ask Kafka which broker coordinates their group before they
join or commit offsets. The response supplies the node ID and network endpoint
the client should use for coordinator requests.

## Go design

`HandleFindCoordinatorRequestV1` parses the coordinator key and key type, then
returns the configured single broker for group keys. The response uses the v1
throttle, error, nullable message, node, host, and port fields.

## Tests

Tests compare the full response bytes and reject malformed headers, key strings,
unsupported coordinator types, invalid endpoints, and oversized frames.

## Deliberate limits

This handler only locates the group coordinator. JoinGroup, SyncGroup,
Heartbeat, OffsetCommit, and OffsetFetch are separate protocol steps.
