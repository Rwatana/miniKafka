# Handling Fetch v3

## Kafka concept

Fetch v3 adds an aggregate response byte budget while keeping per-partition
limits. The broker may return one initial batch that exceeds the aggregate
budget, but it should not keep adding later partition batches beyond the
remaining budget.

## Go design

`HandleFetchRequestV3` decodes the v3 request, narrows each reader limit to the
remaining aggregate budget, maps partition errors, and encodes the v2/v3
response schema with the request correlation ID.

## Tests

Tests verify aggregate limits across partitions, minimum-byte waiting, missing
and failed partitions, offset range errors, framing, and malformed requests.

## Deliberate limits

The dispatcher and ApiVersions range still need to expose Fetch v3. Those are
separate steps.
