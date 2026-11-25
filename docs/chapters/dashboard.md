# Viewing topics and lag in the dashboard

## Kafka concept

Operators need a quick view of partition end offsets and consumer progress. A
dashboard can show both while keeping the broker API as the source of truth.

## Reference source

The reference dashboard under
[`minikafka/dashboard`](https://github.com/ayuxh16/Minikafka/tree/main/minikafka/dashboard)
reads the broker's `/topics` and `/lag` routes.

## Go design

`DashboardHandler.ServeHTTP` serves one self-contained HTML page. Its browser
code refreshes topic metadata and loads lag for a group entered by the user;
the page uses only the same JSON endpoints as other clients.

## Tests

The handler test checks the HTML content type and that the page includes both
API requests and the displayed offset fields.

## Deliberate differences

The Go port removes the separate React build and serves a small page directly
from the broker process. It does not add a second data path or frontend build
dependency.
