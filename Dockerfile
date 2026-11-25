FROM golang:1.24.2-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/minikafka ./cmd/minikafka
RUN mkdir -p /out/data

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/minikafka /minikafka
COPY --from=build --chown=nonroot:nonroot /out/data /data
WORKDIR /app
ENV DATA_DIR=/data PORT=5000
EXPOSE 5000
ENTRYPOINT ["/minikafka"]
