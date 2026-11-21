FROM golang:1.26.10-bookworm AS build
WORKDIR /app
COPY services/ingestion-go/ .
RUN go mod download && CGO_ENABLED=0 go build -o /ingestion .
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /ingestion /ingestion
USER nonroot:nonroot
ENTRYPOINT ["/ingestion"]
