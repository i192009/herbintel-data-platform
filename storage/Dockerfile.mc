FROM golang:1.26.10-bookworm AS build
ENV CGO_ENABLED=0
RUN go install github.com/minio/mc@RELEASE.2025-04-16T18-13-26Z
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/* && useradd --uid 10001 --create-home mc
COPY --from=build /go/bin/mc /usr/local/bin/mc
USER 10001:10001
