FROM golang:1.23-alpine AS builder
ARG TARGETOS=linux
ARG TARGETARCH=amd64
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -ldflags="-s -w" -o /burnin-ce .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
RUN adduser -D -u 1000 burnin
COPY --from=builder /burnin-ce /usr/local/bin/burnin-ce
COPY burnin-config.yaml /etc/burnin/config.yaml

USER burnin
EXPOSE 8895

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD wget -qO- http://localhost:8895/health || exit 1

ENTRYPOINT ["burnin-ce"]
CMD ["--config", "/etc/burnin/config.yaml"]
