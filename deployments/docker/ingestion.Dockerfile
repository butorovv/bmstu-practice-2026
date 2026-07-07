FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/ingestion ./cmd/ingestion

FROM scratch

COPY --from=builder /out/ingestion /ingestion

EXPOSE 8080

ENTRYPOINT ["/ingestion"]
