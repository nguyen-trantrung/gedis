FROM golang:1.24-alpine AS builder

WORKDIR /build
COPY go.mod ./
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o binary ./app

FROM alpine:latest

WORKDIR /app
COPY --from=builder /build/binary .
RUN chmod +x ./binary
RUN mv ./binary ./gedis

ENV GEDIS_HOST=0.0.0.0
ENV GEDIS_PORT=6379
ENV GEDIS_REPLICAOF=

EXPOSE 6379

ENTRYPOINT ["/bin/sh", "-c", "if [ -n \"$GEDIS_REPLICAOF\" ]; then ./gedis --host $GEDIS_HOST --port $GEDIS_PORT --replicaof $GEDIS_REPLICAOF; else ./gedis --host $GEDIS_HOST --port $GEDIS_PORT; fi"]
