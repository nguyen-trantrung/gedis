FROM golang:1.24-alpine AS builder

WORKDIR /app
COPY go.mod ./

COPY . .
RUN go build -o gedis .

FROM alpine:latest

WORKDIR /app
COPY --from=builder /app/gedis .

ENV GEDIS_HOST=0.0.0.0
ENV GEDIS_PORT=6379
ENV GEDIS_REPLICAOF=

EXPOSE 6379

ENTRYPOINT ["/bin/sh", "-c", "if [ -n \"$GEDIS_REPLICAOF\" ]; then ./gedis --host $GEDIS_HOST --port $GEDIS_PORT --replicaof $GEDIS_REPLICAOF; else ./gedis --host $GEDIS_HOST --port $GEDIS_PORT; fi"]
