FROM golang:1.10-alpine
RUN apk update
RUN apk add openssl ca-certificates git
RUN mkdir -p /go/src/cassandra-api
ADD server.go  /go/src/cassandra-api/server.go
ADD build.sh /build.sh
RUN chmod +x /build.sh
RUN /build.sh
WORKDIR /go/src/cassandra-api
CMD ["./server"]
EXPOSE 3000


