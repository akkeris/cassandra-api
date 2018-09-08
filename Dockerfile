FROM golang:1.10-alpine
RUN apk update
RUN apk add openssl ca-certificates git
RUN mkdir -p /go/src/oct-cassandra-api
ADD server.go  /go/src/oct-cassandra-api/server.go
ADD build.sh /build.sh
RUN chmod +x /build.sh
RUN /build.sh
CMD ["/go/src/oct-cassandra-api/server"]
EXPOSE 3900


