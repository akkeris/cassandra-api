cd /go/src
go get  "github.com/lib/pq"
go get  "github.com/go-martini/martini"
go get  "github.com/martini-contrib/binding"
go get  "github.com/martini-contrib/render"
go get  "github.com/gocql/gocql"
go get  "github.com/nu7hatch/gouuid"
cd /go/src/oct-cassandra-api
go build server.go

