package main

import (
	"database/sql"
	"fmt"
	vault "github.com/akkeris/vault-client"
	"github.com/go-martini/martini"
	gocql "github.com/gocql/gocql"
	_ "github.com/lib/pq"
	"github.com/martini-contrib/binding"
	"github.com/martini-contrib/render"
	"github.com/nu7hatch/gouuid"
	"os"
	"strings"
	"time"
)

var Cluster *gocql.ClusterConfig
var S *gocql.Session
var brokerdb string
var cassandra_url string
var cassandra_admin_username string
var cassandra_admin_password string

type Cassandraspec struct {
	Keyspace string `json:"CASSANDRA_KEYSPACE"`
	Location string `json:"CASSANDRA_LOCATION"`
	Password string `json:"CASSANDRA_PASSWORD"`
	Username string `json:"CASSANDRA_USERNAME"`
}

type tagspec struct {
	Resource string `json:"resource"`
	Name     string `json:"name"`
	Value    string `json:"value"`
}
type provisionspec struct {
	Plan        string `json:"plan"`
	Billingcode string `json:"billingcode"`
}

func initSecrets() {
	secret := vault.GetSecret(os.Getenv("CASSANDRA_SECRET"))
	brokerdb = vault.GetFieldFromVaultSecret(secret, "brokerdburl")
	cassandra_url = vault.GetFieldFromVaultSecret(secret, "url")
	cassandra_admin_username = vault.GetFieldFromVaultSecret(secret, "username")
	cassandra_admin_password = vault.GetFieldFromVaultSecret(secret, "password")
}

func main() {
	initSecrets()
	startcluster()
	defer S.Close()
	m := martini.Classic()
	m.Use(render.Renderer())
	m.Get("/v1/cassandra/plans", plans)
	m.Post("/v1/cassandra/instance", binding.Json(provisionspec{}), provision)
	m.Get("/v1/cassandra/url/:keyspace", url)
	m.Delete("/v1/cassandra/instance/:keyspace", Delete)
	m.Run()

}

func url(params martini.Params, r render.Render) {
	keyspace := params["keyspace"]
	var spec Cassandraspec
	spec, err := getDetails(keyspace)
	if err != nil {
		fmt.Println(err)
		r.JSON(500, map[string]string{"status": "500", "msg": err.Error()})
		return
	}
	r.JSON(200, spec)
}

func getDetails(keyspace string) (c Cassandraspec, e error) {
	var spec Cassandraspec
	spec.Keyspace = keyspace
	spec.Location = strings.Join(Cluster.Hosts, ",")
	uri := brokerdb
	db, err := sql.Open("postgres", uri)
	if err != nil {
		fmt.Println(err)
	}
        defer db.Close()
	stmt, err := db.Prepare("select username, password from provision where name = $1 ")
	if err != nil {
		fmt.Println(err)
		return spec, err
	}
	defer stmt.Close()
	rows, err := stmt.Query(keyspace)
	var username string
	var password string
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&username, &password)
		if err != nil {
			fmt.Println(err)
			db.Close()
			return spec, err
		}

	}
	spec.Username = username
	spec.Password = password
	return spec, nil
}

func store(keyspace string, billingcode string, plan string, username string, password string) error {
	uri := brokerdb
	db, err := sql.Open("postgres", uri)
	if err != nil {
		fmt.Println(err)
		return err
	}
        defer db.Close()
	var newname string
	err = db.QueryRow("INSERT INTO provision(name, plan, claimed, billingcode,username,password) VALUES($1,$2,$3,$4,$5,$6) returning name;", keyspace, plan, "yes", billingcode, username, password).Scan(&newname)

	if err != nil {
		fmt.Println(err)
		return err
	}
	err = db.Close()
	if err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}

func Delete(params martini.Params, r render.Render) {
	err := delete(params["keyspace"])
	if err != nil {
		fmt.Println(err)
		r.JSON(500, err)
		return
	}
	r.JSON(200, nil)
}
func delete(keyspace string) error {
	spec, err := getDetails(keyspace)
	if err != nil {
		return err
	}
	query := "DROP USER " + spec.Username + ";"
	q := S.Query(query)
	err = q.Exec()
	if err != nil {
		fmt.Println(err)
		return err
	}

	query = "DROP KEYSPACE " + keyspace + ";"
	q = S.Query(query)
	err = q.Exec()
	if err != nil {
		fmt.Println(err)
		return err
	}

	uri := brokerdb
	db, err := sql.Open("postgres", uri)
	if err != nil {
		fmt.Println(err)
		return err
	}
        defer db.Close()
	stmt, err := db.Prepare("DELETE from  provision where name = $1")
	if err != nil {
		fmt.Println(err)
		return err
	}
	_, err = stmt.Exec(keyspace)
	if err != nil {
		fmt.Println(err)
		return err
	}
	err = db.Close()
	if err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}

func plans(params martini.Params, r render.Render) {
	plans := make(map[string]interface{})
	plans["small"] = "1 replica, SimpleStrategy"
	plans["medium"] = "2 replicas, SimpleStrategy"
	plans["large"] = "3 replicas, SimpleStrategy"
	r.JSON(200, plans)

}

func provision(spec provisionspec, berr binding.Errors, r render.Render) {
	var cspec Cassandraspec
	u, err := uuid.NewV4()
	if err != nil {
		fmt.Println(err)
		r.JSON(500, map[string]string{"error": err.Error()})
		return
	}
	keyspace := os.Getenv("NAME_PREFIX") + strings.Split(u.String(), "-")[0]
	cspec.Keyspace = keyspace

	u, err = uuid.NewV4()
	if err != nil {
		fmt.Println(err)
		r.JSON(500, map[string]string{"error": err.Error()})
		return
	}
	casspassword := "p" + strings.Split(u.String(), "-")[0]
	cspec.Password = casspassword

	u, err = uuid.NewV4()
	if err != nil {
		fmt.Println(err)
		r.JSON(500, map[string]string{"error": err.Error()})
		return
	}
	cassusername := "u" + strings.Split(u.String(), "-")[0]
	cspec.Username = cassusername

	strategy := "SimpleStrategy"
	replication_factor := "1"
	if spec.Plan == "small" {
		replication_factor = "1"
	}
	if spec.Plan == "medium" {
		replication_factor = "2"
	}
	if spec.Plan == "large" {
		replication_factor = "3"
	}
	query := "CREATE KEYSPACE " + keyspace + " WITH replication " + "= {'class':'" + strategy + "', 'replication_factor':" + replication_factor + "}; "
	q := S.Query(query)
	err = q.Exec()
	if err != nil {
		fmt.Println(err)
		r.JSON(500, map[string]string{"error": err.Error()})
		return
	}

	query = "CREATE ROLE " + cassusername + " WITH PASSWORD = '" + casspassword + "' AND LOGIN = true"
	q = S.Query(query)
	err = q.Exec()
	if err != nil {
		fmt.Println(err)
		r.JSON(500, map[string]string{"error": err.Error()})
		return
	}

	query = "GRANT ALL permissions on keyspace " + keyspace + " to " + cassusername + " ;"
	q = S.Query(query)
	err = q.Exec()
	if err != nil {
		fmt.Println(err)
		r.JSON(500, map[string]string{"error": err.Error()})
		return
	}

	err = store(keyspace, spec.Billingcode, spec.Plan, cassusername, casspassword)
	if err != nil {
		fmt.Println(err)
		r.JSON(500, map[string]string{"error": err.Error()})
		return
	}
	cspec.Location = strings.Join(Cluster.Hosts, ",")
	r.JSON(201, cspec)

}

func startcluster() {
	cluster := gocql.NewCluster(cassandra_url)
	pass := gocql.PasswordAuthenticator{cassandra_admin_username, cassandra_admin_password}
	cluster.IgnorePeerAddr = true
	cluster.ProtoVersion = 4
	cluster.CQLVersion = "3.0.0"
	cluster.Authenticator = pass
	duration := 10 * time.Second
	cluster.Timeout = duration
	cluster.ConnectTimeout = duration
	cluster.Consistency = gocql.One
	cluster.Port = 9042
	cluster.NumConns = 1
	var err error
	S, err = cluster.CreateSession()
	if err != nil {
		fmt.Println("got session error")
		fmt.Println(err)
		os.Exit(1)
	}
	Cluster = cluster
}
