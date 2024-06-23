package main

import (
	"fmt"
	"log"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/table"
)

func main() {
	cluster := gocql.NewCluster("172.17.0.2")
	cluster.Keyspace = "testing"
	cluster.ProtoVersion = 4

	session, err := gocqlx.WrapSession(cluster.CreateSession())
	if err != nil {
		log.Fatal("error connect db: ", err)
	}
	defer session.Close()

	err = session.ExecStmt(fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %v WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 3};", cluster.Keyspace))
	if err != nil {
		log.Fatal("error create keyspace: ", err)
	}

	var personMetadata = table.Metadata{
		Name:    "person",
		Columns: []string{"first_name", "last_name", "age"},
		PartKey: []string{"first_name"},
		SortKey: []string{"last_name"},
	}

	log.Printf("person table metadata: %#v", personMetadata)

	var personTable = table.New(personMetadata)

	type Person struct {
		FirstName string
		LastName  string
		Age       int
	}

	p := Person{
		"AlJuan",
		"Hardy",
		34,
	}

	iq := session.Query(personTable.Insert()).BindStruct(p)
	if err := iq.ExecRelease(); err != nil {
		log.Fatal("error insert table: ", err)
	}

	gq := session.Query(personTable.Get()).BindStruct(p)
	if err := gq.GetRelease(&p); err != nil {
		log.Fatal("error get table: ", err)
	}

	log.Printf("person table: %#v", personTable)
}
