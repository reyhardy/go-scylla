package main

import (
	"fmt"
	"log"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/gocqlx/v2/table"
)

const (
	keyspace  = "testing"
	tableName = "persons"
)

func main() {
	cluster := gocql.NewCluster("172.17.0.2")

	session, err := gocqlx.WrapSession(cluster.CreateSession())
	if err != nil {
		log.Fatal("error connect db: ", err)
	}

	defer session.Close()

	session.ExecStmt(fmt.Sprintf("DROP KEYSPACE IF EXISTS %s;", keyspace))

	err = session.ExecStmt(fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};", keyspace))
	if err != nil {
		log.Fatal("error create keyspace: ", err)
	}

	type Person struct {
		FirstName string
		LastName  string
		Age       int
	}

	err = session.ExecStmt(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%s(
		first_name text PRIMARY KEY,
		last_name text,
		age int
	);`, keyspace, tableName))
	if err != nil {
		log.Fatal("error create table: ", err)
	}

	personMetadata := table.Metadata{
		Name:    fmt.Sprintf("%s.%s", keyspace, tableName),
		Columns: []string{"first_name", "last_name", "age"},
		PartKey: []string{"first_name"},
	}

	//insert using query builder
	insertQuery := qb.Insert(personMetadata.Name).Columns(personMetadata.Columns...).Query(session)
	insertQuery.BindStruct(Person{
		FirstName: "reza",
		LastName:  "hadzri",
		Age:       34,
	})
	if err := insertQuery.ExecRelease(); err != nil {
		log.Fatal("insertExecRelease() failed: ", err)
	}

	//insert using table model
	personTable := table.New(personMetadata)
	insertPerson := personTable.InsertQuery(session)
	insertPerson.BindStruct(&Person{
		FirstName: "juan",
		LastName:  "hardy",
		Age:       34,
	})

	//select using table model
	queryPerson := personTable.SelectQuery(session)
	queryPerson.BindStruct(&Person{
		FirstName: "reza",
	})

	var person []*Person
	if err := queryPerson.Select(&person); err != nil {
		log.Fatal("Select() failed: ", err)
	}

	for _, i := range person {
		log.Printf("Select people: %#v", *i)
	}
}
