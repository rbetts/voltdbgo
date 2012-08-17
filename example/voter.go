package main

import (
	"fmt"
	"github.com/rbetts/voltdbgo/voltdb"
	"log"
)

// A simple example using the voltdb/examples/voter application.

type ResultsRow struct {
	Contestant string
	Id         int
	Votes      int
}

func (row *ResultsRow) GoString() string {
	return fmt.Sprintf("%v\t%v\t%v\n", row.Contestant, row.Id, row.Votes)
}

func main() {
	volt, err := voltdb.NewConnection("username", "", "localhost:21212")
	if err != nil {
		log.Fatalf("Connection error %v\n", err)
	}
	if !volt.TestConnection() {
		log.Fatalf("Connection error: failed to ping VoltDB database.")
	}
	printResults(volt)
	volt.Close()
}

func printResults(volt *voltdb.Conn) {
	var row ResultsRow
	response, _ := volt.Call("Results")
	table := response.Table(0)

	for table.HasNext() {
		if err := table.Next(&row); err != nil {
			log.Fatalf("Table iteration error %v\n", err)
		}
		fmt.Printf("%#v", &row)
	}
}
