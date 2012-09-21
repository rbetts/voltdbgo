// An example using the voltdb/examples/voter application.
// To run this, first start the voltdb voter example:
// `cd voltdb-x.y.z/example/voter && ./run.sh`.
// Then run this go program.
package main

import (
	"flag"
	"fmt"
	"github.com/rbetts/voltdbgo/voltdb"
	"log"
	"math/rand"
	"os"
	"runtime/pprof"
	"time"
)

var ttlContestants = 6
var contestants = "Andy,Bob,Cathy,Doug,Erik,Greg"
var voterGoroutines = 8
var votingDuration = 15 * time.Second
var cpuprofile = ""

// ScalarResult is used to read scalar stored procedures results.
type ScalarResult struct {
	Result int
}

func main() {
	flag.DurationVar(&votingDuration, "duration", 15*time.Second, "seconds to execute")
	flag.StringVar(&cpuprofile, "cpuprofile", "", "name of profile file to write")
	flag.IntVar(&voterGoroutines, "threads", 8, "number of concurrent voting goroutines")
	flag.Parse()

	volt := connectOrDie()
	defer volt.Close()
	initialize(volt)
	vote()
	printResults(volt)
}

func setupProfiler() {
	if cpuprofile != "" {
		f, err := os.Create(cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
	}
}

func teardownProfiler() {
	if cpuprofile != "" {
		pprof.StopCPUProfile()
	}
}

func connectOrDie() *voltdb.Conn {
	volt, err := voltdb.NewConnection("username", "", "localhost:21212")
	if err != nil {
		log.Fatalf("Connection error %v\n", err)
	}
	if !volt.TestConnection() {
		log.Fatalf("Connection error: failed to ping VoltDB database.")
	}
	return volt
}

// Add contestants to the database if necessary
func initialize(volt *voltdb.Conn) {
	rsp, err := volt.Call("Initialize", ttlContestants, contestants)
	if err != nil {
		log.Fatalf("Failed in initialize database. %v\n", err)
	}
	if rsp.Status() == voltdb.SUCCESS {
		var rs ScalarResult
		rsp.Table(0).Next(&rs)
		fmt.Printf("Initialized %d contestants.\n", rs.Result)
	} else {
		log.Fatalf("Failed to initialize %#v.\n", rsp)
	}
}

// vote starts voterGoroutine number votes loops and returns when
// they have all run to conclusion.
func vote() {
	setupProfiler()
	defer teardownProfiler()

	var joiners = make([]chan int, 0)
	for i := 0; i < voterGoroutines; i++ {
		var joinchan = make(chan int)
		joiners = append(joiners, joinchan)
		go placeVotes(joinchan)
	}
	var totalVotes = 0
	for v, join := range joiners {
		votes := <-join
		totalVotes += votes
		fmt.Printf("Voter %v finished and placed %v votes.\n", v, votes)
	}
	fmt.Printf("Generated %v votes in %v seconds (%0.0f votes/second)\n",
		totalVotes, votingDuration.Seconds(),
		float64(totalVotes)/votingDuration.Seconds())
	return
}

// placeVotes votes for votingDuration seconds.
func placeVotes(join chan int) {
	volt, err := voltdb.NewConnection("username", "", "localhost:21212")
	defer volt.Close()
	if err != nil {
		log.Fatalf("Connection error db. %v\n", err)
	}

	timeout := time.After(votingDuration)
	placedVotes := 0

	for {
		select {
		case <-timeout:
			join <- placedVotes
			return
		default:
			// construct a phoneNumber with a valid area code.
			var phoneNumber int64 = (5080000000 + int64(rand.Int31()))
			// pick a contestant at random (contestant ids are 1-based)
			var contestant = (rand.Int() % ttlContestants) + 1
			rsp, err := volt.Call("Vote", phoneNumber, contestant, 100)
			if err != nil {
				log.Fatalf("Error voting. %v\n", err)
			}
			if rsp.Status() == voltdb.SUCCESS {
				placedVotes++
			} else {
				fmt.Printf("Vote failed %#v\n", rsp)
			}
		}
	}
}

// printResults displays the current vote tally for each contestant.
func printResults(volt *voltdb.Conn) {
	type ResultsRow struct {
		Contestant string
		Id         int
		Votes      int
	}
	var row ResultsRow
	fmt.Printf("\nCurrent results:\n")
	response, _ := volt.Call("Results")
	table := response.Table(0)
	for table.HasNext() {
		if err := table.Next(&row); err != nil {
			log.Fatalf("Table iteration error %v\n", err)
		}
		fmt.Printf("%v\t%v\t%v\n", row.Contestant, row.Id, row.Votes)
	}
}
