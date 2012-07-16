package main

import (
    "fmt"
    "log"
    "net"
    "github.com/rbetts/voltdbgo"
)

// An example client using the voltdb driver
func main() {
    raddr, err := net.ResolveTCPAddr("tcp", "localhost:21212")
    if err != nil {
        log.Fatalf("Error resolving localhost:21212. Exiting.")
    }

    // connect to the database
    cxn, err := voltdbgo.NewConn("username", "", nil, raddr)
    if err != nil {
        log.Fatalf("Error initializing connection to server: %v. Exiting.", err)
    }
    fmt.Printf("Successfully connected. Connection: %#v\n", cxn)

    // call a procedure
    var handle int64 = 0
    var phoneNumber int64 = 5084055555
    var contestant int32 = 3
    var maxVotes int64 = 10
    err = cxn.Call("Vote", handle, phoneNumber, contestant, maxVotes)
    if err != nil {
        log.Fatalf("Error call Vote procedure: %v. Exiting.", err)
    }

    // read a response.
    resp, err := cxn.Read()
    if err != nil {
        log.Fatalf("Error reading Vote response: %v. Exiting.", err)
    }
    fmt.Printf("%#v", resp)
    for _, table := range resp.ResultSets() {
        fmt.Printf("%#v", table)
    }

    fmt.Printf("done\n")
}


