package main

import (
    "fmt"
    "github.com/rbetts/voltdbgo/voltdb"
)

/*
* tiny demonstrates selecting three row from a trival table:
* CREATE TABLE STORE (KEY VARCHAR(10), VALUE VARCHAR(10));
*/

func main() {
    volt, _ := voltdb.NewConnection("username", "", "localhost:21212")
    response, _ := volt.Call("@AdHoc", "select * from store order by Key limit 3;");
    type Row struct {
        Key string
        Value string
    }
    var row Row
    for response.Table(0).HasNext() {
        response.Table(0).Next(&row)
        fmt.Printf("Row: %v %v\n", row.Key, row.Value)
    }
}
