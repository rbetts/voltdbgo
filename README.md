# voltdbclient

** DEPRECATED. VoltDB produced an official driver to replace this work:**
[official voltdb voltdb-client-go driver](https://github.com/VoltDB/voltdb-client-go)

VoltDB driver for Google go (golang)

See api.go for usage hints. There are some client examples in the
example direcory.

This driver was largely written for fun and is still relatively incomplete.
See below for some of the more important missing parts.


## Using the driver

The driver connects to a running VoltDB database node and calls procedures.
The simplest example, omitting correct error handling, is:

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

## Examples

There are a few examples in github.com/rbetts/voltdbgo/cmds.

## Missing

The driver supports invoking stored procedures and reading responses.
However, there are several serializations that are not yet implemented.

 * Exception deserialization in responses not supported.
 * VARBINARY not supported
 * DECIMAL not supported
 * Creation of serialized VoltTables is not supported.
 * Arrays as stored procedure parameters not supported.
 * SQL NULL is not supported.

There are missing api methods.

 * There is no way to reset the table iterator.

There are missing components expected for a production client: 

 * The client doesn't timeout network reads or writes.
 * The client doesn't reconnect closed sockets.
 * The client doesn't provide a high level interface to connect to multiple
   nodes of a VoltDB database.

Row deserialization could be substantially more flexible. It would be nice
to allow tagged field names to specify columns (instead of requiring the
struct fields to be in column-order).


