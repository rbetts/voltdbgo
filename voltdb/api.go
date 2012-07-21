package voltdb

import (
	"bytes"
	"fmt"
	"net"
)

// Conn is a single connection to a single node of a VoltDB database
type Conn struct {
	tcpConn  *net.TCPConn
	connData *connectionData
}

// connectionData are the values returned by a successful login.
type connectionData struct {
	hostId      int32
	connId      int64
	leaderAddr  int32
	buildString string
}

// NewConn creates an initialized, authenticated Conn.
func NewConnection(user string, passwd string, hostAndPort string) (*Conn, error) {
	var conn = new(Conn)
	var err error
	var raddr *net.TCPAddr
	var login bytes.Buffer

	if raddr, err = net.ResolveTCPAddr("tcp", hostAndPort); err != nil {
		return nil, fmt.Errorf("Error resolving %v.", hostAndPort)
	}
	if conn.tcpConn, err = net.DialTCP("tcp", nil, raddr); err != nil {
		return nil, err
	}
	if login, err = serializeLoginMessage(user, passwd); err != nil {
		return nil, err
	}
	if err = conn.writeMessage(login); err != nil {
		return nil, err
	}
	if conn.connData, err = conn.readLoginResponse(); err != nil {
		return nil, err
	}
	return conn, nil
}

// GoString provides a default printable format for Conn.
func (conn *Conn) GoString() string {
	if conn.connData != nil {
		return fmt.Sprintf("hostId:%v, connId:%v, leaderAddr:%v buildString:%v",
			conn.connData.hostId, conn.connData.connId,
			conn.connData.leaderAddr, conn.connData.buildString)
	}
	return "uninitialized"
}

// Call invokes the procedure 'procedure' with parameter values 'params'
// and returns a pointer to the associated Response.
func (conn *Conn) Call(procedure string, params ...interface{}) (*Response, error) {
	var call bytes.Buffer
	var resp *bytes.Buffer
	var err error

	// Use 0 for handle; it's not necessary in pure sync client.
	if call, err = serializeCall(procedure, 0, params); err != nil {
		return nil, err
	}
	if err := conn.writeMessage(call); err != nil {
		return nil, err
	}
	if resp, err = conn.readMessage(); err != nil {
		return nil, err
	}
	return deserializeCallResponse(resp)
}

// Response is a stored procedure result.
type Response struct {
	clientData      int64
	fieldsPresent   uint8
	status          int8
	statusString    string
	appStatus       int8
	appStatusString string
	clusterLatency  int32
	exceptionLength int32
	exceptionBytes  []byte
	resultCount     int16
	tables          []Table
}

func (rsp *Response) ResultSets() []Table {
	return rsp.tables
}

func (rsp *Response) Table(offset int) *Table {
	return &rsp.tables[offset]
}

func (rsp *Response) GoString() string {
	return fmt.Sprintf("Response: clientData:%v, status:%v, statusString:%v, "+
		"clusterLatency: %v, appStatus: %v, appStatusString: %v\n",
		rsp.clientData, rsp.status, rsp.statusString,
		rsp.clusterLatency, rsp.appStatus, rsp.appStatusString)
}

type Table struct {
	statusCode  int8
	columnCount int16
	columnTypes []int8
	columnNames []string
	rowCount    int32
	rows        bytes.Buffer
}

func (table *Table) GoString() string {
	return fmt.Sprintf("Table: statusCode: %v, columnCount: %v, "+
		"rowCount: %v\n", table.statusCode, table.columnCount,
		table.rowCount)
}

// Rowcount returns the number of rows returned by the server for this table.
func (table *Table) Rowcount() int {
	return int(table.rowCount)
}

// Next populates v (*struct) with the values of the next row.
func (table *Table) Next(v interface{}) error {
	return table.next(v)
}

// HasNext returns true of there are additional rows to read.
func (table *Table) HasNext() bool {
	return table.rows.Len() > 0
}
