package voltdbgo

import (
	"bytes"
	"fmt"
	"net"
)

// kInlineTableDeser when true will deserialize table data
// in to Response.Tables[].  When false, response deserialization
// stores table data as bytes in Response.tableData[]
var kInlineTableDeser bool = false

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

// NewConn creates an initialized, authenticated Conn
func NewConn(user string, passwd string, laddr, raddr *net.TCPAddr) (*Conn, error) {
	conn := new(Conn)
	tcpConn, err := net.DialTCP("tcp", laddr, raddr)
	if err != nil {
		return nil, err
	}
	conn.tcpConn = tcpConn

	login, err := serializeLoginMessage(user, passwd)
	if err != nil {
		return nil, err
	}

	if err = conn.writeMessage(login); err != nil {
		return nil, err
	}

	connData, err := conn.readLoginResponse()
	if err != nil {
		return nil, err
	}
	conn.connData = connData
	return conn, nil
}

// GoString provides a default printable format for Conn
func (conn *Conn) GoString() string {
	if conn.connData != nil {
        return fmt.Sprintf("hostId:%v, connId:%v, leaderAddr:%v buildString:%v",
        conn.connData.hostId, conn.connData.connId,
        conn.connData.leaderAddr, conn.connData.buildString)
	}
	return "uninitialized"
}

// Call invokes the procedure 'procedure' with parameter values 'params'.
func (conn *Conn) Call(procedure string, handle int64, params ...interface{}) error {
	buf, err := serializeCall(procedure, handle, params)
	if err != nil {
		return err
	}
	if err := conn.writeMessage(buf); err != nil {
		return err
	}
	return nil
}

// Read parses a Call response.
func (conn *Conn) Read() (response *Response, err error) {
	buf, err := conn.readMessage()
	if err != nil {
		return nil, err
	}
	response, err = deserializeCallResponse(buf)
	return
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
    tableBufs       []bytes.Buffer
}

func (rsp *Response) ResultSets() []Table {
	return rsp.tables
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
	rows        []Row
}

func (table Table) GoString() string {
	return fmt.Sprintf("Table: statusCode: %v, columnCount: %v, "+
		"rowCount: %v\n", table.statusCode, table.columnCount,
		table.rowCount)
}

type Row struct {
	vals []interface{}
}

