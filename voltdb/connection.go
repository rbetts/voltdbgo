package voltdbgo

import (
	"bytes"
	"fmt"
	"io"
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

// GoString provides a default printable format
func (conn *Conn) GoString() string {
	if conn.connData != nil {
		return conn.connData.GoString()
	}
	return "uninitialized"
}

func (conn *connectionData) GoString() string {
	return fmt.Sprintf("hostId:%v, connId:%v, leaderAddr:%v buildString:%v",
		conn.hostId, conn.connId, conn.leaderAddr, conn.buildString)
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

func (rsp *Response) GoString() string {
	return fmt.Sprintf("Response: clientData:%v, status:%v, statusString:%v, "+
		"clusterLatency: %v, appStatus: %v, appStatusString: %v\n",
		rsp.clientData, rsp.status, rsp.statusString,
		rsp.clusterLatency, rsp.appStatus, rsp.appStatusString)
}

// writeMessage prepends a header and writes header and buf to tcpConn
func (conn *Conn) writeMessage(buf bytes.Buffer) error {
	length := buf.Len() + 1 // length includes proto version.
	var hdr bytes.Buffer
	writeInt(&hdr, int32(length))
	writeProtoVersion(&hdr)
	io.Copy(conn.tcpConn, &hdr)
	io.Copy(conn.tcpConn, &buf)
	return nil // TODO: obviously wrong
}

// readMessageHdr reads the standard wireprotocol header.
func (conn *Conn) readMessageHdr() (size int32, err error) {
	// Total message length Integer  4
	size, err = readInt(conn.tcpConn)
	if err != nil {
		return
	}
	// Version Byte 1
	// TODO: error on incorrect version.
	_, err = readByte(conn.tcpConn)

	// size includes the protocol version.
	return (size - 1), nil
}

// readLoginResponse parses the login response message.
func (conn *Conn) readMessage() (*bytes.Buffer, error) {
	size, err := conn.readMessageHdr()
	if err != nil {
		return nil, err
	}
	data := make([]byte, size)
	if _, err = io.ReadFull(conn.tcpConn, data); err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer(data)
	return buf, nil
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

func (conn *Conn) readLoginResponse() (*connectionData, error) {
	buf, err := conn.readMessage()
	if err != nil {
		return nil, err
	}
	connData, err := deserializeLoginResponse(buf)
	return connData, err
}

// Read parses a Call response.
func (conn *Conn) Read() (response *Response, err error) {
	buf, err := conn.readMessage()
	if err != nil {
		return nil, err
	}
	response, err = deserializeResponse(buf)
	return
}
